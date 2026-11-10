/**
 * End-to-end cases that drive the stand's stock OpenSearch Dashboards against IceGate's
 * OpenSearch listener. Each case pins one client behaviour that IceGate's READMEs and code cite
 * by the case name, so a name is a stable identifier: rename one only together with every
 * citation (`grep -rn '<old name>'`).
 *
 * Inputs are the rows seed.sh writes and the actions below; every expected value is derived by
 * hand from those rows: six logs (3 INFO, 1 WARN, 2 ERROR; `http.method` POST on three of
 * them, GET on three; `http` only on "order accepted") and four spans.
 */
import { test, expect, type Page, type APIRequestContext } from '@playwright/test';
import {
  OSD_HEADERS,
  OSD_URL,
  icegateJson,
  readSeed,
  seedWindow,
  setOsdSettings,
  toRison,
  waitForOsdAvailable,
  type Seed,
} from './stand';

const LOGS_PATTERN_ID = 'e2e-logs';
const ALL_BODIES = [
  'order accepted',
  'payment authorized',
  'inventory low',
  'payment declined',
  'card expired',
  'order shipped',
];

let seed: Seed;
let osdVersion: string;

test.beforeAll(async ({ playwright }) => {
  seed = readSeed();
  const api = await playwright.request.newContext({ baseURL: OSD_URL });
  try {
    osdVersion = await waitForOsdAvailable(api, 300_000);
    // The pattern stores the field list OSD's own field API derives for `logs`: the Discover,
    // visualization and index pattern cases read their fields from it.
    const fields = await osdLogsFields(api);
    const created = await api.post(`/api/saved_objects/index-pattern/${LOGS_PATTERN_ID}?overwrite=true`, {
      headers: OSD_HEADERS,
      data: { attributes: { title: 'logs', timeFieldName: 'timestamp', fields: JSON.stringify(fields) } },
    });
    expect(created.status(), await created.text()).toBe(200);
  } finally {
    await api.dispose();
  }
});

type PhraseFilter = {
  $state: { store: 'appState' };
  meta: { alias: null; disabled: false; negate: false; index: string; key: string; params: { query: string }; type: 'phrase' };
  query: { match_phrase: Record<string, string> };
};

/** A phrase filter pill on one field, in the `_a` URL state the case opens Discover with. */
function phraseFilter(key: string, value: string): PhraseFilter {
  return {
    $state: { store: 'appState' },
    meta: { alias: null, disabled: false, negate: false, index: LOGS_PATTERN_ID, key, params: { query: value }, type: 'phrase' },
    query: { match_phrase: { [key]: value } },
  };
}

/** Opens Discover on the logs index pattern over the seed window with a query and filter pills. */
async function openDiscover(
  page: Page,
  query: { language: 'lucene' | 'kuery'; query: string },
  filters: PhraseFilter[] = []
): Promise<void> {
  const window = seedWindow(seed);
  const globalState = toRison({ filters: [], time: { from: window.from, to: window.to } });
  const appState = toRison({ columns: ['_source'], filters, index: LOGS_PATTERN_ID, query, sort: [] });
  await page.goto(`/app/discover#/?_g=${encodeURIComponent(globalState)}&_a=${encodeURIComponent(appState)}`);
}

/** The response of the first browser search whose body satisfies `matches`. */
function waitForSearchResponse(page: Page, matches: (body: any) => boolean): Promise<any> {
  return page
    .waitForResponse(
      async (response) => {
        if (!response.url().includes('/internal/search/') || response.request().method() !== 'POST') {
          return false;
        }
        const body = await response.json().catch(() => null);
        return body !== null && matches(body);
      },
      { timeout: 60_000 }
    )
    .then((response) => response.json());
}

/** Rows of Discover's document table: one expand toggle cell per row. */
function discoverRows(page: Page) {
  return page.locator('td[data-test-subj="docTableExpandToggleColumn"]');
}

/** Runs one PPL query in the Workbench's PPL editor and returns the Workbench server's answer. */
async function runWorkbenchPpl(page: Page, query: string): Promise<any> {
  await page.goto('/app/opensearch-query-workbench#/');
  await page.getByTestId('switch-button-ppl').click();
  await page.locator('.ace_editor').first().click();
  await page.keyboard.press('ControlOrMeta+a');
  await page.keyboard.type(query);
  const isPplQuery = (url: string) => url.includes('/api/sql_console/pplquery');
  const answered = page.waitForResponse((response) => isPplQuery(response.url()));
  let isRunSent = false;
  page.on('request', (request) => {
    isRunSent ||= isPplQuery(request.url());
  });
  // Run is clicked, at most once a second, until the workbench sends the query: the cases pin
  // what it sends, not how soon after the typing a click takes effect.
  await expect
    .poll(
      async () => {
        if (!isRunSent) {
          await page.getByTestId('pplRunButton').click();
        }
        return isRunSent;
      },
      { message: 'the workbench sends the PPL query', intervals: [1_000], timeout: 30_000 }
    )
    .toBe(true);
  return (await answered).json();
}

test('osd boots and migrates against the default version', async ({ page, request }) => {
  const root = await icegateJson(request, 'GET', '/');
  expect(root.status).toBe(200);
  expect(root.body.version.distribution).toBe('opensearch');
  expect(root.body.version.number).toBe('3.9.0');

  const kibana = await icegateJson(request, 'GET', '/.kibana');
  expect(kibana.status).toBe(200);
  const [concreteIndex, index] = Object.entries(kibana.body)[0] as [string, { aliases: Record<string, unknown> }];
  expect(concreteIndex).toMatch(/^\.kibana_\d+$/);
  expect(Object.keys(index.aliases)).toContain('.kibana');

  // The case asserts that a page load leaves the config object of this version in the store.
  await page.goto('/app/home');
  await expect
    .poll(async () => (await icegateJson(request, 'GET', `/.kibana/_doc/config:${osdVersion}`)).body?.found, {
      message: `config:${osdVersion} written into the store`,
      timeout: 60_000,
    })
    .toBe(true);
});

test('advanced settings persist across a reload', async ({ page, request }) => {
  await page.goto('/app/management/opensearch-dashboards/settings');
  const sampleSize = page.getByTestId('advancedSetting-editField-discover:sampleSize');
  await sampleSize.fill('321');
  try {
    const saved = page.waitForResponse(
      (response) =>
        new URL(response.url()).pathname === '/api/opensearch-dashboards/settings' && response.request().method() === 'POST'
    );
    await page.getByTestId('advancedSetting-saveButton').click();
    expect((await saved).status()).toBe(200);

    await page.reload();
    await expect(page.getByTestId('advancedSetting-editField-discover:sampleSize')).toHaveValue('321');
    const config = await icegateJson(request, 'GET', `/.kibana/_doc/config:${osdVersion}`);
    expect(config.body._source.config['discover:sampleSize']).toBe(321);
  } finally {
    await setOsdSettings(request, { 'discover:sampleSize': null });
  }
});

test('saved objects export and import round-trip', async ({ request }) => {
  const created = await request.post('/api/saved_objects/_bulk_create?overwrite=true', {
    headers: OSD_HEADERS,
    data: [
      { type: 'index-pattern', id: 'e2e-roundtrip', attributes: { title: 'spans', timeFieldName: 'timestamp' } },
      {
        type: 'visualization',
        id: 'e2e-roundtrip-vis',
        attributes: {
          title: 'e2e span count',
          visState: JSON.stringify({
            title: 'e2e span count',
            type: 'metric',
            params: {},
            aggs: [{ id: '1', enabled: true, type: 'count', schema: 'metric', params: {} }],
          }),
          uiStateJSON: '{}',
          description: '',
          version: 1,
          kibanaSavedObjectMeta: {
            searchSourceJSON: JSON.stringify({
              query: { query: '', language: 'kuery' },
              filter: [],
              indexRefName: 'kibanaSavedObjectMeta.searchSourceJSON.index',
            }),
          },
        },
        references: [{ name: 'kibanaSavedObjectMeta.searchSourceJSON.index', type: 'index-pattern', id: 'e2e-roundtrip' }],
      },
    ],
  });
  expect(created.status(), await created.text()).toBe(200);

  const exported = await request.post('/api/saved_objects/_export', {
    headers: OSD_HEADERS,
    data: { objects: [{ type: 'visualization', id: 'e2e-roundtrip-vis' }], includeReferencesDeep: true },
  });
  expect(exported.status(), await exported.text()).toBe(200);
  const ndjson = await exported.text();
  const lines = ndjson
    .split('\n')
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line));
  const details = lines[lines.length - 1];
  expect(details.exportedCount).toBe(2);
  expect(details.missingRefCount).toBe(0);
  expect(lines.slice(0, -1).map((object) => `${object.type}/${object.id}`).sort()).toEqual([
    'index-pattern/e2e-roundtrip',
    'visualization/e2e-roundtrip-vis',
  ]);

  for (const object of ['visualization/e2e-roundtrip-vis', 'index-pattern/e2e-roundtrip']) {
    const deleted = await request.delete(`/api/saved_objects/${object}`, { headers: OSD_HEADERS });
    expect(deleted.status(), object).toBe(200);
    expect((await request.get(`/api/saved_objects/${object}`)).status(), object).toBe(404);
  }

  const imported = await request.post('/api/saved_objects/_import?overwrite=true', {
    headers: OSD_HEADERS,
    multipart: { file: { name: 'export.ndjson', mimeType: 'application/ndjson', buffer: Buffer.from(ndjson) } },
  });
  expect(imported.status(), await imported.text()).toBe(200);
  const importResult = await imported.json();
  expect(importResult.success).toBe(true);
  expect(importResult.successCount).toBe(2);

  const visualization = await request.get('/api/saved_objects/visualization/e2e-roundtrip-vis');
  expect(visualization.status()).toBe(200);
  expect((await visualization.json()).attributes.title).toBe('e2e span count');
  const pattern = await request.get('/api/saved_objects/index-pattern/e2e-roundtrip');
  expect((await pattern.json()).attributes.title).toBe('spans');
  const stored = await icegateJson(request, 'GET', '/.kibana/_doc/visualization:e2e-roundtrip-vis');
  expect(stored.body.found).toBe(true);
});

/** The fields OSD derives for an index pattern over `logs`, through its own field API. */
async function osdLogsFields(request: APIRequestContext): Promise<Array<Record<string, unknown>>> {
  const metaFields = ['_source', '_id', '_type', '_index', '_score'].map((field) => `meta_fields=${field}`).join('&');
  const response = await request.get(`/api/index_patterns/_fields_for_wildcard?pattern=logs&${metaFields}`);
  expect(response.status(), await response.text()).toBe(200);
  return (await response.json()).fields;
}

/** Asserts that OSD lists `name` among `fields` as a searchable, aggregatable keyword. */
function expectKeywordField(fields: Array<Record<string, unknown>>, name: string): void {
  expect(
    fields.find((candidate) => candidate.name === name),
    name
  ).toMatchObject({ type: 'string', esTypes: ['keyword'], searchable: true, aggregatable: true });
}

test('an index pattern over logs lists dotted map keys as flat fields', async ({ page, request }) => {
  const fields = await osdLogsFields(request);
  for (const name of ['log_attributes.http.method', 'log_attributes.http.route']) {
    expectKeywordField(fields, name);
  }
  expect(fields.find((candidate) => candidate.name === 'log_attributes')).toBeUndefined();

  await page.goto(`/app/management/opensearch-dashboards/indexPatterns/patterns/${LOGS_PATTERN_ID}`);
  await page.getByTestId('indexPatternFieldFilter').fill('log_attributes.http.route');
  await expect(page.locator('table').getByText('log_attributes.http.route', { exact: true })).toBeVisible();

  // The page's field refresh: registered after the page rendered its fields, so the response is
  // the refresh's own field request, not the page load's.
  const refreshed = page.waitForResponse(
    (response) =>
      response.url().includes('/api/index_patterns/_fields_for_wildcard') &&
      response.url().includes('pattern=logs') &&
      response.request().method() === 'GET',
    { timeout: 60_000 }
  );
  await page.getByTestId('refreshFieldsIndexPatternButton').click();
  await page.getByTestId('confirmModalConfirmButton').click();
  const refreshedResponse = await refreshed;
  expect(refreshedResponse.status()).toBe(200);
  expectKeywordField((await refreshedResponse.json()).fields, 'log_attributes.http.method');
  await page.getByTestId('indexPatternFieldFilter').fill('log_attributes.http.method');
  await expect(page.locator('table').getByText('log_attributes.http.method', { exact: true })).toBeVisible();
});

test('an index pattern over a key that is both a leaf and a prefix lists both', async ({ page, request }) => {
  const fields = await osdLogsFields(request);
  for (const name of ['log_attributes.http', 'log_attributes.http.method']) {
    expectKeywordField(fields, name);
  }

  await page.goto(`/app/management/opensearch-dashboards/indexPatterns/patterns/${LOGS_PATTERN_ID}`);
  await page.getByTestId('indexPatternFieldFilter').fill('log_attributes.http');
  const table = page.locator('table');
  await expect(table.getByText('log_attributes.http', { exact: true })).toBeVisible();
  await expect(table.getByText('log_attributes.http.method', { exact: true })).toBeVisible();
});

type ResolvedIndices = {
  indices: Array<{ name: string }>;
  aliases: Array<{ name: string }>;
  data_streams: Array<{ name: string }>;
};

/** Every name an `_resolve/index` answer lists: indices, aliases and data streams. */
function resolvedNames(answer: ResolvedIndices): string[] {
  return [...answer.indices, ...answer.aliases, ...answer.data_streams].map((entry) => entry.name);
}

/**
 * The next answer of OSD's resolve_index route whose URL does or does not carry
 * `expand_wildcards=all`, the parameter the "Include system and hidden indices" switch adds.
 */
function waitForResolvedIndices(page: Page, isIncludingHidden: boolean): Promise<ResolvedIndices> {
  return page
    .waitForResponse(
      (response) =>
        response.url().includes('/internal/index-pattern-management/resolve_index/') &&
        response.url().includes('expand_wildcards=all') === isIncludingHidden,
      { timeout: 60_000 }
    )
    .then((response) => response.json());
}

test('the index pattern wizard lists logs and offers timestamp as its time field', async ({ page }) => {
  // The resolve_index route's answer carries IceGate's `_resolve/index` answer; the assertions
  // below pin that it lists no dot-prefixed name until the switch adds `expand_wildcards=all`.
  await page.goto('/app/management/opensearch-dashboards/indexPatterns/create');
  const visibleAnswer = waitForResolvedIndices(page, false);
  await page.getByTestId('createIndexPatternNameInput').fill('*');
  const visibleNames = resolvedNames(await visibleAnswer);
  expect(visibleNames).toContain('logs');
  expect(visibleNames.filter((name) => name.startsWith('.'))).toEqual([]);
  const list = page.getByTestId('createIndexPatternStep1IndicesList');
  await expect(list.getByRole('cell', { name: 'logs', exact: true })).toBeVisible();
  await expect(list.getByRole('cell', { name: /^\./ })).toHaveCount(0);

  const hiddenAnswer = waitForResolvedIndices(page, true);
  await page.getByTestId('showSystemAndHiddenIndices').click();
  const hiddenNames = resolvedNames(await hiddenAnswer);
  expect(hiddenNames).toContain('logs');
  expect(hiddenNames).toContain('.kibana');
  await page.getByTestId('showSystemAndHiddenIndices').click();

  // `logs` is already the title of the suite's own pattern; `logs*` matches the one index `logs`.
  await page.getByTestId('createIndexPatternNameInput').fill('logs*');
  await expect(list.getByRole('cell', { name: 'logs', exact: true })).toBeVisible();
  await expect(list.getByRole('row')).toHaveCount(1);
  await page.getByTestId('createIndexPatternGoToStep2Button').click();
  await expect(page.getByTestId('createIndexPatternTimeFieldSelect').locator('option[value="timestamp"]')).toHaveCount(1);
});

test('discover shows rows and a histogram for a lucene query', async ({ page }) => {
  const histogram = waitForSearchResponse(page, (body) => body.rawResponse?.aggregations?.['2'] !== undefined);
  await openDiscover(page, { language: 'lucene', query: 'severity_text:ERROR' });

  await expect(page.getByTestId('discoverQueryHits')).toHaveText('2');
  await expect(discoverRows(page)).toHaveCount(2);
  await expect(page.getByTestId('docTable')).toContainText('payment declined');
  await expect(page.getByTestId('docTable')).toContainText('card expired');
  await expect(page.getByTestId('discoverChart')).toBeVisible();
  const buckets: Array<{ key: number; doc_count: number }> = (await histogram).rawResponse.aggregations['2'].buckets;
  expect(buckets.reduce((total, bucket) => total + bucket.doc_count, 0)).toBe(2);
});

test('discover shows rows for a dql query', async ({ page }) => {
  await openDiscover(page, { language: 'kuery', query: 'severity_text:WARN' });

  await expect(page.getByTestId('discoverQueryHits')).toHaveText('1');
  await expect(discoverRows(page)).toHaveCount(1);
  await expect(page.getByTestId('docTable')).toContainText('inventory low');
});

test('discover filters on a dotted map key through a filter pill', async ({ page }) => {
  await openDiscover(page, { language: 'kuery', query: '' }, [phraseFilter('log_attributes.http.method', 'POST')]);

  await expect(page.locator('[data-test-subj~="filter-key-log_attributes.http.method"]')).toBeVisible();
  await expect(page.getByTestId('discoverQueryHits')).toHaveText('3');
  for (const body of ['payment authorized', 'payment declined', 'card expired']) {
    await expect(page.getByTestId('docTable')).toContainText(body);
  }
});

test('discover filters on a key that is both a leaf and a prefix', async ({ page }) => {
  await openDiscover(page, { language: 'kuery', query: '' }, [
    phraseFilter('log_attributes.http', 'v1'),
    phraseFilter('log_attributes.http.method', 'GET'),
  ]);
  await expect(page.locator('[data-test-subj~="filter-key-log_attributes.http"]')).toBeVisible();
  await expect(page.getByTestId('discoverQueryHits')).toHaveText('1');
  await expect(page.getByTestId('docTable')).toContainText('order accepted');

  // `GET` is the value of `http.method`, never of `http`: a filter that confused the two keys
  // would match the three GET rows here.
  await openDiscover(page, { language: 'kuery', query: '' }, [phraseFilter('log_attributes.http', 'GET')]);
  await expect(page.getByTestId('discoverNoResults')).toBeVisible();
});

test('discover opens a single document and its context', async ({ page, request }) => {
  const found = await icegateJson(request, 'POST', '/logs/_search', {
    query: { match_phrase: { body: 'payment declined' } },
    size: 1,
  });
  expect(found.status).toBe(200);
  const id: string = found.body.hits.hits[0]._id;

  await page.goto(`/app/discover#/doc/${LOGS_PATTERN_ID}/logs?id=${encodeURIComponent(id)}`);
  await expect(page.getByTestId('doc-hit')).toContainText('payment declined');

  const globalState = encodeURIComponent(toRison({ filters: [] }));
  const appState = encodeURIComponent(toRison({ columns: ['_source'], filters: [] }));
  await page.goto(`/app/discover#/context/${LOGS_PATTERN_ID}/${encodeURIComponent(id)}?_g=${globalState}&_a=${appState}`);
  const context = page.getByTestId('dscDocsContent');
  for (const body of ALL_BODIES) {
    await expect(context).toContainText(body);
  }
});

test('a vertical bar visualization renders its buckets on a dashboard', async ({ page, request }) => {
  const window = seedWindow(seed);
  const created = await request.post('/api/saved_objects/_bulk_create?overwrite=true', {
    headers: OSD_HEADERS,
    data: [
      {
        type: 'visualization',
        id: 'e2e-severity-bars',
        attributes: {
          title: 'e2e severity bars',
          visState: JSON.stringify({
            title: 'e2e severity bars',
            type: 'histogram',
            params: {
              type: 'histogram',
              grid: { categoryLines: false },
              categoryAxes: [
                {
                  id: 'CategoryAxis-1',
                  type: 'category',
                  position: 'bottom',
                  show: true,
                  style: {},
                  scale: { type: 'linear' },
                  labels: { show: true, filter: true, truncate: 100 },
                  title: {},
                },
              ],
              valueAxes: [
                {
                  id: 'ValueAxis-1',
                  name: 'LeftAxis-1',
                  type: 'value',
                  position: 'left',
                  show: true,
                  style: {},
                  scale: { type: 'linear', mode: 'normal' },
                  labels: { show: true, rotate: 0, filter: false, truncate: 100 },
                  title: { text: 'Count' },
                },
              ],
              seriesParams: [
                {
                  show: true,
                  type: 'histogram',
                  mode: 'stacked',
                  data: { label: 'Count', id: '1' },
                  valueAxis: 'ValueAxis-1',
                  drawLinesBetweenPoints: true,
                  lineWidth: 2,
                  showCircles: true,
                },
              ],
              addTooltip: true,
              addLegend: true,
              legendPosition: 'right',
              times: [],
              addTimeMarker: false,
              labels: { show: false },
            },
            aggs: [
              { id: '1', enabled: true, type: 'count', schema: 'metric', params: {} },
              {
                id: '2',
                enabled: true,
                type: 'terms',
                schema: 'segment',
                params: {
                  field: 'severity_text',
                  orderBy: '1',
                  order: 'desc',
                  size: 5,
                  otherBucket: false,
                  otherBucketLabel: 'Other',
                  missingBucket: false,
                  missingBucketLabel: 'Missing',
                },
              },
            ],
          }),
          uiStateJSON: '{}',
          description: '',
          version: 1,
          kibanaSavedObjectMeta: {
            searchSourceJSON: JSON.stringify({
              query: { query: '', language: 'kuery' },
              filter: [],
              indexRefName: 'kibanaSavedObjectMeta.searchSourceJSON.index',
            }),
          },
        },
        references: [{ name: 'kibanaSavedObjectMeta.searchSourceJSON.index', type: 'index-pattern', id: LOGS_PATTERN_ID }],
      },
      {
        type: 'dashboard',
        id: 'e2e-severity-dashboard',
        attributes: {
          title: 'e2e severity dashboard',
          hits: 0,
          description: '',
          panelsJSON: JSON.stringify([
            {
              version: osdVersion,
              gridData: { x: 0, y: 0, w: 24, h: 15, i: '1' },
              panelIndex: '1',
              embeddableConfig: {},
              panelRefName: 'panel_0',
            },
          ]),
          optionsJSON: JSON.stringify({ hidePanelTitles: false, useMargins: true }),
          version: 1,
          timeRestore: true,
          timeFrom: window.from,
          timeTo: window.to,
          refreshInterval: { pause: true, value: 0 },
          kibanaSavedObjectMeta: {
            searchSourceJSON: JSON.stringify({ query: { query: '', language: 'kuery' }, filter: [] }),
          },
        },
        references: [{ name: 'panel_0', type: 'visualization', id: 'e2e-severity-bars' }],
      },
    ],
  });
  expect(created.status(), await created.text()).toBe(200);
  for (const object of (await created.json()).saved_objects) {
    expect(object.error, `${object.type}/${object.id}`).toBeUndefined();
  }

  const searched = waitForSearchResponse(page, (body) => body.rawResponse?.aggregations?.['2']?.buckets !== undefined);
  await page.goto('/app/dashboards#/view/e2e-severity-dashboard');
  const buckets: Array<{ key: string; doc_count: number }> = (await searched).rawResponse.aggregations['2'].buckets;
  expect(buckets.map((bucket) => [bucket.key, bucket.doc_count])).toEqual([
    ['INFO', 3],
    ['ERROR', 2],
    ['WARN', 1],
  ]);
  await expect(page.locator('[data-render-complete="true"]').first()).toBeAttached();
  await expect(page.getByTestId('embeddableErrorLabel')).toHaveCount(0);
  await expect(page.getByTestId('embeddableStackError')).toHaveCount(0);
});

test('workbench runs a ppl query, explains it and downloads csv', async ({ page, request }) => {
  const query = "source = logs | where severity_text = 'ERROR' | sort timestamp | fields body";
  const answer = await runWorkbenchPpl(page, query);
  expect(answer.data.ok).toBe(true);
  const jdbc = JSON.parse(answer.data.resp);
  expect(jdbc.schema).toEqual([{ name: 'body', type: 'string' }]);
  expect(jdbc.datarows).toEqual([['payment declined'], ['card expired']]);
  await expect(page.getByTestId('tableRow')).toHaveCount(2);

  const explained = page.waitForResponse((response) => response.url().includes('/api/sql_console/translateppl'));
  await page.getByRole('button', { name: 'Explain', exact: true }).click();
  const explanation = await (await explained).json();
  expect(explanation.data.ok).toBe(true);
  expect(typeof explanation.data.resp.calcite.logical).toBe('string');
  expect(explanation.data.resp.calcite.logical.length).toBeGreaterThan(0);

  // The case calls the Workbench server's csv and raw download routes directly, with the query
  // the editor ran.
  const csv = await request.post('/api/sql_console/pplcsv', { headers: OSD_HEADERS, data: { query } });
  expect(csv.status()).toBe(200);
  const download = await csv.json();
  expect(download.data.ok).toBe(true);
  expect(
    String(download.data.resp)
      .split(/\r?\n/)
      .filter((line) => line.length > 0)
  ).toEqual(['body', 'payment declined', 'card expired']);

  const raw = await request.post('/api/sql_console/ppltext', { headers: OSD_HEADERS, data: { query } });
  expect(raw.status()).toBe(200);
  const rawBody = await raw.json();
  expect(rawBody.data.ok).toBe(true);
  expect(
    String(rawBody.data.resp)
      .split(/\r?\n/)
      .filter((line) => line.length > 0)
  ).toEqual(['body', 'payment declined', 'card expired']);
});

test('workbench lists the index tree', async ({ page }) => {
  const listed = page.waitForResponse((response) => response.url().includes('/api/sql_console/sqlquery'));
  await page.goto('/app/opensearch-query-workbench#/');
  const answer = await (await listed).json();
  expect(answer.data.ok).toBe(true);
  const names: string[] = JSON.parse(answer.data.resp).datarows.map((row: unknown[]) => row[2]);
  expect(names).toEqual(expect.arrayContaining(['events', 'logs', 'metrics', 'operations', 'spans']));
  expect(names.filter((name) => name.startsWith('.'))).toEqual([]);

  const tree = page.getByTestId('opensearch-tree');
  await expect(tree).toContainText('logs');
  await expect(tree).toContainText('spans');
});

test('discover ppl renders documents and a histogram without highlight', async ({ page, request }) => {
  await setOsdSettings(request, { 'query:enhancements:enabled': true });
  try {
    const window = seedWindow(seed);
    const pplAnswer = (matches: (query: string) => boolean) =>
      page
        .waitForResponse(
          (response) =>
            response.url().includes('/api/enhancements/search/ppl') &&
            matches(String(response.request().postDataJSON()?.query?.query ?? '')),
          { timeout: 60_000 }
        )
        .then((response) => response.json());
    const documents = pplAnswer((query) => !query.includes('stats'));

    const appState = toRison({
      discover: { columns: ['_source'], sort: [] },
      metadata: { indexPattern: LOGS_PATTERN_ID, view: 'discover' },
    });
    const globalState = toRison({ filters: [], time: { from: window.from, to: window.to } });
    await page.goto(`/app/data-explorer/discover#?_a=${encodeURIComponent(appState)}&_g=${encodeURIComponent(globalState)}`);
    await page.getByTestId('queryEditorLanguageSelector').click();
    await page.getByTestId('languageSelectorMenuItem').filter({ hasText: 'PPL' }).click();
    await page.getByTestId('querySubmitButton').click();

    const frame = (await documents).body;
    expect(frame.size).toBe(6);
    const names: string[] = frame.fields.map((field: { name: string }) => field.name);
    expect(names).toContain('body');
    expect(names).not.toContain('_highlight');
    const bodies = frame.fields.find((field: { name: string }) => field.name === 'body').values;
    expect([...bodies].sort()).toEqual([...ALL_BODIES].sort());

    // The case asserts that the histogram's buckets arrive in this same response, under `aggs`,
    // as one entry; all six rows lie in the seed's first six seconds.
    const histogramBuckets = Object.values(frame.aggs ?? {}) as Array<Array<{ key: string; value: number }>>;
    expect(histogramBuckets).toHaveLength(1);
    expect(histogramBuckets[0].reduce((total, bucket) => total + bucket.value, 0)).toBe(6);
    await expect(discoverRows(page)).toHaveCount(6);
    await expect(page.getByTestId('discoverChart')).toBeVisible();
  } finally {
    await setOsdSettings(request, { 'query:enhancements:enabled': null });
  }
});

test('observability logs renders events and the count chart', async ({ page }) => {
  const pplAnswer = (matches: (body: { query: string; format: string }) => boolean) =>
    page
      .waitForResponse(
        (response) =>
          response.url().includes('/api/ppl/search') &&
          response.request().method() === 'POST' &&
          matches(response.request().postDataJSON()),
        { timeout: 60_000 }
      )
      .then((response) => response.json());
  const events = pplAnswer((body) => body.format === 'jdbc' && /^source\s*=\s*logs\b/.test(body.query) && !body.query.includes('stats'));
  const counts = pplAnswer((body) => body.format === 'viz' && body.query.includes('count()'));

  await page.goto('/app/observability-logs#/explorer');
  await page.getByTestId('searchAutocompleteTextArea').fill('source = logs');
  // The last 24 hours, set through the quick select form: it holds every seeded row.
  await page.getByTestId('superDatePickerToggleQuickMenuButton').click();
  const quickSelect = page.getByRole('group', { name: 'Quick select a time range' });
  await quickSelect.getByRole('combobox', { name: 'Time tense' }).selectOption('Last');
  await quickSelect.getByRole('spinbutton', { name: 'Time value' }).fill('24');
  await quickSelect.getByRole('combobox', { name: 'Time unit' }).selectOption('hours');
  await quickSelect.getByRole('button', { name: 'Apply', exact: true }).click();
  await page.getByTestId('superDatePickerApplyTimeButton').click();

  expect((await events).datarows).toHaveLength(6);
  const series: number[] = (await counts).data['count()'];
  expect(series.reduce((total, count) => total + count, 0)).toBe(6);
  await expect(page.getByTestId('eventExplorer__flyout')).toHaveCount(6);

  // A `where` on a dotted map key: three of the six logs carry `http.method` POST.
  const filtered = pplAnswer(
    (body) => body.format === 'jdbc' && body.query.includes('log_attributes.http.method') && !body.query.includes('stats')
  );
  await page.getByTestId('searchAutocompleteTextArea').fill("source = logs | where `log_attributes.http.method` = 'POST'");
  await page.getByTestId('superDatePickerApplyTimeButton').click();
  expect((await filtered).datarows).toHaveLength(3);
  await expect(page.getByTestId('eventExplorer__flyout')).toHaveCount(3);
  for (const body of ['payment authorized', 'payment declined', 'card expired']) {
    await expect(page.getByText(body).first()).toBeVisible();
  }
});

test('a ppl error shows its reason in the workbench', async ({ page }) => {
  const answer = await runWorkbenchPpl(page, "source = logs | where no_such_field = 'x'");

  expect(answer.data.ok).toBe(false);
  expect(String(answer.data.resp)).toContain('no_such_field');
  // The message panel, not the editor that still holds the query text.
  await expect(page.locator('.error-message')).toContainText('no_such_field');
});

test('a browser-sent x-scope-orgid cannot switch the tenant', async ({ page, request }) => {
  const intruder = 'e2e-intruder';
  const otherTenant = await icegateJson(request, 'GET', '/_cat/indices/logs?format=json&h=docs.count', undefined, intruder);
  expect(otherTenant.body).toEqual([{ 'docs.count': '0' }]);

  expect((await icegateJson(request, 'GET', '/.kibana', undefined, intruder)).status).toBe(404);
  expect(
    (await icegateJson(request, 'GET', `/.kibana/_doc/index-pattern:${LOGS_PATTERN_ID}`, undefined, intruder)).status
  ).toBe(404);

  await page.setExtraHTTPHeaders({ 'x-scope-orgid': intruder });
  await openDiscover(page, { language: 'lucene', query: '*' });
  await expect(page.getByTestId('discoverQueryHits')).toHaveText('6');

  const pattern = await request.get(`/api/saved_objects/index-pattern/${LOGS_PATTERN_ID}`, {
    headers: { 'x-scope-orgid': intruder },
  });
  expect(pattern.status()).toBe(200);
  expect((await pattern.json()).attributes.title).toBe('logs');
});
