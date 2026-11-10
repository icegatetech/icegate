/**
 * End-to-end cases that drive the stand's Grafana and its OpenSearch datasource plugin against
 * IceGate's OpenSearch listener, through the three provisioned OpenSearch datasources. Each case
 * pins one client behaviour that IceGate's READMEs and code cite by the case name; rename one only
 * together with every citation.
 *
 * Inputs are the rows seed.sh writes, the dashboards created below and the Explore queries; every
 * expected value is derived by hand from the seeded rows: six logs (3 INFO, 1 WARN, 2 ERROR) and
 * two traces. Trace A (5b8e...) has the root `GET /checkout` lasting 152175 us and the child
 * `authorize payment`, which links to trace B; trace B (0af7...) has the root `POST /refund`
 * lasting 735519 us and the failing child `charge card`.
 */
import { test, expect } from '@grafana/plugin-e2e';
import type { APIRequestContext, Page } from '@playwright/test';
import {
  GRAFANA_ADMIN_HEADERS,
  GRAFANA_PLUGIN_ID,
  GRAFANA_URL,
  METRIC_AGGREGATION_LABELS,
  exploreUrl,
  luceneMetricQuery,
  openMetricTypeOptions,
  queryEditorRow,
  readSeed,
  seedWindow,
  waitForGrafanaHealthy,
  type Seed,
} from './stand';

const LOGS_UID = 'opensearch-demo';
const SPANS_UID = 'opensearch-spans-demo';
const TRACES_UID = 'opensearch-traces-demo';
const TRACE_A = '5b8efff798038103d269b633813fc60c';
const TRACE_B = '0af7651916cd43dd8448eb211c80319c';
const ROOT_SPAN_A = 'eee19b7ec3c1b174';
const CHILD_SPAN_A = 'eee19b7ec3c1b175';

type Frame = { schema?: { name?: string; fields?: Array<{ name: string }> }; data?: { values?: unknown[][] } };
type QueryResult = { frames?: Frame[]; error?: string };
type QueryResponseBody = { results?: Record<string, QueryResult> };
type QueryRequestBody = {
  queries: Array<{ refId: string; datasource?: { uid?: string }; queryType?: string; query?: string; format?: string }>;
};
/** One `/api/ds/query` exchange: the response is kept as received, so a failure can show it. */
type QueryExchange = { request: QueryRequestBody; status: number; text: string };

let seed: Seed;

const datasource = (uid: string) => ({ type: GRAFANA_PLUGIN_ID, uid });

const luceneLogs = (query: string) => ({
  queryType: 'lucene',
  luceneQueryType: 'Logs',
  query,
  metrics: [{ id: '1', type: 'logs' }],
  bucketAggs: [],
});

const luceneRawData = (query: string) => ({
  queryType: 'lucene',
  luceneQueryType: 'RawData',
  query,
  metrics: [{ id: '1', type: 'raw_data', settings: { size: '500' } }],
  bucketAggs: [],
});

const luceneTraces = (query: string) => ({
  queryType: 'lucene',
  luceneQueryType: 'Traces',
  query,
  metrics: [{ id: '1', type: 'count' }],
  bucketAggs: [{ id: '2', type: 'date_histogram', settings: { interval: 'auto' } }],
});

const ppl = (query: string, format: 'table' | 'logs' | 'time_series') => ({ queryType: 'PPL', query, format });

function panel(id: number, title: string, type: string, uid: string, target: Record<string, unknown>) {
  return {
    id,
    title,
    type,
    gridPos: { x: (id - 1) * 8, y: 0, w: 8, h: 8 },
    datasource: datasource(uid),
    targets: [{ refId: 'A', datasource: datasource(uid), ...target }],
    fieldConfig: { defaults: {}, overrides: [] },
    options: {},
  };
}

async function saveDashboard(
  api: APIRequestContext,
  uid: string,
  panels: unknown[],
  variables: unknown[] = []
): Promise<void> {
  const timeRange = seedWindow(seed);
  const response = await api.post('/api/dashboards/db', {
    headers: GRAFANA_ADMIN_HEADERS,
    data: {
      overwrite: true,
      dashboard: {
        uid,
        title: uid,
        schemaVersion: 39,
        time: { from: timeRange.from, to: timeRange.to },
        templating: { list: variables },
        panels,
      },
    },
  });
  expect(response.status(), await response.text()).toBe(200);
}

/** Collects every `/api/ds/query` exchange of the page from now on. */
function collectQueryData(page: Page): QueryExchange[] {
  const exchanges: QueryExchange[] = [];
  page.on('response', async (response) => {
    if (!response.url().includes('/api/ds/query') || response.request().method() !== 'POST') {
      return;
    }
    const request = response.request().postDataJSON() as QueryRequestBody;
    // A body that cannot be read (for example after a navigation) is recorded as such, never as
    // an empty answer, so the case fails with the real cause.
    const text = await response.text().catch((error: Error) => `unreadable: ${error.message}`);
    exchanges.push({ request, status: response.status(), text });
  });
  return exchanges;
}

/** Waits, at most 60 s, for an exchange whose request satisfies `matches`. */
async function findExchange(
  exchanges: QueryExchange[],
  matches: (request: QueryRequestBody) => boolean,
  description: string
): Promise<QueryExchange> {
  // The polled value lists the requests seen so far, so a timeout tells a predicate miss apart
  // from Grafana sending nothing.
  await expect
    .poll(
      () =>
        exchanges.some((exchange) => matches(exchange.request))
          ? 'found'
          : `seen: ${JSON.stringify(exchanges.map((exchange) => exchange.request.queries))}`,
      { message: description, timeout: 60_000 }
    )
    .toBe('found');
  return exchanges.find((exchange) => matches(exchange.request)) as QueryExchange;
}

/** The answer of an exchange; a body that is not JSON fails with its status and text. */
function responseBody(exchange: QueryExchange): QueryResponseBody {
  try {
    return JSON.parse(exchange.text) as QueryResponseBody;
  } catch (error) {
    throw new Error(`status ${exchange.status}: ${exchange.text}`, { cause: error });
  }
}

/** The single query result of an exchange, which must carry no error. */
function onlyResult(exchange: QueryExchange): QueryResult {
  const results = Object.values(responseBody(exchange).results ?? {});
  expect(results, `status ${exchange.status}: ${exchange.text}`).toHaveLength(1);
  expect(results[0].error, results[0].error).toBeUndefined();
  return results[0];
}

/** A frame's rows as objects keyed by field name. */
function frameRows(frame: Frame): Array<Record<string, unknown>> {
  const names = (frame.schema?.fields ?? []).map((field) => field.name);
  const columns = frame.data?.values ?? [];
  const rowCount = columns[0]?.length ?? 0;
  return Array.from({ length: rowCount }, (_, row) =>
    Object.fromEntries(names.map((name, column) => [name, columns[column]?.[row]]))
  );
}

function resultRows(result: QueryResult): Array<Record<string, unknown>> {
  return (result.frames ?? []).flatMap(frameRows);
}

/** Opens a trace by id in Explore on the traces datasource and returns its span rows. */
async function openTraceById(page: Page, traceId: string): Promise<Array<Record<string, unknown>>> {
  const exchanges = collectQueryData(page);
  await page.goto(exploreUrl(TRACES_UID, luceneTraces(`traceId:${traceId}`), seedWindow(seed)));
  const trace = await findExchange(exchanges, (request) => request.queries[0]?.refId === 'A', `trace ${traceId}`);
  return resultRows(onlyResult(trace));
}

test.beforeAll(async ({ playwright }) => {
  seed = readSeed();
  const api = await playwright.request.newContext({ baseURL: GRAFANA_URL });
  try {
    // seed.sh waits for IceGate only; Grafana may still be starting on a cold stand.
    await waitForGrafanaHealthy(api, 300_000);
    await saveDashboard(
      api,
      'e2e-variables',
      [
        panel(1, 'Lucene by level', 'table', LOGS_UID, luceneRawData('severity_text:$level')),
        panel(2, 'PPL by level', 'table', LOGS_UID, ppl("source = logs | where severity_text = '$level' | fields body", 'table')),
      ],
      [
        {
          type: 'query',
          name: 'level',
          datasource: datasource(LOGS_UID),
          query: '{"find": "terms", "field": "severity_text"}',
          refresh: 1,
          regex: '',
          sort: 1,
          multi: false,
          includeAll: false,
          current: { selected: true, text: 'WARN', value: 'WARN' },
          options: [],
          hide: 0,
        },
      ]
    );
    await saveDashboard(
      api,
      'e2e-adhoc',
      [
        panel(1, 'Lucene filtered', 'table', LOGS_UID, luceneRawData('*')),
        panel(2, 'PPL filtered', 'table', LOGS_UID, ppl('source = logs | fields severity_text, body', 'table')),
      ],
      [
        {
          type: 'adhoc',
          name: 'filters',
          datasource: datasource(LOGS_UID),
          filters: [{ key: 'severity_text', operator: '=', value: 'ERROR', condition: '' }],
          hide: 0,
        },
      ]
    );
    await saveDashboard(api, 'e2e-span-metrics', [
      panel(1, 'Span duration', 'timeseries', SPANS_UID, {
        queryType: 'lucene',
        luceneQueryType: 'Metric',
        query: '*',
        timeField: 'timestamp',
        metrics: [
          { id: '1', type: 'avg', field: 'duration_micros' },
          { id: '3', type: 'max', field: 'duration_micros' },
          {
            id: '4',
            type: 'bucket_script',
            pipelineVariables: [
              { name: 'var1', pipelineAgg: '1' },
              { name: 'var2', pipelineAgg: '3' },
            ],
            settings: { script: 'params.var1 / params.var2' },
          },
          { id: '5', type: 'moving_avg', field: '1', settings: { model: 'simple', window: 5 } },
        ],
        bucketAggs: [{ id: '2', type: 'date_histogram', field: 'timestamp', settings: { interval: '1h', min_doc_count: '0' } }],
      }),
    ]);
    await saveDashboard(api, 'e2e-ppl-panels', [
      panel(1, 'PPL errors', 'logs', LOGS_UID, ppl("source = logs | where severity_text = 'ERROR' | sort timestamp | fields timestamp, body", 'logs')),
      panel(2, 'PPL per minute', 'timeseries', LOGS_UID, ppl('source = logs | stats count() by span(timestamp, 1m)', 'time_series')),
    ]);
    await saveDashboard(
      api,
      'e2e-multi-value',
      [
        panel(1, 'PPL in levels', 'table', LOGS_UID, ppl('source = logs | where severity_text in (${levels:singlequote}) | sort timestamp | fields body', 'table')),
      ],
      [
        {
          type: 'custom',
          name: 'levels',
          query: 'ERROR,WARN,INFO',
          multi: true,
          includeAll: false,
          current: { selected: true, text: ['ERROR', 'WARN'], value: ['ERROR', 'WARN'] },
          options: [
            { selected: true, text: 'ERROR', value: 'ERROR' },
            { selected: true, text: 'WARN', value: 'WARN' },
            { selected: false, text: 'INFO', value: 'INFO' },
          ],
          hide: 0,
        },
      ]
    );
    await saveDashboard(
      api,
      'e2e-field-keys',
      [],
      [
        {
          type: 'query',
          name: 'keys',
          datasource: datasource(LOGS_UID),
          query: '{"find": "fields", "type": "keyword"}',
          regex: '/^log_attributes\\.http(?:\\.method)?$/',
          refresh: 1,
          sort: 1,
          multi: false,
          includeAll: false,
          current: {},
          options: [],
          hide: 0,
        },
      ]
    );
  } finally {
    await api.dispose();
  }
});

test('grafana save and test passes on the logs datasource', async ({ gotoDataSourceConfigPage, page }) => {
  await gotoDataSourceConfigPage(LOGS_UID);
  // Matches either button label of the datasource page.
  await page.getByRole('button', { name: /^(Save & test|Test)$/ }).click();
  await expect(page.getByText('Index OK. Time field name OK.')).toBeVisible();
});

test('grafana backend health passes on every opensearch datasource', async ({ request }) => {
  for (const uid of [LOGS_UID, SPANS_UID, TRACES_UID]) {
    const response = await request.get(`/api/datasources/uid/${uid}/health`, { headers: GRAFANA_ADMIN_HEADERS });
    const text = await response.text();
    expect(response.status(), `${uid}: ${text}`).toBe(200);
    expect(JSON.parse(text).status, uid).toBe('OK');
  }
});

test('grafana renders a lucene logs query with its logs volume', async ({ page }) => {
  const exchanges = collectQueryData(page);
  await page.goto(exploreUrl(LOGS_UID, luceneLogs('severity_text:ERROR'), seedWindow(seed)));

  const logs = await findExchange(exchanges, (request) => request.queries.some((query) => query.refId === 'A'), 'the logs query');
  expect(resultRows(onlyResult(logs)).map((row) => row.body).sort()).toEqual(['card expired', 'payment declined']);

  const volume = await findExchange(
    exchanges,
    (request) => request.queries.some((query) => query.refId.startsWith('log-volume')),
    'the logs volume query'
  );
  const volumeCounts = (onlyResult(volume).frames ?? []).flatMap((frame) => (frame.data?.values?.[1] ?? []) as number[]);
  expect(volumeCounts.reduce((total, count) => total + (count ?? 0), 0)).toBe(2);
  await expect(page.getByText('payment declined').first()).toBeVisible();
});

test('template variables from a lucene find query feed lucene and ppl panels', async ({ page }) => {
  const exchanges = collectQueryData(page);
  const terms = page
    .waitForResponse((response) => response.url().includes(`/api/datasources/uid/${LOGS_UID}/resources/_msearch`), {
      timeout: 60_000,
    })
    .then((response) => response.json());
  await page.goto('/d/e2e-variables?orgId=1&var-level=WARN');

  const buckets: Array<{ key: string }> = (await terms).responses[0].aggregations['1'].buckets;
  expect(buckets.map((bucket) => bucket.key).sort()).toEqual(['ERROR', 'INFO', 'WARN']);

  const lucene = await findExchange(exchanges, (request) => request.queries[0]?.queryType === 'lucene', 'the Lucene panel');
  expect(lucene.request.queries[0].query).toBe('severity_text:WARN');
  expect(resultRows(onlyResult(lucene)).map((row) => row.body)).toEqual(['inventory low']);

  const pplPanel = await findExchange(exchanges, (request) => request.queries[0]?.queryType === 'PPL', 'the PPL panel');
  expect(pplPanel.request.queries[0].query).toBe("source = logs | where severity_text = 'WARN' | fields body");
  expect(resultRows(onlyResult(pplPanel)).map((row) => row.body)).toEqual(['inventory low']);
});

test('ad-hoc filters apply to lucene and ppl panels', async ({ page }) => {
  const exchanges = collectQueryData(page);
  await page.goto('/d/e2e-adhoc?orgId=1');

  const lucene = await findExchange(exchanges, (request) => request.queries[0]?.queryType === 'lucene', 'the Lucene panel');
  expect(lucene.request.queries[0].query).toContain('ERROR');
  expect(resultRows(onlyResult(lucene)).map((row) => row.body).sort()).toEqual(['card expired', 'payment declined']);

  const pplPanel = await findExchange(exchanges, (request) => request.queries[0]?.queryType === 'PPL', 'the PPL panel');
  expect(pplPanel.request.queries[0].query).toContain('severity_text');
  expect(pplPanel.request.queries[0].query).toContain("'ERROR'");
  const rows = resultRows(onlyResult(pplPanel));
  expect(rows.map((row) => row.severity_text)).toEqual(['ERROR', 'ERROR']);
  expect(rows.map((row) => row.body).sort()).toEqual(['card expired', 'payment declined']);
});

test('metrics editor bucket_script and moving_avg panels render', async ({ page }) => {
  const exchanges = collectQueryData(page);
  await page.goto('/d/e2e-span-metrics?orgId=1');

  const metrics = await findExchange(exchanges, (request) => request.queries[0]?.queryType === 'lucene', 'the metrics panel');
  const frames = onlyResult(metrics).frames ?? [];
  const bucketScript = frames.find((frame) => JSON.stringify(frame.schema).includes('Bucket Script'));
  const movingAverage = frames.find((frame) => JSON.stringify(frame.schema).includes('Moving Average'));
  expect(bucketScript, JSON.stringify(frames.map((frame) => frame.schema))).toBeDefined();
  expect(movingAverage, JSON.stringify(frames.map((frame) => frame.schema))).toBeDefined();
  // Every span starts in the first four seconds after one minute boundary, so one 1h bucket holds all four:
  // avg(duration_micros) = (152175 + 100000 + 735519 + 500000) / 4 = 371923.5, max = 735519.
  const ratios = ((bucketScript as Frame).data?.values?.[1] ?? []).filter((value) => value !== null) as number[];
  expect(ratios).toHaveLength(1);
  expect(ratios[0]).toBeCloseTo(371923.5 / 735519, 9);
  await expect(page.getByTestId('data-testid Panel status error')).toHaveCount(0);
});

test('grafana field pickers and ad-hoc filter keys list dotted map keys', async ({ gotoVariableEditPage, page }) => {
  const variableEditPage = await gotoVariableEditPage({ dashboard: { uid: 'e2e-field-keys' }, id: '0' });
  await variableEditPage.runQuery();
  await expect(variableEditPage).toDisplayPreviews(['log_attributes.http', 'log_attributes.http.method']);

  // The field picker of a terms bucket aggregation.
  await page.goto(
    exploreUrl(
      LOGS_UID,
      {
        queryType: 'lucene',
        luceneQueryType: 'Metric',
        query: '*',
        metrics: [{ id: '1', type: 'count' }],
        bucketAggs: [
          {
            id: '2',
            type: 'terms',
            field: 'severity_text',
            settings: { size: '10', order: 'desc', orderBy: '_count', min_doc_count: '1' },
          },
        ],
      },
      seedWindow(seed)
    )
  );
  await queryEditorRow(page).getByText('severity_text', { exact: true }).first().click();
  await page.keyboard.type('log_attributes.http');
  await expect(page.getByRole('option').filter({ hasText: /^log_attributes\.http$/ })).toBeVisible();
  await expect(page.getByRole('option').filter({ hasText: /^log_attributes\.http\.method$/ })).toBeVisible();

  // The ad-hoc filter keys.
  await page.goto('/d/e2e-adhoc?orgId=1');
  await page.getByPlaceholder('+ label = value').click();
  await page.keyboard.type('log_attributes.http');
  await expect(page.getByRole('option').filter({ hasText: /^log_attributes\.http$/ })).toBeVisible();
  await expect(page.getByRole('option').filter({ hasText: /^log_attributes\.http\.method$/ })).toBeVisible();
});

test('grafana metrics editor on the spans datasource offers the metric aggregations', async ({ page }) => {
  await page.goto(exploreUrl(SPANS_UID, luceneMetricQuery(), seedWindow(seed)));
  await openMetricTypeOptions(page);
  for (const label of METRIC_AGGREGATION_LABELS) {
    await expect(page.getByRole('option').filter({ hasText: new RegExp(`^${label}$`) }), label).toBeVisible();
  }
});

test('grafana renders a ppl logs query and a ppl time series', async ({ page }) => {
  const exchanges = collectQueryData(page);
  await page.goto('/d/e2e-ppl-panels?orgId=1');

  const logs = await findExchange(exchanges, (request) => request.queries[0]?.format === 'logs', 'the PPL logs panel');
  expect(resultRows(onlyResult(logs)).map((row) => row.body)).toEqual(['payment declined', 'card expired']);

  const series = await findExchange(exchanges, (request) => request.queries[0]?.format === 'time_series', 'the PPL time series');
  const frames = onlyResult(series).frames ?? [];
  expect(frames).toHaveLength(1);
  // All six logs lie in the seed's base minute.
  expect(frames[0].data?.values?.[0]).toEqual([seed.baseMs]);
  expect(frames[0].data?.values?.[1]).toEqual([6]);
  await expect(page.getByTestId('data-testid Panel status error')).toHaveCount(0);
});

test('a multi-value variable interpolates into a ppl panel', async ({ page }) => {
  const exchanges = collectQueryData(page);
  await page.goto('/d/e2e-multi-value?orgId=1&var-levels=ERROR&var-levels=WARN');

  const pplPanel = await findExchange(exchanges, (request) => request.queries[0]?.queryType === 'PPL', 'the PPL panel');
  expect(pplPanel.request.queries[0].query).toBe(
    "source = logs | where severity_text in ('ERROR','WARN') | sort timestamp | fields body"
  );
  expect(resultRows(onlyResult(pplPanel)).map((row) => row.body)).toEqual([
    'inventory low',
    'payment declined',
    'card expired',
  ]);
});

test('grafana opens a trace by id through the traceId input form', async ({ page }) => {
  const spans = await openTraceById(page, TRACE_A);
  expect(spans.map((span) => span.spanID).sort()).toEqual([ROOT_SPAN_A, CHILD_SPAN_A]);
  await expect(page.getByTestId('data-testid SpanBar--wrapper')).toHaveCount(2);
});

test('the grafana trace list shows no null latency or error count', async ({ page }) => {
  const exchanges = collectQueryData(page);
  await page.goto(exploreUrl(TRACES_UID, luceneTraces(''), seedWindow(seed)));

  const list = await findExchange(exchanges, (request) => request.queries[0]?.refId === 'A', 'the trace list query');
  const frame = (onlyResult(list).frames ?? []).find((candidate) => candidate.schema?.name === 'Trace List');
  expect(frame, list.text).toBeDefined();
  const rows = frameRows(frame as Frame)
    .map((row) => [row['Trace Id'], row['Trace Group'], row['Latency (ms)'], row['Error Count']])
    .sort((left, right) => String(left[0]).localeCompare(String(right[0])));
  // Latency: floor(root duration in ns / 10^4) / 100 ms; the error count counts a trace's spans
  // when its root's status code is 2, and both seeded roots are unset.
  expect(rows).toEqual([
    [TRACE_B, 'POST /refund', 735.51, 0],
    [TRACE_A, 'GET /checkout', 152.17, 0],
  ]);
});

test('a failing span shows no error flag and a span with links renders', async ({ page }) => {
  const traceB = await openTraceById(page, TRACE_B);
  const chargeCard = traceB.find((span) => span.operationName === 'charge card');
  expect(chargeCard, JSON.stringify(traceB)).toBeDefined();
  // The span must carry a tags array, so the absent error key is read, not assumed.
  expect(Object.keys(chargeCard as object)).toContain('tags');
  expect(Array.isArray(chargeCard?.tags), JSON.stringify(chargeCard)).toBe(true);
  expect((chargeCard?.tags as Array<{ key: string }>).map((tag) => tag.key)).not.toContain('error');
  await expect(page.getByTestId('data-testid SpanBar--wrapper')).toHaveCount(2);

  const traceA = await openTraceById(page, TRACE_A);
  expect(traceA.map((span) => span.operationName).sort()).toEqual(['GET /checkout', 'authorize payment']);
  await expect(page.getByTestId('data-testid SpanBar--wrapper')).toHaveCount(2);
  await expect(page.getByText('authorize payment').first()).toBeVisible();
});

test('trace-to-logs opens the logs of the span', async ({ page }) => {
  await openTraceById(page, TRACE_A);
  const exchanges = collectQueryData(page);
  await page.getByRole('switch').filter({ hasText: 'GET /checkout' }).click();
  await page.getByText('Logs for this span').first().click();

  const logs = await findExchange(
    exchanges,
    (request) => request.queries[0]?.datasource?.uid === LOGS_UID && request.queries[0]?.refId === 'A',
    'the logs of the span'
  );
  const query = String(logs.request.queries[0].query);
  expect(query).toContain(`trace_id:"${TRACE_A}"`);
  expect(query).toContain(`span_id:"${ROOT_SPAN_A}"`);
  expect(resultRows(onlyResult(logs)).map((row) => row.body)).toEqual(['order accepted']);
});
