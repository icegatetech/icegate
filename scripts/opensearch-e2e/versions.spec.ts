/**
 * End-to-end cases about the version IceGate reports. Each case owns the whole stand: it resets
 * it (`docker compose down -v`), starts the services it needs with the variants it names, and
 * asserts the state on both sides of the change it makes. The operator note beside
 * `query.opensearch.version` in config/helm/icegate/values.yaml cites the strict-mode case.
 */
import { execFileSync } from 'node:child_process';
import { appendFileSync, cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test, expect, type APIRequestContext } from '@playwright/test';
import {
  GRAFANA_ADMIN_HEADERS,
  GRAFANA_URL,
  ICEGATE_URL,
  METRIC_AGGREGATION_LABELS,
  OSD_HEADERS,
  OSD_URL,
  REPO_ROOT,
  TENANT,
  exploreUrl,
  icegateJson,
  listenerPort,
  luceneMetricQuery,
  openMetricTypeOptions,
  readComposeModel,
  readStandConfigValue,
  runCompose,
  setOsdSettings,
  waitForGrafanaHealthy,
  waitForOsdAvailable,
} from './stand';

const VARIANT_DIR = path.join(__dirname, '.stand', 'variants');
/** The profiles of the Dashboards variants: `down` and `logs` must see those services too. */
const VARIANT_PROFILES = ['--profile', 'upgrade', '--profile', 'strict'];

/**
 * The version IceGate reports with the stand's own query document, read from `GET /` in
 * `beforeAll`; the stand's Grafana datasources state the same version.
 */
let defaultVersion: string;

/** Where the version cases keep their stands' logs: beside the Playwright output CI uploads. */
const STAND_LOG = path.join(__dirname, 'test-results', 'stand-versions.log');

/**
 * Appends the stand's logs to `STAND_LOG`, then removes the stand and its volumes, so a failed
 * case's evidence survives the reset the next case starts with.
 */
function resetStand(): void {
  try {
    const logs = runCompose([...VARIANT_PROFILES, 'logs', '--no-color', '--timestamps']);
    mkdirSync(path.dirname(STAND_LOG), { recursive: true });
    appendFileSync(STAND_LOG, logs);
  } finally {
    runCompose([...VARIANT_PROFILES, 'down', '-v', '--remove-orphans']);
  }
}

/**
 * Writes a copy of the stand's query document whose `opensearch.version` is `version` and returns
 * its absolute path. Only the `version` line of the one top-level `opensearch:` block changes.
 */
function writeQueryConfigVariant(version: string): string {
  const lines = readFileSync(path.join(REPO_ROOT, 'config/docker/query.yaml'), 'utf8').split('\n');
  const header = lines.indexOf('opensearch:');
  if (header === -1 || lines.indexOf('opensearch:', header + 1) !== -1) {
    throw new Error('config/docker/query.yaml must hold exactly one top-level `opensearch:` block');
  }
  let end = header + 1;
  while (end < lines.length && lines[end].startsWith('  ')) {
    end += 1;
  }
  const block = lines.slice(header + 1, end).filter((line) => !line.startsWith('  version:'));
  const variant = [...lines.slice(0, header + 1), `  version: "${version}"`, ...block, ...lines.slice(end)];
  mkdirSync(VARIANT_DIR, { recursive: true });
  const target = path.join(VARIANT_DIR, `query-${version}.yaml`);
  writeFileSync(target, variant.join('\n'));
  return target;
}

/**
 * Writes a copy of the stand's Grafana provisioning directory whose three OpenSearch datasources
 * state `version` instead of the default, and returns its absolute path.
 */
function writeGrafanaProvisioningVariant(version: string): string {
  const target = path.join(VARIANT_DIR, `grafana-provisioning-${version}`);
  rmSync(target, { recursive: true, force: true });
  cpSync(path.join(REPO_ROOT, 'config/docker/grafana/provisioning'), target, { recursive: true });
  const datasources = path.join(target, 'datasources', 'datasources.yaml');
  const text = readFileSync(datasources, 'utf8');
  const escapedDefault = defaultVersion.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const versionLine = new RegExp(`^(\\s+version: )"${escapedDefault}"$`, 'gm');
  const labelLine = new RegExp(`^(\\s+versionLabel: )"OpenSearch ${escapedDefault}"$`, 'gm');
  const versionCount = text.match(versionLine)?.length ?? 0;
  const labelCount = text.match(labelLine)?.length ?? 0;
  if (versionCount !== 3 || labelCount !== 3) {
    throw new Error(`expected three OpenSearch datasources stating ${defaultVersion}, found ${versionCount} versions and ${labelCount} labels`);
  }
  writeFileSync(
    datasources,
    text.replace(versionLine, `$1"${version}"`).replace(labelLine, `$1"OpenSearch ${version}"`)
  );
  return target;
}

/** When a service's container started and how often Docker restarted it. */
function readContainerState(service: string): { startedAt: string; restartCount: string; status: string } {
  const id = runCompose(['ps', '-q', service]).trim();
  const [startedAt, restartCount, status] = execFileSync(
    'docker',
    ['inspect', '-f', '{{.State.StartedAt}}|{{.RestartCount}}|{{.State.Status}}', id],
    { encoding: 'utf8' }
  )
    .trim()
    .split('|');
  return { startedAt, restartCount, status };
}

/**
 * IceGate's answer to `GET /`: `version <number>` on a 200, otherwise `status <code>` or
 * `unreachable: <request error>`. The cause stays in the value, so a poll's timeout reports it.
 */
async function readIceGateVersion(request: APIRequestContext): Promise<string> {
  const response = await request
    .get(`${ICEGATE_URL}/`, { headers: { 'x-scope-orgid': TENANT } })
    .catch((error: Error) => error);
  if (response instanceof Error) {
    return `unreachable: ${response.message}`;
  }
  if (response.status() !== 200) {
    return `status ${response.status()}`;
  }
  return `version ${(await response.json()).version.number}`;
}

/** Waits, at most `timeoutMs`, until IceGate's `GET /` reports `version`. */
async function waitForIceGateVersion(request: APIRequestContext, version: string, timeoutMs: number): Promise<void> {
  await expect
    .poll(() => readIceGateVersion(request), {
      message: `IceGate reporting ${version}`,
      timeout: timeoutMs,
      intervals: [2_000],
    })
    .toBe(`version ${version}`);
}

/**
 * How many `GET /_nodes` requests the running query container's OpenSearch listener has
 * counted, read from its Prometheus endpoint. `count` is 0 while that endpoint does not answer;
 * `cause` then says why (no published port, a request error, or a non-200 status).
 */
async function countNodesRequests(request: APIRequestContext): Promise<{ count: number; cause: string }> {
  let address: string;
  try {
    address = runCompose(['port', 'query', String(listenerPort('query.yaml', 'metrics'))]).trim();
  } catch (error) {
    return { count: 0, cause: `no published metrics port: ${(error as Error).message}` };
  }
  const metricsPath = readStandConfigValue('query.yaml', 'metrics', 'path');
  if (metricsPath === undefined) {
    throw new Error('config/docker/query.yaml sets no metrics.path');
  }
  const response = await request
    .get(`http://127.0.0.1:${address.slice(address.lastIndexOf(':') + 1)}${metricsPath}`, { timeout: 5_000 })
    .catch((error: Error) => error);
  if (response instanceof Error) {
    return { count: 0, cause: `unreachable: ${response.message}` };
  }
  if (response.status() !== 200) {
    return { count: 0, cause: `status ${response.status()}` };
  }
  const count = (await response.text())
    .split('\n')
    .filter(
      (line) =>
        line.startsWith('icegate_query_requests_total{') &&
        line.includes('api="opensearch"') &&
        line.includes('endpoint="/_nodes"')
    )
    .reduce((total, line) => total + Number(line.slice(line.lastIndexOf(' ') + 1)), 0);
  return { count, cause: 'status 200' };
}

/**
 * The strict Dashboards' answer to `/api/status`: `status <code>`, or `unreachable: <request
 * error>` while it does not answer, so a failed poll or probe reports the cause.
 */
async function readStrictStatus(request: APIRequestContext): Promise<string> {
  const response = await request
    .get(`${OSD_URL}/api/status?v8format=true`, { timeout: 5_000 })
    .catch((error: Error) => error);
  return response instanceof Error ? `unreachable: ${response.message}` : `status ${response.status()}`;
}

/**
 * The one concrete index behind the `.kibana` alias in a `GET /.kibana` body; fails unless the
 * body names exactly one index and that index carries the alias.
 */
function findKibanaAliasTarget(body: Record<string, { aliases: Record<string, unknown> }>): string {
  const entries = Object.entries(body);
  expect(
    entries.map(([name]) => name),
    'concrete indices behind GET /.kibana'
  ).toHaveLength(1);
  const [concreteIndex, index] = entries[0];
  expect(Object.keys(index.aliases)).toContain('.kibana');
  return concreteIndex;
}

test.beforeAll(async ({ playwright }) => {
  // Compose cannot read query.yaml, so the e2e override copies its metrics port; this pins the
  // copy, so a drift fails here by name instead of leaving the `/_nodes` counter at 0.
  expect(
    (readComposeModel().services.query?.ports ?? []).filter(
      (port) => port.target === listenerPort('query.yaml', 'metrics')
    ),
    'docker-compose.e2e.yml must expose query.yaml metrics.port on the query service'
  ).toHaveLength(1);
  resetStand();
  runCompose(['up', '-d', 'query'], { ICEGATE_E2E_QUERY_CONFIG: undefined, GRAFANA_E2E_PROVISIONING: undefined });
  const api = await playwright.request.newContext();
  try {
    let answer = '';
    await expect
      .poll(
        async () => {
          answer = await readIceGateVersion(api);
          return answer;
        },
        {
          message: 'IceGate answering GET / with the stand as committed',
          timeout: 300_000,
          intervals: [2_000],
        }
      )
      .toMatch(/^version \S+$/);
    defaultVersion = answer.slice('version '.length);
  } finally {
    await api.dispose();
  }
});

test.afterAll(() => {
  resetStand();
});

test('an osd upgrade from 3.8.0 to 3.9.0 migrates the saved objects', async ({ request }) => {
  resetStand();
  runCompose(['up', '-d', 'query', 'ingest']);
  await waitForIceGateVersion(request, defaultVersion, 300_000);

  runCompose(['up', '-d', 'opensearch-dashboards-3-8']);
  expect(await waitForOsdAvailable(request, 600_000)).toBe('3.8.0');
  const created = await request.post(`${OSD_URL}/api/saved_objects/index-pattern/e2e-upgrade`, {
    headers: OSD_HEADERS,
    data: { attributes: { title: 'logs', timeFieldName: 'timestamp' } },
  });
  expect(created.status(), await created.text()).toBe(200);
  await setOsdSettings(request, { 'discover:sampleSize': 123 });
  runCompose(['rm', '--stop', '--force', 'opensearch-dashboards-3-8']);

  runCompose(['up', '-d', 'opensearch-dashboards']);
  expect(await waitForOsdAvailable(request, 600_000)).toBe('3.9.0');
  const upgraded = await request.get(`${OSD_URL}/api/saved_objects/index-pattern/e2e-upgrade`);
  expect(upgraded.status(), await upgraded.text()).toBe(200);
  expect((await upgraded.json()).attributes.title).toBe('logs');
  const settings = await (await request.get(`${OSD_URL}/api/opensearch-dashboards/settings`)).json();
  expect(settings.settings['discover:sampleSize'].userValue).toBe(123);
  const kibana = await icegateJson(request, 'GET', '/.kibana');
  expect(kibana.status).toBe(200);
  expect(findKibanaAliasTarget(kibana.body)).toMatch(/^\.kibana_\d+$/);
});

test('a strict osd rejects icegate at 3.8.0 and migrates after icegate restarts at 3.9.0', async ({ request }) => {
  resetStand();
  const lowerMinor = { ICEGATE_E2E_QUERY_CONFIG: writeQueryConfigVariant('3.8.0') };
  runCompose(['up', '-d', 'query', 'ingest'], lowerMinor);
  await waitForIceGateVersion(request, '3.8.0', 300_000);
  runCompose(['up', '-d', 'opensearch-dashboards-strict'], lowerMinor);

  // The hold is observed only after the strict Dashboards has asked IceGate for `/_nodes` twice:
  // it has then read the reported version and asked again, so an absence below is a decision
  // it made, not a boot that has not reached IceGate yet.
  // `toPass` rather than `expect.poll`: a timeout then reports the last assertion, whose message
  // carries why the counter could not be read.
  await expect(async () => {
    const nodesRequests = await countNodesRequests(request);
    expect(
      nodesRequests.count,
      `GET /_nodes requests from the strict Dashboards (metrics endpoint: ${nodesRequests.cause})`
    ).toBeGreaterThanOrEqual(2);
  }).toPass({ timeout: 300_000, intervals: [2_000] });
  await expect
    .poll(() => readStrictStatus(request), {
      message: 'the strict Dashboards answering 503 while IceGate reports 3.8.0',
      timeout: 60_000,
      intervals: [2_000],
    })
    .toBe('status 503');
  expect((await icegateJson(request, 'GET', '/.kibana')).status, '.kibana created while IceGate reports 3.8.0').toBe(404);
  // A short tail of extra observation; correctness was decided by the gated probes above.
  for (let probe = 0; probe < 3; probe += 1) {
    await new Promise((resolve) => setTimeout(resolve, 5_000));
    expect(await readStrictStatus(request), 'a strict Dashboards serving while IceGate reports 3.8.0').toBe('status 503');
    expect((await icegateJson(request, 'GET', '/.kibana')).status, '.kibana created while IceGate reports 3.8.0').toBe(404);
  }
  const stateBeforeRestart = readContainerState('opensearch-dashboards-strict');
  expect(stateBeforeRestart.status).toBe('running');

  runCompose(['up', '-d', '--force-recreate', '--no-deps', 'query'], { ICEGATE_E2E_QUERY_CONFIG: undefined });
  await waitForIceGateVersion(request, defaultVersion, 300_000);
  expect(await waitForOsdAvailable(request, 600_000)).toBe('3.9.0');
  const kibana = await icegateJson(request, 'GET', '/.kibana');
  expect(kibana.status).toBe(200);
  expect(findKibanaAliasTarget(kibana.body)).toMatch(/^\.kibana_\d+$/);
  const stateAfterRestart = readContainerState('opensearch-dashboards-strict');
  expect(stateAfterRestart.startedAt, 'the Dashboards container restarted').toBe(stateBeforeRestart.startedAt);
  expect(stateAfterRestart.restartCount).toBe('0');
});

test('icegate refuses a pre-release version at startup', () => {
  resetStand();
  const preRelease = { ICEGATE_E2E_QUERY_CONFIG: writeQueryConfigVariant('3.9.0-SNAPSHOT') };
  let failure: { status: number | null; stdout: string; stderr: string } | undefined;
  try {
    runCompose(['run', '--rm', '--no-deps', 'query'], preRelease);
  } catch (error) {
    failure = error as { status: number | null; stdout: string; stderr: string };
  }

  // The positive control is `beforeAll`, which boots the same image with the stand's own document.
  // Only the refused value is asserted, not the message around it: that wording is no protocol
  // contract.
  expect(failure, 'the query service started with version 3.9.0-SNAPSHOT').toBeDefined();
  // A killed `docker compose run` reports a null status: a real exit code is required first.
  expect(failure?.status, 'docker compose run exit code').toEqual(expect.any(Number));
  expect(failure?.status).not.toBe(0);
  expect(`${failure?.stdout ?? ''}${failure?.stderr ?? ''}`).toContain('"3.9.0-SNAPSHOT"');
});

test('grafana provisioned with 3.10.0 offers the metric aggregations', async ({ page, request }) => {
  resetStand();
  const variants = {
    ICEGATE_E2E_QUERY_CONFIG: writeQueryConfigVariant('3.10.0'),
    GRAFANA_E2E_PROVISIONING: writeGrafanaProvisioningVariant('3.10.0'),
  };
  runCompose(['up', '-d', 'query', 'ingest', 'grafana'], variants);
  await waitForIceGateVersion(request, '3.10.0', 300_000);
  await waitForGrafanaHealthy(request, 300_000);

  const provisioned = await request.get(`${GRAFANA_URL}/api/datasources/uid/opensearch-spans-demo`, {
    headers: GRAFANA_ADMIN_HEADERS,
  });
  expect(provisioned.status(), await provisioned.text()).toBe(200);
  expect((await provisioned.json()).jsonData.version).toBe('3.10.0');
  const health = await request.get(`${GRAFANA_URL}/api/datasources/uid/opensearch-spans-demo/health`, {
    headers: GRAFANA_ADMIN_HEADERS,
  });
  expect(health.status(), await health.text()).toBe(200);

  const now = Date.now();
  const lastHour = { from: new Date(now - 3_600_000).toISOString(), to: new Date(now).toISOString() };
  await page.goto(`${GRAFANA_URL}${exploreUrl('opensearch-spans-demo', luceneMetricQuery(), lastHour)}`);
  await openMetricTypeOptions(page);
  for (const label of METRIC_AGGREGATION_LABELS) {
    await expect(page.getByRole('option').filter({ hasText: new RegExp(`^${label}$`) }), label).toBeVisible();
  }
});
