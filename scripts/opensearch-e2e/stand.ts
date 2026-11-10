/**
 * Shared facts and helpers of the end-to-end suite: where the stand's services listen, the
 * tenant the stand serves, what seed.sh recorded, and the waits every case bounds.
 *
 * No port is written here. The service URLs come from the stand's Compose model and the
 * listener ports from the stand's own config documents under `config/docker/`, which own them;
 * a drift fails every case at its first request instead of passing silently.
 */
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { expect, type APIRequestContext } from '@playwright/test';

/** The repository root; the Compose files and the stand's config documents are read from it. */
export const REPO_ROOT = path.resolve(__dirname, '..', '..');
const COMPOSE_FILES = ['config/docker/docker-compose.yml', 'config/docker/docker-compose.e2e.yml'];

/**
 * Runs `docker compose` over the stand's file plus the suite's overrides and returns its stdout.
 * `args` may start with global flags such as `--profile`. `env` sets variables the override
 * interpolates; `undefined` removes one, so the stand's own file is mounted. Throws on a
 * non-zero exit, with the command's stdout and stderr on the error.
 */
export function runCompose(args: string[], env: Record<string, string | undefined> = {}): string {
  const fileArgs = COMPOSE_FILES.flatMap((file) => ['-f', path.join(REPO_ROOT, file)]);
  const mergedEnv: NodeJS.ProcessEnv = { ...process.env };
  for (const [key, value] of Object.entries(env)) {
    if (value === undefined) {
      delete mergedEnv[key];
    } else {
      mergedEnv[key] = value;
    }
  }
  return execFileSync('docker', ['compose', ...fileArgs, ...args], {
    cwd: REPO_ROOT,
    env: mergedEnv,
    encoding: 'utf8',
    timeout: 900_000,
    // `logs` of a whole stand exceeds the default stdout buffer of execFileSync.
    maxBuffer: 512 * 1024 * 1024,
  });
}

/** The part of `docker compose config --format json` the suite reads. */
export type ComposeModel = {
  services: Record<string, { image?: string; ports?: Array<{ target: number; published?: string }> }>;
};

/**
 * The stand's Compose model as `docker compose config` resolves it. Services gated behind a
 * profile are included only when `globalArgs` names the profile (`['--profile', '<name>']`).
 * Needs the Docker CLI, not a running stand.
 */
export function readComposeModel(globalArgs: string[] = []): ComposeModel {
  return JSON.parse(runCompose([...globalArgs, 'config', '--format', 'json'])) as ComposeModel;
}

/**
 * The value of `key` in the top-level `block` of `config/docker/<file>`, or `undefined` when the
 * block or the key is absent. Reads two-space-indented `key: value` lines, the layout of the
 * stand's documents; surrounding double quotes are dropped.
 */
export function readStandConfigValue(file: string, block: string, key: string): string | undefined {
  const lines = readFileSync(path.join(REPO_ROOT, 'config', 'docker', file), 'utf8').split('\n');
  const header = lines.indexOf(`${block}:`);
  if (header === -1) {
    return undefined;
  }
  for (let line = header + 1; line < lines.length && (lines[line].startsWith('  ') || lines[line] === ''); line += 1) {
    const match = /^  ([A-Za-z_]+):\s*"?([^"#]*?)"?\s*(#.*)?$/.exec(lines[line]);
    if (match !== null && match[1] === key) {
      return match[2];
    }
  }
  return undefined;
}

/** The `port` of a listener block of `config/docker/<file>`; throws when the block sets none. */
export function listenerPort(file: string, block: string): number {
  const port = Number(readStandConfigValue(file, block, 'port'));
  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(`config/docker/${file} sets no integer ${block}.port`);
  }
  return port;
}

/**
 * The host URL of `service`: the published port of its container port `target`, or of its only
 * port when `target` is omitted. Throws unless exactly one published port matches.
 */
function publishedUrl(model: ComposeModel, service: string, target?: number): string {
  const ports = (model.services[service]?.ports ?? []).filter(
    (port) => (target === undefined || port.target === target) && (port.published ?? '') !== ''
  );
  if (ports.length !== 1) {
    throw new Error(`the stand publishes ${ports.length} matching ports of ${service} (container port ${target ?? 'any'})`);
  }
  // One loopback for every service: IceGate's listeners bind the IPv4 `host: 0.0.0.0` in
  // config/docker, so the URL names the IPv4 loopback, not whatever `localhost` resolves to first.
  return `http://127.0.0.1:${ports[0].published}`;
}

const STAND_MODEL = readComposeModel();

/** The query service's OpenSearch listener. */
export const ICEGATE_URL = publishedUrl(STAND_MODEL, 'query', listenerPort('query.yaml', 'opensearch'));
/** The stand's OpenSearch Dashboards. */
export const OSD_URL = publishedUrl(STAND_MODEL, 'opensearch-dashboards');
/** The stand's Grafana. */
export const GRAFANA_URL = publishedUrl(STAND_MODEL, 'grafana');
/** The one tenant the stand's ingest writes and every stand client sends. */
export const TENANT = 'demo';
/** Sent on every state-changing request to OSD's HTTP API. */
export const OSD_HEADERS = { 'osd-xsrf': 'true' };

const SEED_FILE = path.join(__dirname, '.stand', 'seed.json');

/** What seed.sh recorded: the minute boundary every seeded time is an offset from. */
export type Seed = { baseMs: number };

/** Reads the seed file; a missing file means seed.sh has not run against this stand. */
export function readSeed(): Seed {
  let text: string;
  try {
    text = readFileSync(SEED_FILE, 'utf8');
  } catch {
    throw new Error(`no seed file at ${SEED_FILE}: run scripts/opensearch-e2e/seed.sh against the stand first`);
  }
  const seed = JSON.parse(text) as Seed;
  if (!Number.isInteger(seed.baseMs) || seed.baseMs % 60_000 !== 0) {
    throw new Error(`${SEED_FILE} must hold {"baseMs": <a minute boundary in epoch ms>}, found ${text}`);
  }
  return seed;
}

/** A window holding every seeded row: one minute before the base to ten minutes after it. */
export function seedWindow(seed: Seed): { from: string; to: string } {
  return {
    from: new Date(seed.baseMs - 60_000).toISOString(),
    to: new Date(seed.baseMs + 600_000).toISOString(),
  };
}

/**
 * Encodes a value as rison, for the `_g` and `_a` URL state the OSD cases open pages with.
 * Strings that are not valid rison identifiers are quoted; `!` and `'` are escaped with `!`.
 */
export function toRison(value: unknown): string {
  if (value === null) {
    return '!n';
  }
  if (value === true) {
    return '!t';
  }
  if (value === false) {
    return '!f';
  }
  if (typeof value === 'number') {
    return String(value);
  }
  if (typeof value === 'string') {
    return risonString(value);
  }
  if (Array.isArray(value)) {
    return `!(${value.map(toRison).join(',')})`;
  }
  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>).filter(([, item]) => item !== undefined);
    return `(${entries.map(([key, item]) => `${risonString(key)}:${toRison(item)}`).join(',')})`;
  }
  throw new Error(`rison cannot encode ${String(value)}`);
}

function risonString(text: string): string {
  // A rison identifier starts with a letter, `_`, `.`, `/` or `~` and holds no reserved character.
  if (/^[A-Za-z_./~][-A-Za-z0-9_./~]*$/.test(text)) {
    return text;
  }
  return `'${text.replace(/!/g, '!!').replace(/'/g, "!'")}'`;
}

/** Sends one request straight to IceGate's OpenSearch listener under `tenant`. */
export async function icegateJson(
  request: APIRequestContext,
  method: 'GET' | 'POST',
  pathAndQuery: string,
  body?: unknown,
  tenant: string = TENANT
): Promise<{ status: number; body: any }> {
  const response = await request.fetch(`${ICEGATE_URL}${pathAndQuery}`, {
    method,
    headers: { 'x-scope-orgid': tenant },
    data: body,
  });
  const text = await response.text();
  return { status: response.status(), body: text.length > 0 ? JSON.parse(text) : null };
}

/**
 * Waits, at most `timeoutMs`, until `/api/status` reports the overall level `available`, and
 * returns the reported version.
 */
export async function waitForOsdAvailable(request: APIRequestContext, timeoutMs: number): Promise<string> {
  let version = '';
  await expect
    .poll(
      async () => {
        // The request error stays in the polled value, so a timeout reports its cause.
        const response = await request
          .get(`${OSD_URL}/api/status?v8format=true`)
          .catch((error: Error) => error);
        if (response instanceof Error) {
          return `unreachable: ${response.message}`;
        }
        if (response.status() !== 200) {
          return `status ${response.status()}`;
        }
        const status = await response.json();
        version = status.version.number;
        return status.status.overall.level;
      },
      { message: 'OSD overall status', timeout: timeoutMs, intervals: [2_000] }
    )
    .toBe('available');
  return version;
}

/** Writes advanced settings through OSD's own settings API; `null` resets a setting. */
export async function setOsdSettings(request: APIRequestContext, changes: Record<string, unknown>): Promise<void> {
  const response = await request.post(`${OSD_URL}/api/opensearch-dashboards/settings`, {
    headers: OSD_HEADERS,
    data: { changes },
  });
  expect(response.status(), await response.text()).toBe(200);
}
