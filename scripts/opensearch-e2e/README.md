# OpenSearch end-to-end suite

These cases run the real clients of IceGate's OpenSearch listener against the Compose stand:
the stock OpenSearch Dashboards image and Grafana with the OpenSearch datasource plugin, at the
versions `config/docker/docker-compose.yml` pins. They are the only tests that pin how those
clients behave against IceGate. A statement in IceGate's code or READMEs about what a client does
cites the case that pins it, by its exact name (the test title), so a case name is a stable
identifier: renaming one means updating every citation (`grep -rn '<old name>'`).

The integration tests in `crates/icegate-query/tests/opensearch/` pin IceGate's own contract with
client-shaped requests; they make no claim about a client.

| Project | File | Client |
|---|---|---|
| `osd` | `osd.spec.ts` | OpenSearch Dashboards: boot and migration, saved objects, index patterns, Discover, a dashboard, Query Workbench, Discover PPL, Observability Logs, the tenant header |
| `grafana` | `grafana.spec.ts` | Grafana with the OpenSearch datasource plugin, through the stand's three OpenSearch datasources: health, Lucene and PPL logs, variables, ad-hoc filters, the metrics editor, traces, trace-to-logs |

The Grafana cases create their dashboards through Grafana's HTTP API in `beforeAll`, each
named `e2e-<purpose>`, and run Explore queries from URLs, so every query reaches IceGate through
the plugin.

## Running the suite

Docker is required, with a Compose that reads the `!reset` tag
`config/docker/docker-compose.e2e.yml` uses, and the Node version that `engines.node` in
`scripts/opensearch-e2e/package.json` names. From the repository root:

```bash
npm --prefix scripts/opensearch-e2e ci
npx --prefix scripts/opensearch-e2e playwright install chromium
PROFILE=debug docker compose -f config/docker/docker-compose.yml -f config/docker/docker-compose.e2e.yml up -d --build
scripts/opensearch-e2e/seed.sh
cd scripts/opensearch-e2e && npx playwright test --project=osd --project=grafana
```

`npx playwright test --list` prints every case. A failed case keeps its trace in
`test-results/`; `npx playwright show-trace <trace.zip>` opens it with the client's network
requests and IceGate's answers. Reset the stand between full runs with
`docker compose -f config/docker/docker-compose.yml -f config/docker/docker-compose.e2e.yml down -v`:
`seed.sh` refuses a stand that already holds rows, because the cases assert exact counts.

`.github/workflows/opensearch-e2e.yml` runs the suite as a separate CI job.

## The stand

`config/docker/docker-compose.e2e.yml` changes one thing in the stand: container logs use
Docker's json-file driver instead of the bundled collector. The collector would write every
service's logs into `demo`, the tenant the cases count rows in, and `docker compose logs` must
work when a run fails.

The suite writes no port. `stand.ts` reads each service's host URL from the stand's Compose
model (`docker compose config`, so the Docker CLI is needed even to list the cases) and each
listener's container port from the stand's config documents under `config/docker/`; `seed.sh`
does the same with `docker compose port`.

## The seed

`seed.sh` writes six logs and two traces of two spans through ingest's OTLP/HTTP endpoint under
the stand's tenant, then waits, bounded by `COMMIT_TIMEOUT_SECS` in `seed.sh`, until IceGate's
`_cat/indices` reports them committed. Every value a case asserts on is stated in `seed.sh`; the
cases state their expected values literally, derived by hand from those rows.

The seeded times are offsets from a minute boundary two minutes before the seed runs, recorded
in `.stand/seed.json`. They are relative to the real clock on purpose: field discovery lists
attribute keys only from rows inside its window, and the clients' default and commonly used
time ranges are relative to now.

## Writing a case

- Name it by the client behaviour it pins, in lower case: it is cited verbatim.
- State the input (the seeded rows, the client actions) and the expected values in the case;
  never derive an expected value with code under test, and never read a recording of traffic.
- Bound every wait (`expect.poll`, `waitForResponse` and locator assertions all time out).
  Cases are not retried: a flaky case is a defect to fix.
- Prefer the client's own surfaces: its UI where the behaviour is rendering, and its server
  routes or HTTP API where the behaviour is what the client asks IceGate and how it reads the
  answer.
