#!/usr/bin/env bash
# Writes the rows the end-to-end cases assert on into the Compose stand, through ingest's
# OTLP/HTTP endpoint under the stand's tenant, then waits (bounded) until IceGate reports them
# committed. Every value a case asserts on is stated here; the cases derive their expected
# values from these rows by hand.
#
# Usage: scripts/opensearch-e2e/seed.sh   (the stand must be up and empty)
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
readonly repo_root

compose() {
  docker compose -f "${repo_root}/config/docker/docker-compose.yml" \
    -f "${repo_root}/config/docker/docker-compose.e2e.yml" "$@"
}

# The `port` of a top-level listener block of a stand config document under config/docker.
config_port() {
  local port
  port="$(sed -n "/^$2:/,/^[^ ]/s/^  port: *\([0-9][0-9]*\).*/\1/p" "${repo_root}/config/docker/$1")"
  if [[ -z "${port}" ]]; then
    echo "seed.sh: config/docker/$1 sets no $2.port" >&2
    return 1
  fi
  echo "${port}"
}

# The host URL at which the running stand publishes a service's container port. IceGate's
# listeners bind the IPv4 `host: 0.0.0.0` in config/docker, so the URL names the IPv4 loopback,
# not whatever `localhost` resolves to first.
published_url() {
  local address
  address="$(compose port "$1" "$2")" || return 1
  echo "http://127.0.0.1:${address##*:}"
}

# The URLs come from the stand's own documents; set either variable to point at another stand.
# Each substitution is its own assignment, so a failing lookup stops the script (`set -e`).
if [[ -z "${INGEST_URL:-}" ]]; then
  ingest_port="$(config_port ingest.yaml otlp_http)"
  INGEST_URL="$(published_url ingest "${ingest_port}")"
fi
if [[ -z "${ICEGATE_URL:-}" ]]; then
  opensearch_port="$(config_port query.yaml opensearch)"
  ICEGATE_URL="$(published_url query "${opensearch_port}")"
fi
readonly INGEST_URL ICEGATE_URL
readonly TENANT=demo
readonly READY_TIMEOUT_SECS=180
readonly COMMIT_TIMEOUT_SECS=300
readonly EXPECTED_LOGS=6
readonly EXPECTED_SPANS=4
readonly seed_dir="${repo_root}/scripts/opensearch-e2e/.stand"

# A minute boundary two minutes back. Every seeded time lies in the first six seconds after it,
# so a one-minute or one-hour bucket holds all of them, the rows sit inside field discovery's
# window and the clients' relative ranges, and no row is in the future when a case runs.
now_secs=$(date +%s)
readonly base_secs=$(( now_secs / 60 * 60 - 120 ))

# Nanoseconds since the epoch of an offset in milliseconds from the base.
at() {
  echo "$(( base_secs * 1000000000 + $1 * 1000000 ))"
}

# docs.count IceGate reports for one index under the stand's tenant, or `unavailable`.
docs_count() {
  curl -fsS -H "x-scope-orgid: ${TENANT}" "${ICEGATE_URL}/_cat/indices/$1?format=json&h=docs.count" \
    | sed -n 's/.*"docs\.count": *"\([0-9]*\)".*/\1/p' || echo unavailable
}

# Sent once: an OTLP write is not idempotent, so a retry after a failure that reached the WAL
# would write the rows twice. An HTTP error stops the script with its status.
post_otlp() {
  curl -fsS -H 'content-type: application/json' -H "x-scope-orgid: ${TENANT}" \
    --data-binary @- "${INGEST_URL}$1" > /dev/null
}

# Runs `curl` with the given arguments every two seconds until it succeeds; stops the script
# once READY_TIMEOUT_SECS have passed, naming the `$1` that did not answer.
wait_for_answer() {
  local listener="$1"
  shift
  local deadline=$(( $(date +%s) + READY_TIMEOUT_SECS ))
  until curl "$@"; do
    if (( $(date +%s) >= deadline )); then
      echo "seed.sh: ${listener} did not answer within ${READY_TIMEOUT_SECS}s" >&2
      exit 1
    fi
    sleep 2
  done
}

wait_for_answer "IceGate's GET /" -fsS -o /dev/null -H "x-scope-orgid: ${TENANT}" "${ICEGATE_URL}/"
# Without `-f` any HTTP status counts: the OTLP/HTTP listener answering is what the writes below
# need, and a GET writes nothing.
wait_for_answer "ingest's OTLP/HTTP listener" -sS -o /dev/null "${INGEST_URL}/v1/logs"

logs_before="$(docs_count logs)"
spans_before="$(docs_count spans)"
if [[ "${logs_before}" != 0 || "${spans_before}" != 0 ]]; then
  echo "seed.sh: the stand already holds logs or spans for ${TENANT} (logs=${logs_before}, spans=${spans_before});" \
    "the cases assert exact counts." >&2
  echo "Reset it first: docker compose -f config/docker/docker-compose.yml -f config/docker/docker-compose.e2e.yml down -v" >&2
  exit 1
fi

# Six logs of one service. The first carries both `http` and `http.method`: a key that is both a
# leaf and a prefix of another key. Trace and span ids tie logs to the spans below.
post_otlp /v1/logs <<JSON
{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"e2e-checkout"}}]},
 "scopeLogs":[{"scope":{"name":"e2e-seed"},"logRecords":[
  {"timeUnixNano":"$(at 50)","severityNumber":9,"severityText":"INFO","body":{"stringValue":"order accepted"},
   "attributes":[{"key":"http","value":{"stringValue":"v1"}},{"key":"http.method","value":{"stringValue":"GET"}},{"key":"http.route","value":{"stringValue":"/checkout"}}],
   "traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174"},
  {"timeUnixNano":"$(at 1000)","severityNumber":9,"severityText":"INFO","body":{"stringValue":"payment authorized"},
   "attributes":[{"key":"http.method","value":{"stringValue":"POST"}},{"key":"http.route","value":{"stringValue":"/payments"}}],
   "traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b175"},
  {"timeUnixNano":"$(at 2000)","severityNumber":13,"severityText":"WARN","body":{"stringValue":"inventory low"},
   "attributes":[{"key":"http.method","value":{"stringValue":"GET"}},{"key":"http.route","value":{"stringValue":"/inventory"}}]},
  {"timeUnixNano":"$(at 3000)","severityNumber":17,"severityText":"ERROR","body":{"stringValue":"payment declined"},
   "attributes":[{"key":"http.method","value":{"stringValue":"POST"}},{"key":"http.route","value":{"stringValue":"/payments"}}],
   "traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331"},
  {"timeUnixNano":"$(at 4000)","severityNumber":17,"severityText":"ERROR","body":{"stringValue":"card expired"},
   "attributes":[{"key":"http.method","value":{"stringValue":"POST"}}]},
  {"timeUnixNano":"$(at 5000)","severityNumber":9,"severityText":"INFO","body":{"stringValue":"order shipped"},
   "attributes":[{"key":"http.method","value":{"stringValue":"GET"}}]}
 ]}]}]}
JSON

# Two traces of two spans each. Trace A (5b8e...): root `GET /checkout`, 152175 us, and a child
# carrying a link to trace B's root. Trace B (0af7...): root `POST /refund`, 735519 us, and a
# failing child (status code 2) with a `retry` event that has no error attribute.
post_otlp /v1/traces <<JSON
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"e2e-checkout"}}]},
 "scopeSpans":[{"scope":{"name":"e2e-seed"},"spans":[
  {"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b174","name":"GET /checkout","kind":2,
   "startTimeUnixNano":"$(at 0)","endTimeUnixNano":"$(( $(at 0) + 152175000 ))","status":{}},
  {"traceId":"5b8efff798038103d269b633813fc60c","spanId":"eee19b7ec3c1b175","parentSpanId":"eee19b7ec3c1b174",
   "name":"authorize payment","kind":3,"startTimeUnixNano":"$(at 1000)","endTimeUnixNano":"$(at 1100)","status":{"code":1},
   "links":[{"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331"}]},
  {"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"b7ad6b7169203331","name":"POST /refund","kind":2,
   "startTimeUnixNano":"$(at 3000)","endTimeUnixNano":"$(( $(at 3000) + 735519000 ))","status":{}},
  {"traceId":"0af7651916cd43dd8448eb211c80319c","spanId":"00f067aa0ba902b7","parentSpanId":"b7ad6b7169203331",
   "name":"charge card","kind":3,"startTimeUnixNano":"$(at 3100)","endTimeUnixNano":"$(at 3600)",
   "status":{"code":2,"message":"card declined"},
   "events":[{"timeUnixNano":"$(at 3200)","name":"retry","attributes":[{"key":"attempt","value":{"stringValue":"2"}}]}]}
 ]}]}]}
JSON

deadline=$(( $(date +%s) + COMMIT_TIMEOUT_SECS ))
until [[ "$(docs_count logs)" == "${EXPECTED_LOGS}" && "$(docs_count spans)" == "${EXPECTED_SPANS}" ]]; do
  if (( $(date +%s) >= deadline )); then
    echo "seed.sh: after ${COMMIT_TIMEOUT_SECS}s IceGate reports logs=$(docs_count logs) spans=$(docs_count spans)," \
      "expected ${EXPECTED_LOGS} and ${EXPECTED_SPANS} committed" >&2
    exit 1
  fi
  sleep 2
done

mkdir -p "${seed_dir}"
printf '{"baseMs": %d}\n' "$(( base_secs * 1000 ))" > "${seed_dir}/seed.json"
echo "seed.sh: seeded ${EXPECTED_LOGS} logs and ${EXPECTED_SPANS} spans at base $(( base_secs * 1000 )) ms"
