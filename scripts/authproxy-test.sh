#!/usr/bin/env bash
#
# Check the ingest auth proxy against a self-contained token issuer.
#
# What it pins, and why none of it is visible in a YAML diff:
#   - the client's own x-scope-orgid never reaches icegate, and the value that
#     does comes from the token's tenant_id claim (the filter order in
#     icegate.authProxyHttpFilters is what makes this true; the route-level form
#     of the same rule would delete the token's value instead);
#   - a token whose payload `typ` is not the discriminator the configuration
#     under test admits is refused, and a token whose JOSE `typ` is not the one
#     it admits is refused too; both values are read out of that configuration
#     rather than named here;
#   - a token offered as an `access_token` query parameter is not a token: the
#     Authorization header is the only place the configuration takes one from,
#     so a bearer never reaches an access log through a URL;
#   - a token for another audience, and a request with no token at all, are
#     refused before anything reaches icegate;
#   - a token signed by a key the JWKS does not publish, and one issued by another
#     issuer, are refused — the two forgeries every other case here is blind to,
#     since they all mint with the configured issuer and the published key;
#   - an expired token is refused, and one expired within clock_skew_seconds is
#     still accepted, so a clock that drifts between the issuer and the proxy
#     does not stop ingest;
#   - an upstream that outlasts the route timeout is cut off and one inside it is
#     proxied, so the ceiling the proxy puts on icegate's answer is a value of the
#     configuration rather than whatever the image defaults to. Both listeners are
#     driven: over HTTP the stub holds the answer back on its /slow path, over
#     gRPC the upstream Envoy holds it back with a fault delay on one method, so
#     the cut-off is observed as the grpc-status an OTLP exporter reads rather
#     than only as a timeout value read out of the configuration;
#   - the same holds on the OTLP/gRPC listener, where a refusal has to surface as
#     a grpc-status rather than as an HTTP status the exporter cannot read:
#     UNAUTHENTICATED for a missing or invalid token, PERMISSION_DENIED for a
#     token the type marker refuses;
#   - the OTLP/HTTP listener routes by prefix rather than by a list of paths, so
#     the token is what admits a request and the route table is not a second,
#     silent allow-list of the paths icegate happens to serve today.
#
# Both copies of the filter chain are checked: the one the Helm chart renders and
# the one the Compose stand ships. They are meant to agree, and nothing else
# fails when they stop agreeing.
#
# The issuer here is a throwaway RSA key generated per run; no token issuer of
# any deployment is contacted, and the keys never leave the temporary directory.
#
# Requires: docker (OrbStack), helm, openssl, curl. Not part of `make ci` —
# see the `authproxy-test` target in the Makefile.
set -euo pipefail

# The issuer the stand's copy carries, read out of it rather than pinned here: a
# token minted under any other name is refused by jwt_authn before a single case
# runs, and the chart render below is given the same value so both configurations
# check one issuer.
readonly STAND_CONFIG="config/docker/auth-proxy/envoy.yaml"
ISSUER=$(awk '$1 == "issuer:" { print $2; exit }' "$STAND_CONFIG" | tr -d '"')
readonly ISSUER
[ -n "$ISSUER" ] || {
    echo "$STAND_CONFIG names no issuer" >&2
    exit 1
}
# The host the token_issuer_jwks cluster addresses. Derived from the issuer
# rather than named again, so pointing the configuration at another issuer moves
# the JWKS rewrite in prepare_stand_config with it instead of leaving the cluster
# aimed at the host this file used to know.
readonly ISSUER_HOST="${ISSUER#*://}"
readonly AUDIENCE="icegate-ingest"
# An issuer no configuration names. Under the TLD RFC 2606 reserves for
# documentation, like the stand's own placeholder, so a token minted under it
# names nothing that resolves anywhere.
readonly FOREIGN_ISSUER="https://other.example"
readonly TENANT="acme"
readonly FORGED_TENANT="victim"
# A path outside crates/icegate-ingest/src/otlp_http/routes.rs. The upstream stub
# answers it 200 like any other POST, so what the two cases below report is the
# proxy's routing rather than a path icegate serves.
readonly UNKNOWN_PATH="/v1/unknown"
# The OTLP Export the gRPC cases call, and beside it a method the gRPC upstream
# answers late. Both sit under the prefix the gRPC listener routes on, so the
# slow one reaches the same filter chain and the same route timeout; the upstream
# holds it back with a fault delay, which is what lets the case see what the
# proxy does to an RPC icegate has not finished.
readonly GRPC_EXPORT_PATH="/opentelemetry.proto.collector.logs.v1.LogsService/Export"
readonly GRPC_SLOW_EXPORT_PATH="/opentelemetry.proto.collector.logs.v1.LogsService/ExportSlow"
readonly PYTHON_IMAGE="python:3.13-alpine"

# Read out of the chart render rather than pinned here: the version the chart
# deploys is the version the checks have to run against. The ports each
# configuration binds are read the same way, out of the configuration under test
# — hardcoding them here would turn a re-defaulted port into a readiness timeout
# with no indication of what moved.
ENVOY_IMAGE=""
LISTENER_HTTP_PORT=""
LISTENER_GRPC_PORT=""
UPSTREAM_HTTP_PORT=""
UPSTREAM_GRPC_PORT=""
CLOCK_SKEW_SECONDS=""
TOKEN_TYPE=""
FOREIGN_TOKEN_TYPE=""
HEADER_TYPE=""
FOREIGN_HEADER_TYPE=""
ROUTE_TIMEOUT_SECONDS=""
SCHEME=""
# How long ago the two expiry cases expired. Fixed here rather than derived from
# the configuration's clock_skew_seconds: a case that computes its own input from
# the value under test passes for every value, including zero. What the
# configuration is allowed to say is checked instead, in read_config_values.
readonly EXPIRY_WITHIN_SKEW_SECONDS=30
readonly EXPIRY_PAST_SKEW_SECONDS=3600
# The route timeout both configurations are put under test with, replacing the
# one they ship: the deployed value is a WAL write's worth of patience, and a
# case that waits it out would spend that long twice per run. The mechanism is
# unchanged — what is checked is that the route carries the timeout at all and
# that the value comes from the configuration, which a value of this script's
# choosing exercises exactly as the shipped one would.
readonly ROUTE_TIMEOUT_UNDER_TEST_SECONDS=4
# How long the upstream stub holds the answer back in the case that must stay
# inside the ceiling. Fixed rather than derived from the value under test, for the
# reason spelled out above the expiry pair, and far enough below it that a runner
# pulling images under four containers does not turn the margin into a coin flip.
# read_config_values refuses a configuration whose timeout is not above it.
readonly FAST_UPSTREAM_SLEEP_SECONDS=1
readonly JWKS_PORT=8080
# Host-side ports, which belong to this check rather than to any configuration.
readonly PUBLISHED_HTTP_PORT=18318
readonly PUBLISHED_GRPC_PORT=18317

readonly NETWORK="icegate-authproxy-test"
readonly UPSTREAM_CONTAINER="icegate-authproxy-upstream"
readonly GRPC_UPSTREAM_CONTAINER="icegate-authproxy-upstream-grpc"
readonly JWKS_CONTAINER="icegate-authproxy-jwks"
readonly PROXY_CONTAINER="icegate-authproxy-envoy"

# What the chart is rendered with, in one place: the same render carries both the
# configuration under test and the image tag the containers run, and a value the
# chart requires that reached only one of two lists would surface as "the chart
# render named no proxy image" rather than as the value that is missing.
# `tenant.mode=multi` belongs to it because the chart refuses the default tenant
# behind the proxy (icegate.validateAuthProxy).
CHART_SET_ARGS=(
    --set ingest.tenant.mode=multi
    --set ingest.authProxy.enabled=true
    --set ingest.authProxy.jwt.issuer="$ISSUER"
    --set ingest.authProxy.jwt.audience="$AUDIENCE"
    --set ingest.authProxy.jwt.jwksUri="http://$JWKS_CONTAINER:$JWKS_PORT/jwks.json"
    --set ingest.authProxy.tls.secretName=ingest-tls
    --set ingest.authProxy.upstreamTimeout="${ROUTE_TIMEOUT_UNDER_TEST_SECONDS}s"
)

WORKDIR=""
# The key the JWKS does not carry, named once because two cases address it: the
# one that mints with it and the generator that writes it.
UNPUBLISHED_KEY=""
FAILURES=0

cleanup() {
    docker rm -f "$PROXY_CONTAINER" "$GRPC_UPSTREAM_CONTAINER" "$UPSTREAM_CONTAINER" \
        "$JWKS_CONTAINER" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
    [ -n "$WORKDIR" ] && rm -rf "$WORKDIR"
}
trap cleanup EXIT

fail() {
    echo "FAIL: $*" >&2
    FAILURES=$((FAILURES + 1))
}

pass() {
    echo "ok: $*"
}

require() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "$1 is required" >&2
        exit 1
    }
}

base64url() {
    openssl base64 -A | tr '+/' '-_' | tr -d '='
}

# Two RSA keys for the run. The first is published as a JWKS the proxy fetches
# over plain HTTP; the second is deliberately left out of it, so a token signed
# with it is one whose signature the published keys cannot verify.
write_issuer_keys() {
    openssl genrsa -out "$WORKDIR/issuer.pem" 2048 2>/dev/null
    openssl genrsa -out "$UNPUBLISHED_KEY" 2048 2>/dev/null
    local modulus_hex n
    modulus_hex=$(openssl rsa -in "$WORKDIR/issuer.pem" -noout -modulus | cut -d= -f2)
    # JWKS `n` is the modulus as base64url of its raw bytes, so the hex openssl
    # prints has to be decoded. `sed` turns it into a `printf` format string of
    # \xHH escapes rather than calling a hex decoder: xxd ships with vim-common
    # and is absent from minimal images, including the CI runner this script now
    # runs on. The bytes go straight into the pipe — a command substitution would
    # drop the NULs a modulus may contain.
    n=$(printf "$(printf '%s' "$modulus_hex" | sed 's/../\\x&/g')" | base64url)
    cat >"$WORKDIR/jwks.json" <<EOF
{"keys":[{"kty":"RSA","use":"sig","alg":"RS256","kid":"check","n":"$n","e":"AQAB"}]}
EOF
}

# Self-signed certificate for the chart's listeners, which terminate TLS.
write_server_cert() {
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=localhost" \
        -keyout "$WORKDIR/tls.key" -out "$WORKDIR/tls.crt" 2>/dev/null
    chmod 644 "$WORKDIR/tls.key"
}

# Mint an RS256 token. $1 is the payload `typ`, $2 the audience, $3 the lifetime
# in seconds counted from now — negative for a token that has already expired.
# $4 is the issuer, $5 the signing key and $6 the JOSE `typ` of the header; all
# three default to the ones the proxy is configured to accept, and a case names
# them only when refusing them is the point. The tenant_id claim is always
# $TENANT.
mint_token() {
    local payload_typ="$1" audience="$2" lifetime="$3"
    local issuer="${4:-$ISSUER}" key="${5:-$WORKDIR/issuer.pem}" header_typ="${6:-$HEADER_TYPE}"
    local now exp header payload signing_input signature
    now=$(date +%s)
    exp=$((now + lifetime))
    header=$(printf '{"alg":"RS256","kid":"check","typ":"%s"}' "$header_typ" | base64url)
    payload=$(printf '{"iss":"%s","aud":"%s","sub":"check","typ":"%s","tenant_id":"%s","iat":%s,"exp":%s}' \
        "$issuer" "$audience" "$payload_typ" "$TENANT" "$now" "$exp" | base64url)
    signing_input="$header.$payload"
    signature=$(printf '%s' "$signing_input" |
        openssl dgst -sha256 -sign "$key" -binary | base64url)
    printf '%s.%s' "$signing_input" "$signature"
}

# Stands in for icegate: answers 200 and reports the tenant header it was given,
# which is the only way to see what the proxy actually forwarded.
write_upstream() {
    cat >"$WORKDIR/upstream.py" <<'EOF'
"""Echo the x-scope-orgid the proxy forwarded, so the check can assert on it."""
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SLOW_PREFIX = "/slow/"


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        self.rfile.read(int(self.headers.get("content-length", 0) or 0))
        # `/slow/<seconds>` holds the answer back for that long, which is how the
        # route timeout is observed on this listener: what the proxy does with a
        # request icegate has not finished is invisible from a stub that answers
        # at once. The gRPC upstream holds its own method back with a fault delay.
        if self.path.startswith(SLOW_PREFIX):
            time.sleep(float(self.path[len(SLOW_PREFIX):]))
        tenants = self.headers.get_all("x-scope-orgid") or ["<absent>"]
        body = (",".join(tenants)).encode()
        self.send_response(200)
        self.send_header("content-type", "text/plain")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


# Threading, because of the /slow path above: a single-threaded server keeps the
# sleeping handler on the accept path, and the case that follows the slow one
# waits behind it and hits the proxy's timeout it is supposed to stay inside.
ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler).serve_forever()
EOF
}

# Stands in for icegate's OTLP/gRPC receiver. Envoy rather than the Python stub
# above: the icegate_otlp_grpc cluster speaks HTTP/2 with prior knowledge, which
# http.server does not. The tenant it was handed comes back as a response header,
# since a direct_response carries no body the check could read.
#
# $GRPC_SLOW_EXPORT_PATH is answered after a fault delay longer than the route
# timeout under test, which is what a WAL write the proxy gives up on looks like
# from the listener an OTLP exporter speaks to. The delay is stated per route, so
# every other method is answered at once and only the case that wants it pays it.
write_grpc_upstream() {
    local delay_seconds=$((ROUTE_TIMEOUT_SECONDS + 2))
    cat >"$WORKDIR/grpc-upstream.yaml" <<EOF
admin:
  address:
    socket_address: { address: 127.0.0.1, port_value: 9902 }

static_resources:
  listeners:
    - name: grpc_upstream
      address:
        socket_address: { address: 127.0.0.1, port_value: $UPSTREAM_GRPC_PORT }
      filter_chains:
        - filters:
            - name: envoy.filters.network.http_connection_manager
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                stat_prefix: grpc_upstream
                codec_type: HTTP2
                route_config:
                  name: grpc_upstream
                  virtual_hosts:
                    - name: grpc_upstream
                      domains: ["*"]
                      routes:
                        - match: { path: $GRPC_SLOW_EXPORT_PATH }
                          direct_response:
                            status: 200
                          typed_per_filter_config:
                            envoy.filters.http.fault:
                              "@type": type.googleapis.com/envoy.extensions.filters.http.fault.v3.HTTPFault
                              delay:
                                fixed_delay: ${delay_seconds}s
                                percentage: { numerator: 100, denominator: HUNDRED }
                        - match: { prefix: / }
                          direct_response:
                            status: 200
                          response_headers_to_add:
                            - header:
                                key: x-seen-orgid
                                value: "%REQ(x-scope-orgid)%"
                http_filters:
                  # Carries no fault of its own: the delay above is a route's,
                  # and the filter has to be in the chain for that route to
                  # configure it.
                  - name: envoy.filters.http.fault
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.fault.v3.HTTPFault
                  - name: envoy.filters.http.router
                    typed_config:
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
EOF
}

# The chart's own render, with the issuer pointed at the throwaway JWKS server.
# Rendered once and read twice: the configuration the containers load and the
# image tag they run come out of the same document, so the two cannot describe
# different releases of the chart.
read_chart_render() {
    local rendered
    rendered=$(helm template icegate config/helm/icegate "${CHART_SET_ARGS[@]}")
    printf '%s\n' "$rendered" |
        awk '
            /^  envoy\.yaml: \|/ { collecting = 1; next }
            collecting && /^    / { sub(/^    /, ""); print; next }
            collecting && NF { exit }
        ' >"$WORKDIR/chart-envoy.yaml"
    [ -s "$WORKDIR/chart-envoy.yaml" ] || {
        echo "the chart render carried no envoy.yaml" >&2
        exit 1
    }
    ENVOY_IMAGE=$(printf '%s\n' "$rendered" | awk '/image: envoyproxy\/envoy/ { print $2; exit }')
    [ -n "$ENVOY_IMAGE" ] || {
        echo "the chart render named no proxy image" >&2
        exit 1
    }
}

# First port_value under the named listener or cluster of $2. Both are list
# entries introduced by `- name: <name>`, and the address is the first port in
# either, so one reader serves both.
read_port() {
    local name="$1" config="$2" port
    port=$(awk -v name="$name" '
        $0 ~ "^ *- name: " name "$" { found = 1; next }
        found && match($0, /port_value: *[0-9]+/) {
            print substr($0, RSTART, RLENGTH)
            exit
        }
    ' "$config" | tr -dc '0-9')
    [ -n "$port" ] || {
        echo "$config names no port for $name" >&2
        exit 1
    }
    printf '%s' "$port"
}

# Seconds of the route timeout on the route to cluster $2. Read beside the cluster
# name and within the three lines that follow it, not as the first `timeout:` of
# the document: the jwt_authn provider carries one of its own for the JWKS fetch,
# and the two configurations write the route's on different lines — inline in the
# stand's copy, under a `route:` block in the chart's.
read_route_timeout_seconds() {
    local config="$1" cluster="$2" seconds
    seconds=$(awk -v cluster="$cluster" '
        $0 ~ ("cluster: " cluster) { near = 1 }
        near && ++lines <= 3 && match($0, /timeout: *"?[0-9]+s/) {
            print substr($0, RSTART, RLENGTH)
            exit
        }
    ' "$config" | tr -dc '0-9')
    [ -n "$seconds" ] || {
        echo "$config sets no timeout on the route to $cluster" >&2
        exit 1
    }
    printf '%s' "$seconds"
}

# The value the rbac policy demands of the jwt_authn metadata key $2 — jwt_header
# for the JOSE `typ`, jwt_payload for the claim. Read rather than pinned: the
# chart carries the two as ingest.authProxy.jwt.headerType and .tokenType, so a
# deployment that renamed either must still be checked against the value it
# configured. Addressed by metadata key rather than by the first string_match of
# the document: the policy states both principals in the same form, so position
# alone would follow whichever is written first.
read_metadata_match() {
    local config="$1" metadata_key="$2" value
    value=$(awk -v key="$metadata_key" '
        $0 ~ ("key: " key "$") { near = 1; next }
        near && ++lines <= 4 && match($0, /exact: *[^ }]+/) {
            value = substr($0, RSTART + 7, RLENGTH - 7)
            gsub(/[" ]/, "", value)
            print value
            exit
        }
    ' "$config")
    [ -n "$value" ] || {
        echo "$config states no rbac match on the $metadata_key typ" >&2
        exit 1
    }
    printf '%s' "$value"
}

# Everything the cases address is taken from the configuration under test, so the
# two configurations may disagree without the check going looking for the wrong
# port or assuming the wrong tolerance.
read_config_values() {
    local config="$1"
    LISTENER_HTTP_PORT=$(read_port otlp_http "$config")
    LISTENER_GRPC_PORT=$(read_port otlp_grpc "$config")
    UPSTREAM_HTTP_PORT=$(read_port icegate_otlp_http "$config")
    UPSTREAM_GRPC_PORT=$(read_port icegate_otlp_grpc "$config")
    CLOCK_SKEW_SECONDS=$(awk '/clock_skew_seconds:/ { print $2; exit }' "$config")
    [ -n "$CLOCK_SKEW_SECONDS" ] || {
        echo "$config names no clock_skew_seconds" >&2
        exit 1
    }
    TOKEN_TYPE=$(read_metadata_match "$config" jwt_payload)
    HEADER_TYPE=$(read_metadata_match "$config" jwt_header)
    # Derived from the configured values rather than named, so a refusal case
    # cannot accidentally mint the very type the configuration admits.
    FOREIGN_TOKEN_TYPE="not-$TOKEN_TYPE"
    FOREIGN_HEADER_TYPE="not-$HEADER_TYPE"
    ROUTE_TIMEOUT_SECONDS=$(read_route_timeout_seconds "$config" icegate_otlp_http)
    # Both listeners are read, and they have to agree. The gRPC timeout case
    # below would catch a route that lost its own, but only once four containers
    # are up and only as a grpc-status; refusing here names both values and does
    # it before the stand starts.
    local grpc_timeout_seconds
    grpc_timeout_seconds=$(read_route_timeout_seconds "$config" icegate_otlp_grpc)
    if [ "$grpc_timeout_seconds" != "$ROUTE_TIMEOUT_SECONDS" ]; then
        echo "$config times the OTLP/HTTP route out after ${ROUTE_TIMEOUT_SECONDS}s and the" \
            "OTLP/gRPC one after ${grpc_timeout_seconds}s; both carry the same ceiling on icegate" >&2
        exit 1
    fi
    # The fast case has to stay inside the ceiling whatever the configuration
    # says, and it sleeps a fixed amount rather than one derived from it: an input
    # computed from the value under test passes for every value, including one too
    # small to hold it. Refused here, where the two numbers are both in hand.
    if [ "$ROUTE_TIMEOUT_SECONDS" -le "$FAST_UPSTREAM_SLEEP_SECONDS" ]; then
        echo "$config times the OTLP routes out after ${ROUTE_TIMEOUT_SECONDS}s; the case that" \
            "must stay inside it answers after ${FAST_UPSTREAM_SLEEP_SECONDS}s" >&2
        exit 1
    fi
    # The expiry cases only mean something while the tolerance sits between them:
    # a smaller one refuses the token that is supposed to be accepted, a larger
    # one accepts the token that is supposed to be refused. Either way the pair
    # stops testing the tolerance, so it is a failure of this check rather than a
    # case to skip.
    if [ "$CLOCK_SKEW_SECONDS" -le "$EXPIRY_WITHIN_SKEW_SECONDS" ] ||
        [ "$CLOCK_SKEW_SECONDS" -ge "$EXPIRY_PAST_SKEW_SECONDS" ]; then
        echo "$config sets clock_skew_seconds to $CLOCK_SKEW_SECONDS; the expiry cases need it" \
            "between $EXPIRY_WITHIN_SKEW_SECONDS and $EXPIRY_PAST_SKEW_SECONDS" >&2
        exit 1
    fi
}

# The stand's copy, with the issuer address rewritten and the route timeout put
# on the value this check runs under (see ROUTE_TIMEOUT_UNDER_TEST_SECONDS); the
# filter chain, which is the thing under test, is used exactly as committed. The
# substitution matches only a route line, so a copy that carries no route timeout
# reaches read_route_timeout_seconds unchanged and fails there by name.
prepare_stand_config() {
    sed -e "s#$ISSUER/.well-known/jwks.json#http://$JWKS_CONTAINER:$JWKS_PORT/jwks.json#" \
        -e "/cluster: icegate_otlp_/ s/timeout: \"[0-9]*s\"/timeout: \"${ROUTE_TIMEOUT_UNDER_TEST_SECONDS}s\"/" \
        "$STAND_CONFIG" |
        awk -v host="$JWKS_CONTAINER" -v port="$JWKS_PORT" -v issuer_host="$ISSUER_HOST" '
            $0 ~ ("address: " issuer_host ", port_value: 443") {
                sub(issuer_host, host); sub(/443/, port)
            }
            # The throwaway JWKS server speaks plain HTTP, so the upstream TLS
            # block of that cluster does not apply here. Dropping it ends at the
            # next cluster: without that the deletion would run to the end of the
            # file, and a cluster added after token_issuer_jwks would vanish from
            # the document under test.
            /^    - name: / { skip = 0 }
            /transport_socket:/ && seen_jwks { skip = 1 }
            /name: token_issuer_jwks/ { seen_jwks = 1 }
            skip { next }
            { print }
        ' >"$WORKDIR/stand-envoy.yaml"
}

start_stand() {
    local config="$1"
    read_config_values "$config"
    write_grpc_upstream
    docker network create "$NETWORK" >/dev/null
    docker run -d --name "$JWKS_CONTAINER" --network "$NETWORK" \
        -v "$WORKDIR:/w:ro" -w /w "$PYTHON_IMAGE" \
        python3 -m http.server "$JWKS_PORT" >/dev/null
    # This container owns the network namespace the proxy and the gRPC upstream
    # join, so both published ports are declared on it.
    docker run -d --name "$UPSTREAM_CONTAINER" --network "$NETWORK" \
        -p "$PUBLISHED_HTTP_PORT:$LISTENER_HTTP_PORT" \
        -p "$PUBLISHED_GRPC_PORT:$LISTENER_GRPC_PORT" \
        -v "$WORKDIR:/w:ro" "$PYTHON_IMAGE" \
        python3 /w/upstream.py "$UPSTREAM_HTTP_PORT" >/dev/null
    # --base-id moves this Envoy's hot-restart domain socket off the default: the
    # socket lives in the network namespace, which this container shares with the
    # proxy, and the second Envoy to start would otherwise fail to bind it. The
    # proxy keeps the default, so it runs exactly as the chart deploys it.
    docker run -d --name "$GRPC_UPSTREAM_CONTAINER" --network "container:$UPSTREAM_CONTAINER" \
        -v "$WORKDIR/grpc-upstream.yaml:/etc/envoy/envoy.yaml:ro" \
        "$ENVOY_IMAGE" /usr/local/bin/envoy --base-id 1 --config-path /etc/envoy/envoy.yaml >/dev/null
    docker run -d --name "$PROXY_CONTAINER" --network "container:$UPSTREAM_CONTAINER" \
        -v "$config:/etc/envoy/envoy.yaml:ro" \
        -v "$WORKDIR/tls.crt:/etc/icegate/tls/tls.crt:ro" \
        -v "$WORKDIR/tls.key:/etc/icegate/tls/tls.key:ro" \
        "$ENVOY_IMAGE" /usr/local/bin/envoy --config-path /etc/envoy/envoy.yaml >/dev/null

    # The proxy answers only once it has fetched the JWKS, so readiness is polled
    # rather than slept for; a token is what proves the fetch completed. The gRPC
    # listener is polled on the tenant its upstream reports rather than on the
    # HTTP status: Envoy answers a gRPC request 200 whatever the outcome, so that
    # status is already 200 while the JWKS is still missing and while the gRPC
    # upstream stub is still starting. The x-seen-orgid header appears only once
    # the proxy has admitted the request and the stub has answered it.
    local token
    token=$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)
    for _ in $(seq 1 60); do
        if [ "$(request "$token" "")" = "200" ]; then
            request_grpc "$token" "" >/dev/null
            if [ "$(read_response_header x-seen-orgid)" = "$TENANT" ]; then
                return 0
            fi
        fi
        sleep 1
    done
    echo "the proxy did not become ready; envoy log follows" >&2
    docker logs "$PROXY_CONTAINER" >&2 || true
    exit 1
}

stop_stand() {
    docker rm -f "$PROXY_CONTAINER" "$GRPC_UPSTREAM_CONTAINER" "$UPSTREAM_CONTAINER" \
        "$JWKS_CONTAINER" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
}

# POST $3 (/v1/logs unless given) with $1 as the bearer token (empty for none)
# and $2 as a forged tenant header (empty for none). Prints the status code; the
# response body, which carries the tenant the upstream saw, is left in
# $WORKDIR/body.
request() {
    local token="$1" forged="$2" path="${3:-/v1/logs}"
    # This listener answers no grpc-message, so the file the gRPC cases read is
    # emptied rather than left holding the last gRPC refusal.
    : >"$WORKDIR/headers"
    local args=(-s -k -o "$WORKDIR/body" -w '%{http_code}'
        -X POST "$SCHEME://127.0.0.1:$PUBLISHED_HTTP_PORT$path"
        -H 'content-type: application/x-protobuf' --data-binary '')
    [ -n "$token" ] && args+=(-H "authorization: Bearer $token")
    [ -n "$forged" ] && args+=(-H "x-scope-orgid: $forged")
    curl "${args[@]}"
}

# The same call against the gRPC listener: an OTLP Export with an empty
# length-prefixed message, which is enough for the filter chain to run. $3 names
# the method, $GRPC_EXPORT_PATH unless given. Prints the
# status code; the response headers, which carry grpc-status, grpc-message and the
# tenant the upstream saw, are left in $WORKDIR/headers, and the body — empty on
# this listener, since a gRPC refusal travels in the headers — replaces the one
# the HTTP cases left. HTTP/2 is demanded explicitly — over TLS the listener
# offers it through ALPN, and the plain-text stand has no upgrade to negotiate.
request_grpc() {
    local token="$1" forged="$2" path="${3:-$GRPC_EXPORT_PATH}"
    local args=(-s -k -o "$WORKDIR/body" -D "$WORKDIR/headers" -w '%{http_code}'
        -X POST "$SCHEME://127.0.0.1:$PUBLISHED_GRPC_PORT$path"
        -H 'content-type: application/grpc' -H 'te: trailers'
        --data-binary "@$WORKDIR/grpc-empty")
    if [ "$SCHEME" = "https" ]; then
        args+=(--http2)
    else
        args+=(--http2-prior-knowledge)
    fi
    [ -n "$token" ] && args+=(-H "authorization: Bearer $token")
    [ -n "$forged" ] && args+=(-H "x-scope-orgid: $forged")
    curl "${args[@]}"
}

# Value of response header $1 (lower case) from the last request_grpc, or the
# empty string. The whole value is returned, not its first word: grpc-message
# carries a sentence.
read_response_header() {
    awk -v name="$1" 'tolower($1) == name ":" {
        sub(/^[^:]*: */, "")
        sub(/\r$/, "")
        print
        exit
    }' "$WORKDIR/headers"
}

expect_status() {
    local label="$1" expected="$2" actual="$3" reason
    if [ "$actual" = "$expected" ]; then
        pass "$label -> $actual"
        return
    fi
    # Envoy's own reason ("Jwt audiences are not allowed", "RBAC: access denied")
    # is what says *which* filter refused. The HTTP listener puts it in the body
    # and the gRPC one in grpc-message, and each request overwrites the body and
    # the headers alike, so what is reported here belongs to the case that failed.
    reason=$(cat "$WORKDIR/body")
    [ -n "$reason" ] || reason=$(read_response_header grpc-message)
    fail "$label: expected $expected, got $actual ($reason)"
}

run_cases() {
    local label="$1"
    local status

    status=$(request "" "")
    expect_status "$label: no token" 401 "$status"

    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "")
    expect_status "$label: valid ingest token" 200 "$status"
    if [ "$(cat "$WORKDIR/body")" = "$TENANT" ]; then
        pass "$label: the tenant comes from the token claim"
    else
        fail "$label: upstream saw tenant '$(cat "$WORKDIR/body")', expected '$TENANT'"
    fi

    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "$FORGED_TENANT")
    expect_status "$label: valid token plus a forged tenant header" 200 "$status"
    if [ "$(cat "$WORKDIR/body")" = "$TENANT" ]; then
        pass "$label: the forged tenant header is dropped"
    else
        fail "$label: upstream saw tenant '$(cat "$WORKDIR/body")', expected '$TENANT'"
    fi

    status=$(request "$(mint_token "$FOREIGN_TOKEN_TYPE" "$AUDIENCE" 600)" "")
    expect_status "$label: payload typ is not the configured one" 403 "$status"

    # The other half of the token profile, and the one no filter would look at
    # on its own: jwt_authn verifies a signature whatever the header says it is
    # signing, so a token valid in every other respect passes unless the policy
    # demands the JOSE typ too.
    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600 "$ISSUER" "$WORKDIR/issuer.pem" \
        "$FOREIGN_HEADER_TYPE")" "")
    expect_status "$label: JOSE typ is not the configured one" 403 "$status"

    # A token in the URL is not a token: the query parameter jwt_authn would take
    # one from is closed, so a bearer cannot arrive by a route that writes it to
    # every access log between the sender and the proxy.
    status=$(request "" "" "/v1/logs?access_token=$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)")
    expect_status "$label: token offered as a query parameter" 401 "$status"

    # The two ways a token can be forged outright, each of them a complete bypass
    # of the authentication if it worked: signed by a key the issuer never
    # published, and issued by someone else entirely. Every other case here mints
    # with the configured issuer and the published key, so a filter chain that
    # lost its remote_jwks, or an issuer that rendered empty, would leave them all
    # green.
    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600 "$ISSUER" "$UNPUBLISHED_KEY")" "")
    expect_status "$label: signed by a key the JWKS does not carry" 401 "$status"

    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600 "$FOREIGN_ISSUER")" "")
    expect_status "$label: token from another issuer" 401 "$status"

    # 403, not 401: this release answers a mismatched audience that way. The body
    # is asserted too, because the status alone does not say which filter refused
    # — and the point of the case is that the audience check still stands on its
    # own, before rbac ever looks at the type marker. The wording is Envoy's, and
    # the image tag is pinned, so it changes only with a deliberate bump.
    status=$(request "$(mint_token "$TOKEN_TYPE" "icegate-api" 600)" "")
    expect_status "$label: another audience" 403 "$status"
    if grep -qi "audiences" "$WORKDIR/body"; then
        pass "$label: the audience is refused by jwt_authn, not by rbac"
    else
        fail "$label: expected an audience refusal, got '$(cat "$WORKDIR/body")'"
    fi

    # The listener routes by prefix, so the token is the whole admission rule:
    # the first case pins that jwt_authn covers a path no route names, the second
    # that the route table is not a second allow-list on top of it.
    status=$(request "" "" "$UNKNOWN_PATH")
    expect_status "$label: no token on a path outside the OTLP set" 401 "$status"

    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "" "$UNKNOWN_PATH")
    expect_status "$label: valid token on a path outside the OTLP set" 200 "$status"

    # The two sides of clock_skew_seconds: a token that expired within the
    # tolerance is still ingest, one that expired well past it is not. A clock
    # that runs ahead of the issuer's is what the first case stands for.
    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" -"$EXPIRY_WITHIN_SKEW_SECONDS")" "")
    expect_status "$label: expired within the clock skew" 200 "$status"

    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" -"$EXPIRY_PAST_SKEW_SECONDS")" "")
    expect_status "$label: expired past the clock skew" 401 "$status"

    # The two sides of the route timeout, which is the ceiling the proxy puts on
    # icegate's answer. An OTLP request is answered only once the batch is durable
    # in the WAL, so the ceiling decides whether a slow write reaches the sender as
    # a response or as a refusal it will retry — writing the batch twice. Both
    # cases address the upstream stub's /slow path and are stated against the value
    # the configuration under test carries, so removing the timeout fails the check
    # in read_route_timeout_seconds rather than passing quietly.
    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "" \
        "/slow/$((ROUTE_TIMEOUT_SECONDS + 2))")
    expect_status "$label: upstream slower than the route timeout" 504 "$status"

    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "" \
        "/slow/$FAST_UPSTREAM_SLEEP_SECONDS")
    expect_status "$label: upstream faster than the route timeout" 200 "$status"

    run_grpc_cases "$label"
}

# The gRPC listener carries the same filter chain over a different codec, and a
# refusal has to reach the exporter as a grpc-status rather than as an HTTP one:
# Envoy answers a gRPC request with 200 and puts the refusal in grpc-status,
# which is what an OTLP exporter reads.
run_grpc_cases() {
    local label="$1"
    local status seen

    status=$(request_grpc "" "")
    expect_status "$label: gRPC without a token" 200 "$status"
    seen=$(read_response_header grpc-status)
    # 16 is UNAUTHENTICATED in the gRPC status codes.
    if [ "$seen" = "16" ]; then
        pass "$label: gRPC without a token -> grpc-status 16"
    else
        fail "$label: gRPC without a token: expected grpc-status 16, got '$seen'"
    fi

    # The forged tokens of the HTTP cases, on the codec an exporter actually
    # speaks: the refusal has to arrive as UNAUTHENTICATED, the same code the
    # missing-token case above is read by.
    status=$(request_grpc "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600 "$ISSUER" "$UNPUBLISHED_KEY")" "")
    expect_status "$label: gRPC signed by a key the JWKS does not carry" 200 "$status"
    seen=$(read_response_header grpc-status)
    if [ "$seen" = "16" ]; then
        pass "$label: gRPC signed by an unpublished key -> grpc-status 16"
    else
        fail "$label: gRPC signed by an unpublished key: expected grpc-status 16, got '$seen'"
    fi

    status=$(request_grpc "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600 "$FOREIGN_ISSUER")" "")
    expect_status "$label: gRPC with a token from another issuer" 200 "$status"
    seen=$(read_response_header grpc-status)
    if [ "$seen" = "16" ]; then
        pass "$label: gRPC token from another issuer -> grpc-status 16"
    else
        fail "$label: gRPC token from another issuer: expected grpc-status 16, got '$seen'"
    fi

    # The refusal the HTTP listener answers 403 with, read through the same
    # mapping: Envoy turns a local reply's 403 into grpc-status 7, so the two
    # listeners do not agree on the marker alone — they agree on the code the
    # exporter reads.
    status=$(request_grpc "$(mint_token "$FOREIGN_TOKEN_TYPE" "$AUDIENCE" 600)" "")
    expect_status "$label: gRPC with a payload typ that is not the configured one" 200 "$status"
    seen=$(read_response_header grpc-status)
    # 7 is PERMISSION_DENIED in the gRPC status codes.
    if [ "$seen" = "7" ]; then
        pass "$label: gRPC payload typ is not the configured one -> grpc-status 7"
    else
        fail "$label: gRPC payload typ is not the configured one: expected grpc-status 7, got '$seen'"
    fi

    status=$(request_grpc "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600 "$ISSUER" "$WORKDIR/issuer.pem" \
        "$FOREIGN_HEADER_TYPE")" "")
    expect_status "$label: gRPC with a JOSE typ that is not the configured one" 200 "$status"
    seen=$(read_response_header grpc-status)
    if [ "$seen" = "7" ]; then
        pass "$label: gRPC JOSE typ is not the configured one -> grpc-status 7"
    else
        fail "$label: gRPC JOSE typ is not the configured one: expected grpc-status 7, got '$seen'"
    fi

    status=$(request_grpc "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "$FORGED_TENANT")
    expect_status "$label: gRPC with a valid token" 200 "$status"
    seen=$(read_response_header x-seen-orgid)
    if [ "$seen" = "$TENANT" ]; then
        pass "$label: gRPC upstream saw the tenant from the token claim"
    else
        fail "$label: gRPC upstream saw tenant '$seen', expected '$TENANT'"
    fi

    # The route timeout on the codec an OTLP exporter speaks: the upstream holds
    # this method past the ceiling, and what the exporter has to be told is a
    # grpc-status rather than the 504 the HTTP listener answers with. 14 is
    # UNAVAILABLE, which this release maps that 504 to — a code an exporter
    # retries, which is what makes the ceiling a choice about writing the batch
    # twice rather than about latency alone (see ingest.authProxy.upstreamTimeout
    # in the chart's values.yaml).
    status=$(request_grpc "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "" "$GRPC_SLOW_EXPORT_PATH")
    expect_status "$label: gRPC upstream slower than the route timeout" 200 "$status"
    seen=$(read_response_header grpc-status)
    if [ "$seen" = "14" ]; then
        pass "$label: gRPC upstream slower than the route timeout -> grpc-status 14"
    else
        fail "$label: gRPC upstream slower than the route timeout: expected grpc-status 14, got '$seen'"
    fi
}

main() {
    require docker
    require helm
    require openssl
    require curl

    WORKDIR=$(mktemp -d)
    UNPUBLISHED_KEY="$WORKDIR/unpublished.pem"
    chmod 755 "$WORKDIR"
    write_issuer_keys
    write_server_cert
    write_upstream
    # An empty gRPC message: the five-byte length prefix and nothing after it.
    printf '\0\0\0\0\0' >"$WORKDIR/grpc-empty"
    read_chart_render
    prepare_stand_config

    SCHEME=https
    start_stand "$WORKDIR/chart-envoy.yaml"
    run_cases "chart"
    stop_stand

    # The stand's listeners do not terminate TLS; everything else is the same.
    SCHEME=http
    start_stand "$WORKDIR/stand-envoy.yaml"
    run_cases "stand"
    stop_stand

    if [ "$FAILURES" -ne 0 ]; then
        echo "$FAILURES check(s) failed" >&2
        exit 1
    fi
    echo "auth proxy checks passed"
}

main "$@"
