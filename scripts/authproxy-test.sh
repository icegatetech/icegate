#!/usr/bin/env bash
#
# Check the ingest auth proxy against a self-contained token issuer.
#
# What it pins, and why none of it is visible in a YAML diff:
#   - the client's own x-scope-orgid never reaches icegate, and the value that
#     does comes from the token's tenant_id claim (the filter order of the
#     configuration under test is what makes this true; the route-level form of
#     the same rule would delete the token's value instead);
#   - a token whose payload `typ` is not the discriminator the configuration
#     under test admits is refused, and a token whose JOSE `typ` is not the one
#     it admits is refused too; both values are read out of that configuration
#     rather than named here;
#   - a token carrying no tenant_id claim is refused by the proxy rather than
#     forwarded: the header the claim is copied into is then absent, and icegate
#     answers such a request 400 — the batch blamed for a fault of the token;
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
#     silent allow-list of the paths icegate happens to serve today;
#   - for the example, a certificate renewed under the directory its listeners
#     read it from is served to new connections without restarting the proxy
#     (`certificate rotation`);
#   - for the example, an upstream presenting a certificate its CA signed for
#     another name than the one the configuration verifies is not forwarded to:
#     503 over HTTP, UNAVAILABLE over gRPC (`wrong upstream name`).
#
# Both copies of the filter chain are checked: the one the deployment example
# ships (config/helm/auth-proxy) and the one the Compose stand ships. They are
# meant to agree, and nothing else fails when they stop agreeing. The example
# carries TLS on both hops — its listeners, and the stubs standing in for icegate
# behind it — so its cases run over https; the stand carries none, and its cases
# run over http.
#
# The issuer here is a throwaway RSA key generated per run, and so is the CA that
# signs the certificates; no token issuer of any deployment is contacted, and the
# keys never leave the temporary directory.
#
# Requires: docker (OrbStack), openssl, curl, python3. Not part of `make ci` — see
# the `authproxy-test` target in the Makefile.
set -euo pipefail

# The issuer the stand's copy carries, read out of it rather than pinned here: a
# token minted under any other name is refused by jwt_authn before a single case
# runs. The example's copy is held to the same name below, so both configurations
# check one issuer.
readonly STAND_CONFIG="config/docker/auth-proxy/envoy.yaml"
readonly EXAMPLE_CONFIGMAP="config/helm/auth-proxy/configmap-envoy.yaml"
readonly EXAMPLE_VALUES="config/helm/auth-proxy/values-authproxy.yaml"
ISSUER=$(awk '$1 == "issuer:" { print $2; exit }' "$STAND_CONFIG" | tr -d '"')
readonly ISSUER
[ -n "$ISSUER" ] || {
    echo "$STAND_CONFIG names no issuer" >&2
    exit 1
}
# One issuer for both copies: every token is minted under the name read above, so
# a copy that names another one refuses every case of its own half of the run —
# and says only that the proxy never became ready.
example_issuer=$(awk '$1 == "issuer:" { print $2; exit }' "$EXAMPLE_CONFIGMAP" | tr -d '"')
[ "$example_issuer" = "$ISSUER" ] || {
    echo "$EXAMPLE_CONFIGMAP issues for $example_issuer, $STAND_CONFIG for $ISSUER" >&2
    exit 1
}
unset example_issuer
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
# A name no configuration verifies, under the TLD RFC 2606 reserves for
# documentation like the issuer placeholders: the certificate of the
# `wrong upstream name` case is issued for it.
readonly WRONG_UPSTREAM_NAME="wrong.example"
# What the reason the proxy gives for refusing such a certificate carries. The
# wording is the one the pinned image reports, so it changes only with a
# deliberate bump of the tag, as the audience refusal's does.
readonly UPSTREAM_VERIFY_FAILURE="CERTIFICATE_VERIFY_FAILED"
# Where the upstream stubs find the certificate they serve. Theirs alone, so the
# `wrong upstream name` case can hand them another one without touching the
# directory the proxy reads.
readonly UPSTREAM_TLS_DIR="/upstream-tls"
# How long a renewed certificate may take to reach new connections.
readonly ROTATION_TIMEOUT_SECONDS=10
# How long a replaced upstream stub may take to listen.
readonly UPSTREAM_START_TIMEOUT_SECONDS=30

# Read out of the example's values rather than pinned here: the version that
# example deploys is the version the checks have to run against. The ports each
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
# https for the example, which carries TLS on both hops, http for the stand, which
# carries none: the client requests and the upstream stubs both follow it.
SCHEME=""
# The example's TLS wiring, read out of its configuration in read_example_config
# for the reason the ports are: the stand mounts each file where Envoy reads it,
# and `make envoy-config-test` holds those paths to the mounts of the example's
# values. UPSTREAM_SERVICE_NAME is the name its upstream clusters verify, which
# the stubs standing in for icegate are issued certificates for.
EXAMPLE_SDS_PATH=""
EXAMPLE_SECRET_DIR=""
EXAMPLE_CA_DIR=""
UPSTREAM_SERVICE_NAME=""
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

WORKDIR=""
# The key the JWKS does not carry, named once because two cases address it: the
# one that mints with it and the generator that writes it.
UNPUBLISHED_KEY=""
FAILURES=0

cleanup() {
    stop_stand
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

# A server certificate the run's CA signs for the DNS names $2..., written into
# directory $1 under the two keys of a kubernetes.io/tls Secret. Readable by all,
# for the account the Envoy image runs as (the docstring of
# scripts/envoy-config-test.py says which). `x509 -req` rather than
# `req -x509 -CA`, which OpenSSL added in 3.0, so that the LibreSSL a macOS
# checkout finds first on its PATH signs it as well.
issue_server_cert() {
    local dir="$1"
    shift
    local names
    names=$(printf 'DNS:%s,' "$@")
    mkdir -p "$dir"
    printf 'subjectAltName=%s\n' "${names%,}" >"$WORKDIR/server.ext"
    openssl req -new -newkey rsa:2048 -nodes -subj "/CN=$1" \
        -keyout "$dir/tls.key" -out "$WORKDIR/server.csr" 2>/dev/null
    openssl x509 -req -days 1 -in "$WORKDIR/server.csr" \
        -CA "$WORKDIR/ca/ca.crt" -CAkey "$WORKDIR/ca.key" -CAserial "$WORKDIR/ca.srl" -CAcreateserial \
        -extfile "$WORKDIR/server.ext" -out "$dir/tls.crt" 2>/dev/null
    chmod 755 "$dir"
    chmod 644 "$dir/tls.crt" "$dir/tls.key"
}

# The CA, and the certificate icegate and the example's proxy both serve, laid out
# as the example mounts them: ca/ holds the CA ConfigMap's ca.crt, and tls/ the
# Secret. In tls/, tls.crt and tls.key are symlinks through ..data, which names
# the directory of the current version, so that check_certificate_rotation renews
# both files at once by replacing ..data alone. The certificate carries the name
# the upstream clusters verify, and localhost, the name the checks ask the
# listeners for.
write_certificates() {
    mkdir -p "$WORKDIR/ca" "$WORKDIR/tls"
    chmod 755 "$WORKDIR/ca" "$WORKDIR/tls"
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj "/CN=authproxy-test-ca" \
        -keyout "$WORKDIR/ca.key" -out "$WORKDIR/ca/ca.crt" 2>/dev/null
    chmod 644 "$WORKDIR/ca/ca.crt"
    issue_server_cert "$WORKDIR/tls/..v1" "$UPSTREAM_SERVICE_NAME" localhost
    ln -s ..v1 "$WORKDIR/tls/..data"
    ln -s ..data/tls.crt "$WORKDIR/tls/tls.crt"
    ln -s ..data/tls.key "$WORKDIR/tls/tls.key"
}

# Mint an RS256 token. $1 is the payload `typ`, $2 the audience, $3 the lifetime
# in seconds counted from now — negative for a token that has already expired.
# $4 is the issuer, $5 the signing key and $6 the JOSE `typ` of the header; all
# three default to the ones the proxy is configured to accept, and a case names
# them only when refusing them is the point. $7 is the tenant_id claim, $TENANT
# unless given: the empty string mints a token carrying no tenant_id at all,
# which is the issuing mistake the rbac policy has to refuse itself.
mint_token() {
    local payload_typ="$1" audience="$2" lifetime="$3"
    local issuer="${4:-$ISSUER}" key="${5:-$WORKDIR/issuer.pem}" header_typ="${6:-$HEADER_TYPE}"
    local tenant="${7-$TENANT}"
    local now exp header payload signing_input signature
    now=$(date +%s)
    exp=$((now + lifetime))
    header=$(printf '{"alg":"RS256","kid":"check","typ":"%s"}' "$header_typ" | base64url)
    if [ -n "$tenant" ]; then
        payload=$(printf '{"iss":"%s","aud":"%s","sub":"check","typ":"%s","tenant_id":"%s","iat":%s,"exp":%s}' \
            "$issuer" "$audience" "$payload_typ" "$tenant" "$now" "$exp" | base64url)
    else
        payload=$(printf '{"iss":"%s","aud":"%s","sub":"check","typ":"%s","iat":%s,"exp":%s}' \
            "$issuer" "$audience" "$payload_typ" "$now" "$exp" | base64url)
    fi
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
import ssl
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
server = ThreadingHTTPServer(("0.0.0.0", int(sys.argv[1])), Handler)
# A second argument names the directory of tls.crt and tls.key and turns TLS on:
# icegate serves its receivers over TLS behind the example, in plain behind the
# stand.
if len(sys.argv) > 2:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(f"{sys.argv[2]}/tls.crt", f"{sys.argv[2]}/tls.key")
    # The protocol icegate's OTLP/HTTP receiver offers: ApplicationProtocol in
    # crates/icegate-common/src/listener/server_listener.rs.
    context.set_alpn_protocols(["http/1.1"])
    server.socket = context.wrap_socket(server.socket, server_side=True)
server.serve_forever()
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
#
# Behind the example it serves TLS from $UPSTREAM_TLS_DIR, as icegate's receiver
# does once the example turns TLS on, offering the protocol that receiver offers
# (ApplicationProtocol in crates/icegate-common/src/listener/server_listener.rs).
write_grpc_upstream() {
    local delay_seconds=$((ROUTE_TIMEOUT_SECONDS + 2))
    local transport_socket=""
    if [ "$SCHEME" = "https" ]; then
        transport_socket="
          transport_socket:
            name: envoy.transport_sockets.tls
            typed_config:
              \"@type\": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
              common_tls_context:
                tls_certificates:
                  - certificate_chain: { filename: $UPSTREAM_TLS_DIR/tls.crt }
                    private_key: { filename: $UPSTREAM_TLS_DIR/tls.key }
                alpn_protocols: [\"h2\"]"
    fi
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
                      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router$transport_socket
EOF
}

# The file the example's ConfigMap carries under key $1, written to $2.
read_configmap_key() {
    local key="$1" dst="$2"
    awk -v header="  $key: |" '
        $0 == header { collecting = 1; next }
        collecting && /^    / { sub(/^    /, ""); print; next }
        collecting && NF { exit }
    ' "$EXAMPLE_CONFIGMAP" >"$dst"
    [ -s "$dst" ] || {
        echo "$EXAMPLE_CONFIGMAP carried no $key" >&2
        exit 1
    }
}

# The example's committed configuration, lifted out of its ConfigMap. The chart
# renders no proxy, so this file and the stand's are the only two copies — both
# are rewritten by prepare_config below before anything loads them. The SDS file
# its listeners read comes out of the same ConfigMap, and is loaded as committed.
read_example_config() {
    local committed="$WORKDIR/example-committed.yaml"
    read_configmap_key envoy.yaml "$committed"
    ENVOY_IMAGE=$(awk '/image: envoyproxy\/envoy/ { print $2; exit }' "$EXAMPLE_VALUES")
    [ -n "$ENVOY_IMAGE" ] || {
        echo "$EXAMPLE_VALUES names no proxy image" >&2
        exit 1
    }

    # The gRPC listener and cluster refer to what the HTTP ones write by alias,
    # so each value is read where it is written.
    EXAMPLE_SDS_PATH=$(read_entry_block "$committed" otlp_http | read_scalar path)
    [ -n "$EXAMPLE_SDS_PATH" ] || {
        echo "the otlp_http listener of $EXAMPLE_CONFIGMAP names no SDS path" >&2
        exit 1
    }
    read_configmap_key "${EXAMPLE_SDS_PATH##*/}" "$WORKDIR/example-sds.yaml"
    EXAMPLE_SECRET_DIR=$(read_scalar path <"$WORKDIR/example-sds.yaml")
    EXAMPLE_CA_DIR=$(read_entry_block "$committed" icegate_otlp_http | read_scalar filename)
    EXAMPLE_CA_DIR="${EXAMPLE_CA_DIR%/*}"
    UPSTREAM_SERVICE_NAME=$(read_entry_block "$committed" icegate_otlp_http | read_scalar sni)
    case "$UPSTREAM_SERVICE_NAME" in
    "" | "*"*)
        echo "the icegate_otlp_http cluster of $EXAMPLE_CONFIGMAP writes no name as its sni" >&2
        exit 1
        ;;
    esac
    [ -n "$EXAMPLE_SECRET_DIR" ] && [ -n "$EXAMPLE_CA_DIR" ] || {
        echo "$EXAMPLE_CONFIGMAP names no watched_directory or no CA of icegate_otlp_http" >&2
        exit 1
    }
}

# The listener or cluster named $2 of document $1: from its own `- name:` line to
# the next entry at that indent, or to the end of the document for the last one.
read_entry_block() {
    awk -v entry="    - name: $2" '
        $0 == entry { collecting = 1; print; next }
        collecting && /^    - name: / { exit }
        collecting { print }
    ' "$1"
}

# The value of the first `$1:` key of the document on stdin, as written: a flow
# mapping's brace is not part of it, and neither is an anchor the value carries —
# the example writes its upstream name once, as `sni: &<anchor> <name>`.
read_scalar() {
    awk -v key="$1" '
        match($0, key ": *(&[^ ]+ +)?[^ ,}]+") {
            value = substr($0, RSTART, RLENGTH)
            sub(key ": *(&[^ ]+ +)?", "", value)
            print value
            exit
        }
    '
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
# name and within the three lines that follow it, rather than as the first
# `timeout:` of the document: the jwt_authn provider carries one of its own for
# the JWKS fetch. Both copies write the route's on the line that names the
# cluster; the three-line reach covers a copy that spells it as a `route:` block
# instead, which loads the same and is what the rewrite in prepare_config would
# miss — assert_config_rewritten refuses that copy by name.
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
# for the JOSE `typ`, jwt_payload for the claim. Read rather than pinned: both
# markers belong to the token issuer, so a deployment that renamed either must
# still be checked against the value it configured. Addressed by metadata key
# rather than by the first string_match of
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

# A committed copy, with the issuer address pointed at the throwaway JWKS server
# and the route timeout put on the value this check runs under (see
# ROUTE_TIMEOUT_UNDER_TEST_SECONDS); the filter chain, which is the thing under
# test, is used exactly as committed. Serves both copies: they write these three
# places in the same form, which is what assert_config_rewritten checks before
# either is loaded.
prepare_config() {
    local src="$1" dst="$2"
    sed -e "s#$ISSUER/.well-known/jwks.json#http://$JWKS_CONTAINER:$JWKS_PORT/jwks.json#" \
        -e "/cluster: icegate_otlp_/ s/timeout: \"[0-9]*s\"/timeout: \"${ROUTE_TIMEOUT_UNDER_TEST_SECONDS}s\"/" \
        "$src" |
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
        ' >"$dst"
}

# A copy whose JWKS address or route timeout is written in another form passes
# through the rewrite untouched and fails sixty seconds later as a proxy that
# never became ready. Named here instead, against the document about to be loaded
# ($2), which is compared with the committed one it was rewritten from ($1).
#
# Stated as three places the rewrite must have reached, not as the absence of
# $ISSUER_HOST from the document: the `issuer` of the jwt_authn provider names
# that host too and has to keep naming it. mint_token signs `iss` with $ISSUER
# and the filter compares the claim against that string, so a rewrite there
# answers every case 401.
assert_config_rewritten() {
    local committed="$1" config="$2" cluster cluster_name seconds

    grep -q "uri: \"http://$JWKS_CONTAINER:$JWKS_PORT/jwks.json\"" "$config" || {
        echo "$config does not fetch the JWKS from the throwaway server:" >&2
        grep -n "uri:" "$config" >&2
        exit 1
    }

    cluster=$(read_entry_block "$config" token_issuer_jwks)
    [ -n "$cluster" ] || {
        echo "$config carries no token_issuer_jwks cluster" >&2
        exit 1
    }
    case "$cluster" in
    *"$ISSUER_HOST"*)
        echo "the token_issuer_jwks cluster of $config still addresses $ISSUER_HOST," \
            "which resolves nowhere, so jwt_authn fetches no keys:" >&2
        printf '%s\n' "$cluster" >&2
        exit 1
        ;;
    esac
    case "$cluster" in
    *transport_socket*)
        echo "the token_issuer_jwks cluster of $config still speaks TLS; the throwaway" \
            "JWKS server serves plain HTTP" >&2
        exit 1
        ;;
    esac

    # The transport_socket the rewrite drops is the JWKS cluster's alone. The two
    # icegate clusters precede it, and the example's carry their own, which has to
    # survive: without it the proxy would speak plain text to stubs serving TLS
    # and every case would fail on a 503 that names nothing of the cause. Compared
    # whole with the committed copy, which states that for both copies alike.
    for cluster_name in icegate_otlp_http icegate_otlp_grpc; do
        if [ "$(read_entry_block "$committed" "$cluster_name")" != \
            "$(read_entry_block "$config" "$cluster_name")" ]; then
            echo "the rewrite of $config changed its $cluster_name cluster, not only the" \
                "token_issuer_jwks one:" >&2
            diff <(read_entry_block "$committed" "$cluster_name") \
                <(read_entry_block "$config" "$cluster_name") >&2 || true
            exit 1
        fi
    done

    seconds=$(read_route_timeout_seconds "$config" icegate_otlp_http)
    [ "$seconds" = "$ROUTE_TIMEOUT_UNDER_TEST_SECONDS" ] || {
        echo "$config times the OTLP/HTTP route out after ${seconds}s, not the" \
            "${ROUTE_TIMEOUT_UNDER_TEST_SECONDS}s this check runs under: the route timeout is" \
            "written in a form the rewrite does not reach" >&2
        exit 1
    }
}

start_stand() {
    local config="$1"
    read_config_values "$config"
    write_grpc_upstream
    docker network create "$NETWORK" >/dev/null
    docker run -d --name "$JWKS_CONTAINER" --network "$NETWORK" \
        -v "$WORKDIR:/w:ro" -w /w "$PYTHON_IMAGE" \
        python3 -m http.server "$JWKS_PORT" >/dev/null
    # The example's proxy reads the SDS file, icegate's certificate and the CA
    # where its configuration names them. The certificate is a directory mount,
    # as the Secret volume is, so a renewal inside it reaches the proxy.
    local proxy_mounts=(-v "$config:/etc/envoy/envoy.yaml:ro")
    if [ "$SCHEME" = "https" ]; then
        proxy_mounts+=(
            -v "$WORKDIR/example-sds.yaml:$EXAMPLE_SDS_PATH:ro"
            -v "$WORKDIR/tls:$EXAMPLE_SECRET_DIR:ro"
            -v "$WORKDIR/ca:$EXAMPLE_CA_DIR:ro"
        )
    fi
    # The proxy owns the network namespace the upstream stubs join, so both
    # published ports are declared on it, and the stubs can be replaced under a
    # proxy that keeps running — which check_upstream_name_mismatch does.
    docker run -d --name "$PROXY_CONTAINER" --network "$NETWORK" \
        -p "$PUBLISHED_HTTP_PORT:$LISTENER_HTTP_PORT" \
        -p "$PUBLISHED_GRPC_PORT:$LISTENER_GRPC_PORT" \
        "${proxy_mounts[@]}" \
        "$ENVOY_IMAGE" /usr/local/bin/envoy --config-path /etc/envoy/envoy.yaml >/dev/null
    start_upstreams "$WORKDIR/tls"
    wait_for_stand
}

# The two stubs standing in for icegate, in the proxy's network namespace, where
# they bind the loopback ports its upstream clusters dial. Behind the example both
# serve TLS from the certificate in directory $1; behind the stand neither does.
# A proxy that refused its configuration has exited by now and has no namespace
# to join, so a stub that cannot start reports the proxy's log rather than
# Docker's complaint.
start_upstreams() {
    local cert_dir="$1"
    local http_mounts=(-v "$WORKDIR:/w:ro")
    local http_command=(python3 /w/upstream.py "$UPSTREAM_HTTP_PORT")
    local grpc_mounts=(-v "$WORKDIR/grpc-upstream.yaml:/etc/envoy/envoy.yaml:ro")
    if [ "$SCHEME" = "https" ]; then
        http_mounts+=(-v "$cert_dir:$UPSTREAM_TLS_DIR:ro")
        http_command+=("$UPSTREAM_TLS_DIR")
        grpc_mounts+=(-v "$cert_dir:$UPSTREAM_TLS_DIR:ro")
    fi
    docker run -d --name "$UPSTREAM_CONTAINER" --network "container:$PROXY_CONTAINER" \
        "${http_mounts[@]}" "$PYTHON_IMAGE" "${http_command[@]}" >/dev/null ||
        exit_with_proxy_log "the upstream stub could not join the proxy"
    # --base-id moves this Envoy's hot-restart domain socket off the default: the
    # socket lives in the network namespace, which this container shares with the
    # proxy, and the second Envoy to start would otherwise fail to bind it. The
    # proxy keeps the default, so it runs exactly as the example deploys it.
    docker run -d --name "$GRPC_UPSTREAM_CONTAINER" --network "container:$PROXY_CONTAINER" \
        "${grpc_mounts[@]}" \
        "$ENVOY_IMAGE" /usr/local/bin/envoy --base-id 1 --config-path /etc/envoy/envoy.yaml >/dev/null ||
        exit_with_proxy_log "the gRPC upstream stub could not join the proxy"
}

exit_with_proxy_log() {
    echo "$1; envoy log follows" >&2
    docker logs "$PROXY_CONTAINER" >&2 || true
    exit 1
}

# The stubs again, now serving the certificate in directory $1, under the proxy
# that keeps running.
replace_upstreams() {
    docker rm -f "$GRPC_UPSTREAM_CONTAINER" "$UPSTREAM_CONTAINER" >/dev/null
    start_upstreams "$1"
}

# The proxy answers only once it has fetched the JWKS, so readiness is polled
# rather than slept for; a token is what proves the fetch completed. The gRPC
# listener is polled on the tenant its upstream reports rather than on the HTTP
# status: Envoy answers a gRPC request 200 whatever the outcome, so that status is
# already 200 while the JWKS is still missing and while the gRPC upstream stub is
# still starting. The x-seen-orgid header appears only once the proxy has admitted
# the request and the stub has answered it.
wait_for_stand() {
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
    exit_with_proxy_log "the proxy did not become ready"
}

# The stubs go before the proxy whose network namespace they joined.
stop_stand() {
    docker rm -f "$GRPC_UPSTREAM_CONTAINER" "$UPSTREAM_CONTAINER" "$PROXY_CONTAINER" \
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

    # A token the issuer signed without the claim the tenant comes from. The
    # refusal belongs to the proxy: claim_to_headers writes no header for an
    # absent claim, so without this the request reaches icegate and is answered
    # 400 — a status naming the batch for a fault that is the token's.
    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600 "$ISSUER" "$WORKDIR/issuer.pem" \
        "$HEADER_TYPE" "")" "")
    expect_status "$label: token without a tenant_id claim" 403 "$status"

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

    status=$(request_grpc "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600 "$ISSUER" "$WORKDIR/issuer.pem" \
        "$HEADER_TYPE" "")" "")
    expect_status "$label: gRPC with a token without a tenant_id claim" 200 "$status"
    seen=$(read_response_header grpc-status)
    if [ "$seen" = "7" ]; then
        pass "$label: gRPC token without a tenant_id claim -> grpc-status 7"
    else
        fail "$label: gRPC token without a tenant_id claim: expected grpc-status 7, got '$seen'"
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
    # twice rather than about latency alone (see the route timeout of
    # config/helm/auth-proxy/configmap-envoy.yaml).
    status=$(request_grpc "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "" "$GRPC_SLOW_EXPORT_PATH")
    expect_status "$label: gRPC upstream slower than the route timeout" 200 "$status"
    seen=$(read_response_header grpc-status)
    if [ "$seen" = "14" ]; then
        pass "$label: gRPC upstream slower than the route timeout -> grpc-status 14"
    else
        fail "$label: gRPC upstream slower than the route timeout: expected grpc-status 14, got '$seen'"
    fi
}

# SHA-256 fingerprint of the certificate the OTLP/HTTP listener serves a new
# connection, or the empty string when it serves none.
read_served_fingerprint() {
    openssl s_client -connect "127.0.0.1:$PUBLISHED_HTTP_PORT" -servername localhost </dev/null 2>/dev/null |
        openssl x509 -noout -fingerprint -sha256 2>/dev/null || true
}

# A certificate renewed under the directory the listeners read it from reaches new
# connections while the proxy keeps running. The renewal is a second certificate of
# the same CA in a version directory of its own, swapped in by replacing ..data
# with one rename (see write_certificates). A Docker host whose bind mounts deliver
# no file events fails this case on the timeout rather than passing it, and the
# failure then says nothing about the proxy: the environment cannot run the case.
check_certificate_rotation() {
    local label="$1" renewed served deadline status
    issue_server_cert "$WORKDIR/tls/..v2" "$UPSTREAM_SERVICE_NAME" localhost
    renewed=$(openssl x509 -in "$WORKDIR/tls/..v2/tls.crt" -noout -fingerprint -sha256)
    served=$(read_served_fingerprint)
    if [ -z "$served" ] || [ "$served" = "$renewed" ]; then
        fail "$label: certificate rotation: the listener serves '$served' before the renewal"
        return
    fi
    ln -s ..v2 "$WORKDIR/tls/..data_tmp"
    # os.replace is rename(2), which replaces the old link in one step; BSD mv
    # has no -T, and without it would move the new link into the directory the
    # old one names.
    python3 -c 'import os, sys; os.replace(sys.argv[1], sys.argv[2])' \
        "$WORKDIR/tls/..data_tmp" "$WORKDIR/tls/..data"

    deadline=$(($(date +%s) + ROTATION_TIMEOUT_SECONDS))
    while served=$(read_served_fingerprint) && [ "$served" != "$renewed" ]; do
        if [ "$(date +%s)" -ge "$deadline" ]; then
            fail "$label: certificate rotation: new connections are still served '$served'" \
                "${ROTATION_TIMEOUT_SECONDS}s after the renewal to '$renewed'"
            return
        fi
        sleep 0.5
    done
    pass "$label: certificate rotation: new connections are served the renewed certificate"

    status=$(request "$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)" "")
    expect_status "$label: valid ingest token after the certificate rotation" 200 "$status"
}

# An upstream presenting a certificate the trusted CA signed for another name is
# not forwarded to: the stubs are replaced, under the running proxy, by ones
# serving such a certificate. The proxy answers 503 over HTTP and UNAVAILABLE over
# gRPC — 14, which an exporter retries — and on both the reason names the failed
# verification, which is what tells this refusal apart from a stub that is still
# starting. The stubs then get a certificate of the same CA for the right name
# back, and the stand has to become ready on it before this returns: that is what
# makes the name, and not the CA, the cause of the refusal.
check_upstream_name_mismatch() {
    local label="$1" token status seen
    issue_server_cert "$WORKDIR/wrong-name" "$WRONG_UPSTREAM_NAME"
    replace_upstreams "$WORKDIR/wrong-name"
    token=$(mint_token "$TOKEN_TYPE" "$AUDIENCE" 600)

    for _ in $(seq 1 "$UPSTREAM_START_TIMEOUT_SECONDS"); do
        status=$(request "$token" "")
        if [ "$status" = "503" ] && grep -q "$UPSTREAM_VERIFY_FAILURE" "$WORKDIR/body"; then
            break
        fi
        sleep 1
    done
    expect_status "$label: wrong upstream name" 503 "$status"
    if grep -q "$UPSTREAM_VERIFY_FAILURE" "$WORKDIR/body"; then
        pass "$label: wrong upstream name is refused on the certificate verification"
    else
        fail "$label: wrong upstream name: expected a reason naming $UPSTREAM_VERIFY_FAILURE," \
            "got '$(cat "$WORKDIR/body")'"
    fi

    for _ in $(seq 1 "$UPSTREAM_START_TIMEOUT_SECONDS"); do
        status=$(request_grpc "$token" "")
        seen=$(read_response_header grpc-status)
        if [ "$seen" = "14" ] && read_response_header grpc-message | grep -q "$UPSTREAM_VERIFY_FAILURE"; then
            break
        fi
        sleep 1
    done
    expect_status "$label: gRPC wrong upstream name" 200 "$status"
    if [ "$seen" = "14" ] && read_response_header grpc-message | grep -q "$UPSTREAM_VERIFY_FAILURE"; then
        pass "$label: gRPC wrong upstream name -> grpc-status 14 on the certificate verification"
    else
        fail "$label: gRPC wrong upstream name: expected grpc-status 14 naming $UPSTREAM_VERIFY_FAILURE," \
            "got '$seen' ($(read_response_header grpc-message))"
    fi

    replace_upstreams "$WORKDIR/tls"
    wait_for_stand
}

main() {
    require docker
    require openssl
    require curl
    require python3

    WORKDIR=$(mktemp -d)
    UNPUBLISHED_KEY="$WORKDIR/unpublished.pem"
    chmod 755 "$WORKDIR"
    write_issuer_keys
    write_upstream
    # An empty gRPC message: the five-byte length prefix and nothing after it.
    printf '\0\0\0\0\0' >"$WORKDIR/grpc-empty"
    read_example_config
    write_certificates
    prepare_config "$WORKDIR/example-committed.yaml" "$WORKDIR/example-envoy.yaml"
    assert_config_rewritten "$WORKDIR/example-committed.yaml" "$WORKDIR/example-envoy.yaml"
    prepare_config "$STAND_CONFIG" "$WORKDIR/stand-envoy.yaml"
    assert_config_rewritten "$STAND_CONFIG" "$WORKDIR/stand-envoy.yaml"

    SCHEME=https
    start_stand "$WORKDIR/example-envoy.yaml"
    run_cases "example"
    check_certificate_rotation "example"
    check_upstream_name_mismatch "example"
    stop_stand

    # The stand carries no TLS on either hop; everything else is the same.
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
