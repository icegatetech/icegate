# Ingest auth proxy (example)

An example deployment that puts an Envoy sidecar in front of the icegate ingest
receivers. It is not part of the `icegate` chart and the chart knows nothing
about it: `configmap-envoy.yaml` is applied on its own, and
`values-authproxy.yaml` only fills in the chart's generic keys — the loopback
binds of the OTLP receivers plus `ingest.extraContainers` / `ingest.extraVolumes`.

## What it does

Four HTTP filters run on both listeners, and the order is the contract:

1. `header_mutation` drops whatever the client sent as `x-scope-orgid`. It has to
   be a filter, and it has to be this one: route-level
   `request_headers_to_remove` is applied by the router filter, which runs last,
   so it would delete the value `jwt_authn` wrote rather than the one the client
   sent, and every request would reach icegate with no tenant at all. The case
   `valid token plus a forged tenant header` in
   [`scripts/authproxy-test.sh`](../../../scripts/authproxy-test.sh) fails if this
   stops holding.
2. `jwt_authn` verifies the ingest token and copies its `tenant_id` claim into
   the now-empty `x-scope-orgid` header the ingest handlers read. `forward: false`
   keeps the token itself from reaching icegate.
3. `rbac` checks both halves of the token profile, and a request needs both. The
   JOSE `typ` of the header says the object is an access token rather than any
   other JOSE object the same key signs; the payload `typ` is the issuer's own
   discriminator, which says *which* application token this is. The audience
   replaces neither: it separates the ingest token from the API token, not an
   ingest token from any other token the same issuer signs for the same audience.
   The same policy demands a non-empty `tenant_id`, so that a token issued
   without one is refused here as a token rather than reaching icegate as a
   request naming no tenant. All three are pinned by that same script.
4. `router` proxies what survived, to icegate on the pod loopback.

Each listener terminates TLS from the Secret named in `values-authproxy.yaml`;
the OTLP/gRPC one also negotiates `h2` through ALPN.

## Install

```bash
kubectl apply -f config/helm/auth-proxy/configmap-envoy.yaml
helm install icegate oci://ghcr.io/icegatetech/charts/icegate \
  -f config/helm/auth-proxy/values-authproxy.yaml
```

The ConfigMap is named `ingest-authproxy`, without the release name every chart
resource carries. Two releases in one namespace therefore mount the same
configuration, and editing it for one of them changes the proxy of the other: for
a second release rename it together with the `ingest.extraVolumes[].configMap.name`
that mounts it.

## Update

Applying a changed `configmap-envoy.yaml` — another issuer, another JWKS uri,
another route timeout — leaves the running pod as it was, so restart it:

```bash
kubectl apply -f config/helm/auth-proxy/configmap-envoy.yaml
kubectl rollout restart deployment/<release>-ingest
```

The chart rolls the pod on a change to its own ConfigMap, through the
`checksum/config` annotation of `deployment-ingest.yaml`. This ConfigMap is not
the chart's — nothing renders it, so nothing hashes it, and the rollout is the
step that replaces it in the pod.

## What you must set

Four values name something outside this repository, and the example cannot know
any of them:

- `issuer` of the `ingest_token` provider — every token your issuer signs carries
  another `iss`, so each one is refused;
- `audiences` of that provider — same, refused on the audience instead;
- `remote_jwks.http_uri.uri` together with the `token_issuer_jwks` cluster (its
  endpoint `address`, its `sni`, and the `match_typed_subject_alt_names`
  matcher) — `issuer.example` sits under the TLD RFC 2606 reserves for
  documentation and resolves nowhere, so the filter holds no signing keys and
  refuses every request;
- `secretName` of the `authproxy-tls` volume — without that Secret in the
  namespace the pod never starts, because the volume cannot be mounted.

A JWKS uri on `http://` — a holder inside the same protected network — needs one
more change: the `transport_socket` of the `token_issuer_jwks` cluster is deleted
whole. Kept, Envoy opens a TLS handshake against a plain-HTTP port and holds no
signing keys, while `async_fetch` lets the proxy start and pass its `tcpSocket`
probe, so the pod is Ready and every OTLP request is answered 401 by a proxy no
icegate counter sees. `scripts/authproxy-test.sh` deletes the same block before it
loads this document, its throwaway JWKS server serving plain HTTP.

## What the chart no longer checks

These four were refused at render time while the proxy was a chart feature. The
chart no longer knows whether a proxy is there, so none of them is refused by a
render now, and what still catches two of them is a `make` target reading this
directory instead:

- an empty TLS Secret — the pod then never starts. `make helm-metadata-test`
  refuses an empty `issuer`, `audiences` or JWKS uri in this file, the three of
  that group it can read here, and an empty `issuer` is the one that fails open:
  the provider verifies no `iss` and admits every token of the same audience
  signed by a key of the same JWKS;
- a `route timeout` below the WAL acknowledgement deadline
  (`WAL_ACK_TIMEOUT` in `crates/icegate-ingest/src/wal/writer.rs`) — the proxy
  gives up before icegate acknowledges a durable write, the sender is told the
  write failed, and its retry writes the batch a second time. `make
  helm-metadata-test` compares this file's route timeouts against that constant,
  which is the one of the four that is still checked, and only for this file;
- an Ingress over these ports — they carry TLS, and what an ingress controller
  has to be told to reach a TLS backend is the controller's own setting;
- `ingest.tenant.mode: single` on an id the issuer does not emit — every batch
  signed for any other tenant is refused. `values-authproxy.yaml` sets `multi`
  for that reason.

## What it does not cover

Rate limiting, mTLS to the client, and authorization of query API requests: this
configuration fronts the OTLP receivers alone. A cluster running a service mesh
does not need it — the mesh's own mechanism decides who may reach the receivers,
and two things deciding that is one more than the number of places to look when a
request is refused.

## How it is checked

- `make envoy-config-test` loads `envoy.yaml` out of the ConfigMap with `envoy
  --mode validate`, compares the image tag of `values-authproxy.yaml` and the
  route timeouts of both listeners against `config/docker/auth-proxy/`, the second
  copy of this configuration, and compares this file's ports against the example's
  own: the upstream clusters against the receiver ports, and the listeners against
  `ingest.service.otlpHttpPort` / `ingest.service.otlpGrpcPort`, which is what the
  Service publishes;
- `make authproxy-test` runs this document against a throwaway token issuer and
  asserts the rules end to end — the header the client sent, the tenant the
  upstream receives, the three markers the policy demands (both `typ` and a
  non-empty `tenant_id`, the last of them refused 403 here rather than 400 by
  icegate), expiry within and past the clock skew, and the route timeout on both
  codecs. The script's own header lists every case;
- `make helm-metadata-test` compares the route timeouts against the WAL
  acknowledgement deadline, as described above, and refuses an empty `issuer`,
  `audiences` or JWKS uri in `configmap-envoy.yaml`.
