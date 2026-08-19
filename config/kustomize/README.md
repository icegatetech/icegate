# Kustomize Configuration

All overlays in this directory are **demo-only** environments intended for local development and testing. Secrets use placeholder values, Grafana has anonymous admin access, and security settings are relaxed for convenience.

## Structure

```text
kustomize/
├── base/                  # Shared infrastructure & observability stack
└── overlays/
    ├── skaffold/          # Local development (local Helm chart)
    ├── orbstack/          # Local development (local Helm chart + CRDs)
    ├── external-s3/       # K3s with external S3
    └── aws-glue/          # AWS with Glue catalog (no RustFS)
```

## Base

Deploys the shared infrastructure and observability stack used by all overlays:

- **Prometheus** (kube-prometheus-stack) — metrics collection
- **Grafana** — dashboards

All observability resources are deployed to the `observability` namespace. Infrastructure resources use the `infra` namespace.

## Overlays

### `skaffold`

Self-contained local development overlay. Deploys the full stack — IceGate (from the local Helm chart at `config/helm/icegate/`), RustFS, and the observability base — so changes to chart templates are picked up immediately without publishing to OCI.

**Includes:** IceGate (local chart), RustFS (S3-compatible storage), observability stack, AWS secrets, icegate namespace.

**Deploy (preferred):**

```bash
skaffold dev   # Hot-reload on code changes
skaffold run   # One-shot deploy
```

**Deploy (kustomize only):**

```bash
kustomize build --enable-helm config/kustomize/overlays/skaffold | kubectl apply --server-side --force-conflicts -f -
```

### `orbstack`

Local development overlay for OrbStack Kubernetes. Deploys everything via kustomize, including IceGate from the local Helm chart at `config/helm/icegate/` and the Prometheus Operator CRDs the ServiceMonitors need.

**Includes:** RustFS, IceGate (local chart), observability stack, AWS secrets, CRDs.

**Deploy:**

```bash
kustomize build --enable-helm config/kustomize/overlays/orbstack | kubectl apply --server-side --force-conflicts -f -
```

### `external-s3`

For K3s clusters with an external S3-compatible endpoint. Does not deploy RustFS — storage points to an external S3 bucket. The catalog is IceGate's own S3-backed one, served over the read-only Iceberg REST endpoint at `icegate-catalog.icegate.svc.cluster.local:8181`. That Service is `ClusterIP`, so it reaches engines running inside the cluster only — nothing publishes it outside.

**Includes:** IceGate (OCI chart) with the catalog REST server, observability stack, AWS secrets. Exposes Grafana and Ingest services via Tailscale.

**Chart version.** Pinned to the pre-release `0.2.0-rc5` because the `catalogServer` component landed in the chart after the stable `0.1.1`: on that version `catalogServer.enabled: true` renders neither a Deployment nor a Service, and the overlay would come up silently without the catalog server.

**Deploy:**

```bash
kustomize build --enable-helm config/kustomize/overlays/external-s3 | kubectl apply --server-side --force-conflicts -f -
```

### `aws-glue`

For AWS deployments using Glue as the Iceberg catalog. No RustFS — uses real AWS S3 and Glue directly.

**Includes:** IceGate (OCI chart), observability stack, AWS secrets. Exposes Grafana and Ingest services via Tailscale.

**Chart version.** Pinned to the pre-release `0.2.0-rc5` on purpose, in step with `external-s3`: this is a demo environment, so it follows the chart under development rather than the last stable tag.

**Deploy:**

```bash
kustomize build --enable-helm config/kustomize/overlays/aws-glue | kubectl apply --server-side --force-conflicts -f -
```

## Render checks

Overlay rendering is checked by hand only. CI covers the Helm chart alone (`.github/workflows/helm-lint.yml`). Chart sources differ per overlay: the base pulls `kube-prometheus-stack` and `grafana` from `prometheus-community` and `grafana`, `skaffold` and `orbstack` pull RustFS from `charts.rustfs.com` and take the IceGate chart locally (`helmGlobals.chartHome`), `external-s3` and `aws-glue` pull it from `oci://ghcr.io/icegatetech/charts`. What holds for all four is the base, so rendering any overlay needs those registries reachable and a CI step would report their availability alongside a defect in this repository.

After changing an overlay, its values, or the chart version it pins, render it:

```bash
kustomize build --enable-helm config/kustomize/overlays/<name> > /dev/null
```

A failure here is what a wrong chart version, an unmet `required` value, or a patch whose target no longer exists looks like before it reaches a cluster.
