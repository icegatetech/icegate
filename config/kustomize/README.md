# Kustomize Configuration

All overlays in this directory are **demo-only** environments intended for local development and testing. Secrets use placeholder values, Grafana has anonymous admin access, and security settings are relaxed for convenience.

## Structure

```text
kustomize/
├── base/                  # Shared infrastructure & observability stack
└── overlays/
    ├── skaffold/          # Local development (local Helm chart)
    ├── orbstack/          # Local development (OCI Helm chart)
    ├── external-s3/       # K3s with external S3 + Nessie
    └── aws-glue/          # AWS with Glue catalog (no MinIO/Nessie)
```

## Base

Deploys the shared infrastructure and observability stack used by all overlays:

- **Prometheus** (kube-prometheus-stack) — metrics collection
- **Grafana** — dashboards
- **Jaeger** — distributed tracing

All observability resources are deployed to the `observability` namespace. Infrastructure resources use the `infra` namespace.

## Overlays

### `skaffold`

Self-contained local development overlay. Deploys the full stack — IceGate (from the local Helm chart at `config/helm/icegate/`), MinIO, Nessie, and the observability base — so changes to chart templates are picked up immediately without publishing to OCI.

**Includes:** IceGate (local chart), MinIO (S3-compatible storage), Nessie (Iceberg REST catalog), observability stack, AWS secrets, icegate namespace.

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

Local development overlay for OrbStack Kubernetes. Deploys everything via kustomize, including the IceGate chart from the OCI registry (`oci://ghcr.io/icegatetech/charts`).

**Includes:** MinIO, Nessie, IceGate (OCI chart), observability stack, AWS secrets.

**Deploy:**

```bash
kustomize build --enable-helm config/kustomize/overlays/orbstack | kubectl apply --server-side --force-conflicts -f -
```

### `external-s3`

For K3s clusters with an external S3-compatible endpoint. Runs Nessie locally but does not deploy MinIO — storage points to an external S3 bucket.

**Includes:** Nessie, IceGate (OCI chart), observability stack, AWS secrets. Exposes Grafana and Ingest services via Tailscale.

**Deploy:**

```bash
kustomize build --enable-helm config/kustomize/overlays/external-s3 | kubectl apply --server-side --force-conflicts -f -
```

### `aws-glue`

For AWS deployments using Glue as the Iceberg catalog. No MinIO or Nessie — uses real AWS S3 and Glue directly.

**Includes:** IceGate (OCI chart), observability stack, AWS secrets. Exposes Grafana and Ingest services via Tailscale.

**Deploy:**

```bash
kustomize build --enable-helm config/kustomize/overlays/aws-glue | kubectl apply --server-side --force-conflicts -f -
```
