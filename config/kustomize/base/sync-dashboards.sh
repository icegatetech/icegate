#!/usr/bin/env bash
# Copies Grafana dashboards from Docker Compose layout to kustomize base,
# applying label transformations required for Kubernetes:
#   - job="ingest" → job="icegate-ingest"
#   - Nodes dashboard: "name" label → "pod" label
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$SCRIPT_DIR/../../docker/grafana/dashboards"
DST_DIR="$SCRIPT_DIR/dashboards"

mkdir -p "$DST_DIR"

for src in "$SRC_DIR"/*.json; do
  filename="$(basename "$src")"
  dst="$DST_DIR/$filename"

  # Common fix: job="ingest" → job="icegate-ingest"
  sed 's/job=\\"ingest\\"/job=\\"icegate-ingest\\"/g' "$src" > "$dst"

  # Nodes dashboard: replace Docker-specific "name" label with k8s "pod" label
  # Covers: PromQL selectors {name=…}, grouping by (name), legend {{name}},
  # and JSON key "name" — but not unrelated fields like "names" or "renameByName".
  if [[ "$filename" == Nodes* ]]; then
    sed -i.bak \
        -e 's/{name}/{pod}/g' \
        -e 's/{name=/{pod=/g' \
        -e 's/by (name)/by (pod)/g' \
        -e 's/{{name}}/{{pod}}/g' \
        -e 's/"name"/"pod"/g' \
        -e 's/"Name"/"Pod"/g' "$dst"
    rm -f "$dst.bak"
  fi

  echo "Synced: $filename"
done