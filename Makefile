.PHONY: dev debug test check fmt fmt-fix clippy clippy-fix audit install ci bench down \
       helm-lint helm-template

run-docker-core-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml up --build

# Run core services with otlp ingestion via otelgen
run-docker-load-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml --profile load up --build

# Run core services with monitoring
run-docker-monitoring-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml --profile monitoring up --build --force-recreate

# Run core services with Trino
run-docker-analytics-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml --profile analytics up --build

run-kubernetes-core-release:
	kustomize  build --enable-helm config/kustomize/overlays/orbstack | kubectl apply --server-side --force-conflicts -f - || true
	kustomize  build --enable-helm config/kustomize/overlays/orbstack | kubectl apply --server-side --force-conflicts -f -

dev:
	PROFILE=debug docker build --build-arg PROFILE=debug -f config/docker/Dockerfile .
	PROFILE=debug docker compose -f config/docker/docker-compose.yml up --watch --build

debug:
	QUERY_REPLICAS=0 PROFILE=debug docker compose -f config/docker/docker-compose.yml up

test:
	cargo test

check:
	cargo check --all-targets

fmt:
	cargo +nightly fmt -- --check

fmt-fix:
	cargo +nightly fmt

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

clippy-fix:
	cargo clippy --workspace --all-targets --fix --allow-dirty

audit:
	cargo audit

install:
	cargo install cargo-audit

bench:
	cargo bench --bench queue_s3_bench --bench loki_queries -- --output-format bencher | tee output.txt

down:
	docker compose -f config/docker/docker-compose.yml down

helm-lint:
	helm lint config/helm/icegate

helm-template:
	helm template icegate config/helm/icegate > /dev/null

ci: check fmt clippy test audit helm-lint helm-template
