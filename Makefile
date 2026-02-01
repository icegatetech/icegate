.PHONY: dev debug test check fmt fmt-fix clippy clippy-fix audit install ci bench

run-core-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml up --build

run-load-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml --profile load up

run-monitoring-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml --profile monitoring up

run-analytics-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml --profile analytics up

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
	cargo bench --package icegate-query

ci: check fmt clippy test audit
