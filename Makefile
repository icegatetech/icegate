.PHONY: dev debug test check fmt fmt-fix clippy clippy-fix audit install ci

dev:
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

ci: check fmt clippy test audit
