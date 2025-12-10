.PHONY: dev test check fmt clippy audit install ci

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
	cargo clippy --all-targets -- -D warnings

clippy-fix:
	cargo clippy --all-targets --fix --allow-dirty

audit:
	cargo audit

install:
	cargo install cargo-audit

ci: check fmt clippy test audit

