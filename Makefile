.PHONY: dev test check fmt clippy audit install ci

dev:
	PROFILE=debug docker compose -f config/docker/docker-compose.yml up --watch --build

test:
	cargo test

check:
	cargo check --all-targets

fmt:
	cargo +nightly fmt -- --check

clippy:
	cargo clippy --all-targets -- -D warnings

audit:
	cargo audit

install:
	cargo install cargo-audit

ci: check fmt clippy test audit

