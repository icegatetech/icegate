.PHONY: dev debug test check fmt fmt-fix clippy clippy-fix audit install ci bench down \
       helm-lint helm-template helm-catalog-test helm-rest-uri-test catalog-rest-check catalog-rest-test catalog-rest-clippy \
       sanitize sanitize-address sanitize-leak sanitize-memory

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

catalog-rest-check:
	cargo check -p icegate-catalog-s3 --all-targets --features rest

catalog-rest-test:
	cargo test -p icegate-catalog-s3 --all-targets --features rest

catalog-rest-clippy:
	cargo clippy -p icegate-catalog-s3 --all-targets --features rest -- -D warnings

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

ci: check fmt clippy test audit helm-lint helm-template helm-catalog-test helm-rest-uri-test catalog-rest-check catalog-rest-test catalog-rest-clippy

# The catalog server is off by default, so the default render above never covers
# its templates. Enabling it must produce a complete deployable unit, and pairing
# it with any backend other than s3 must fail the render rather than ship a
# server wired to a catalog it cannot read.
#
# The REST case carries a `catalog.rest.uri`: without one the render stops on
# that requirement (see `helm-rest-uri-test`) and this block would pass on an
# unrelated failure instead of on the catalog-server guard it exists to check.
#
# S3 addressing is rendered only when set, because an absent key is what makes
# the catalog derive the policy from the endpoint instead of forcing path-style
# onto AWS. Both halves are asserted: a chart that started emitting a default
# would silently take that choice away, and a guard that stopped emitting an
# explicit value would silently ignore the operator.
helm-catalog-test:
	@rendered=$$(helm template icegate config/helm/icegate --set catalogServer.enabled=true --set catalog.backend=s3) || exit 1; \
	for resource in "kind: Deployment" "kind: Service" "kind: ConfigMap"; do \
		printf '%s\n' "$$rendered" | grep -F -A 3 "$$resource" | grep -F "name: icegate-catalog" > /dev/null || { \
			echo "catalog server render is missing $$resource"; \
			exit 1; \
		}; \
	done
	@if error=$$(helm template icegate config/helm/icegate --set catalogServer.enabled=true --set catalog.backend=rest --set catalog.rest.uri=http://catalog.example:19120/iceberg 2>&1 > /dev/null); then \
		echo "expected catalog server with REST backend to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "catalogServer.enabled requires catalog.backend=s3" > /dev/null
	@default_render=$$(helm template icegate config/helm/icegate --set catalogServer.enabled=true --set catalog.backend=s3) || exit 1; \
	if printf '%s\n' "$$default_render" | grep -E "path.style.access|path_style_access" > /dev/null; then \
		echo "chart must leave S3 addressing unset so the catalog derives it from the endpoint"; \
		exit 1; \
	fi; \
	set_render=$$(helm template icegate config/helm/icegate --set catalogServer.enabled=true --set catalog.backend=s3 --set catalog.s3.pathStyleAccess=false) || exit 1; \
	printf '%s\n' "$$set_render" | grep -F 's3.path-style-access: "false"' > /dev/null || { \
		echo "an explicit pathStyleAccess must reach the FileIO properties"; \
		exit 1; \
	}; \
	printf '%s\n' "$$set_render" | grep -F "path_style_access: false" > /dev/null || { \
		echo "an explicit pathStyleAccess must reach the catalog server config"; \
		exit 1; \
	}

# A REST backend names an external catalog service this chart does not deploy.
# Without a default there is nothing to inherit, so the render must fail loudly
# rather than emit an empty `uri` that only surfaces as a runtime connection
# error inside the pod.
helm-rest-uri-test:
	@if error=$$(helm template icegate config/helm/icegate --set catalog.backend=rest 2>&1 > /dev/null); then \
		echo "expected backend=rest without catalog.rest.uri to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "catalog.rest.uri is required" > /dev/null

# Run the test suite under LLVM sanitizers. Linux-only (leak and memory do not
# exist on Darwin); scripts/sanitize.sh re-execs itself in a container on macOS.
# Not part of `ci` — these run nightly, see .github/workflows/sanitizers.yml.
sanitize-address:
	scripts/sanitize.sh address

sanitize-leak:
	scripts/sanitize.sh leak

# MemorySanitizer does not currently work, which is why it is excluded from
# `sanitize`. It fails at the first C file (MSan is clang-only, `cc` is GCC), and
# even with clang would false-positive on the hand-written assembly aws-lc-sys
# and ring ship. Both blockers and the remediation are in
# config/sanitizers/README.md.
sanitize-memory:
	scripts/sanitize.sh memory

sanitize: sanitize-address sanitize-leak
