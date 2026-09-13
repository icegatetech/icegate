.PHONY: dev debug test check fmt fmt-fix clippy clippy-fix audit install ci bench down \
       helm-lint helm-template helm-catalog-test helm-rest-uri-test helm-tenant-test catalog-rest-check catalog-rest-test catalog-rest-clippy \
       helm-envoy-test authproxy-test \
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

# Core services behind the Envoy auth proxy (TLS off; the proxy checks the
# ingest token and rewrites x-scope-orgid). Not combinable with `load`.
run-docker-proxy-release:
	PROFILE=release docker compose -f config/docker/docker-compose.yml -f config/docker/docker-compose.proxy.yml up --build

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

ci: check fmt clippy test audit helm-lint helm-template helm-catalog-test helm-rest-uri-test helm-tenant-test helm-metadata-test catalog-rest-check catalog-rest-test catalog-rest-clippy

# The tenant policy, the auth proxy and the ingest operational listener are the
# three places where a rendering mistake fails open rather than loudly: a
# `single` policy with no id rejects every batch, a proxy that does not move
# icegate onto the pod loopback leaves the receivers published for any neighbour
# in the namespace to write to, and a loopback `ingest.metrics.host` answers the
# container alone, so both probes fail with nothing to say beyond that.
# None of the three is visible in the default render, so each is rendered here
# on purpose.
helm-tenant-test:
	@if error=$$(helm template icegate config/helm/icegate --set ingest.tenant.mode=single --set ingest.tenant.id="" 2>&1 > /dev/null); then \
		echo "expected tenant.mode=single without tenant.id to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "ingest.tenant.id is required" > /dev/null
	@if error=$$(helm template icegate config/helm/icegate --set ingest.tenant.mode=bogus 2>&1 > /dev/null); then \
		echo "expected an unknown tenant.mode to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "/ingest/tenant/mode" > /dev/null
	@if error=$$(helm template icegate config/helm/icegate --set ingest.tenant.mode=null 2>&1 > /dev/null); then \
		echo "expected an absent tenant.mode to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "ingest.tenant.mode must be single or multi" > /dev/null
	@helm template icegate config/helm/icegate --set ingest.tenant.mode=multi | grep -F "tenant: !multi" > /dev/null || { \
		echo "tenant.mode=multi must render the !multi tag"; \
		exit 1; \
	}
	@rendered=$$(helm template icegate config/helm/icegate --show-only templates/configmap-ingest.yaml) || exit 1; \
	printf '%s\n' "$$rendered" | grep -F "tenant: !single" > /dev/null || { \
		echo "the default render must carry the !single tag"; \
		exit 1; \
	}; \
	printf '%s\n' "$$rendered" | grep -F 'id: "default"' > /dev/null || { \
		echo "the default render must name the chart's tenant id"; \
		exit 1; \
	}
	@helm template icegate config/helm/icegate --show-only templates/configmap-ingest.yaml --set ingest.tenant.id=acme \
		| grep -F 'id: "acme"' > /dev/null || { \
		echo "ingest.tenant.id must reach the rendered tenant section"; \
		exit 1; \
	}
	@rendered=$$(helm template icegate config/helm/icegate --show-only templates/deployment-ingest.yaml --set ingest.metrics.enabled=false) || exit 1; \
	printf '%s\n' "$$rendered" | grep -F -e "- name: metrics" > /dev/null || { \
		echo "the metrics port must be declared even with ingest.metrics.enabled=false: the probes address it by name"; \
		exit 1; \
	}; \
	[ "$$(printf '%s\n' "$$rendered" | grep -c -F "port: metrics")" = "2" ] || { \
		echo "both probes must still address the metrics port by name"; \
		exit 1; \
	}
	@# Every spelling the guard names, the bracketed IPv6 loopback included: that
	@# is the form run_operational_server parses out of `{host}:{port}`, so a
	@# guard blind to it would refuse the roundabout spellings alone.
	@for host in 127.0.0.1 localhost "::1" "[::1]"; do \
		if error=$$(helm template icegate config/helm/icegate --set "ingest.metrics.host=$$host" 2>&1 > /dev/null); then \
			echo "expected the loopback ingest.metrics.host $$host to fail rendering: the probes address that listener on the Pod IP"; \
			exit 1; \
		fi; \
		printf '%s\n' "$$error" | grep -F "is a loopback address" > /dev/null || { \
			echo "ingest.metrics.host $$host failed the render for another reason: $$error"; \
			exit 1; \
		}; \
	done
	@# A `null` in an overlay removes the key rather than setting one, and an
	@# empty string renders a listener the ingest pod refuses on load. Both are
	@# refused by a message that names the key: read straight, the guard would
	@# abort on a type error addressing a line of _helpers.tpl instead.
	@for host in null ""; do \
		if error=$$(helm template icegate config/helm/icegate --set "ingest.metrics.host=$$host" 2>&1 > /dev/null); then \
			echo "expected ingest.metrics.host=\"$$host\" to fail rendering"; \
			exit 1; \
		fi; \
		printf '%s\n' "$$error" | grep -F "ingest.metrics.host is required" > /dev/null || { \
			echo "ingest.metrics.host=\"$$host\" failed the render for another reason: $$error"; \
			exit 1; \
		}; \
	done
	@if error=$$(helm template icegate config/helm/icegate --set ingest.authProxy.enabled=true 2>&1 > /dev/null); then \
		echo "expected authProxy.enabled without jwt settings to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "ingest.authProxy.jwt.issuer is required" > /dev/null
	@if error=$$(helm template icegate config/helm/icegate --set ingest.authProxy.enabled=true \
		--set ingest.tenant.mode=multi \
		--set ingest.authProxy.upstreamTimeout=2s \
		--set ingest.authProxy.jwt.issuer=https://api.example \
		--set ingest.authProxy.jwt.audience=icegate-ingest \
		--set ingest.authProxy.jwt.jwksUri=https://api.example/.well-known/jwks.json \
		--set ingest.authProxy.tls.secretName=ingest-tls 2>&1 > /dev/null); then \
		echo "expected an upstreamTimeout below the WAL acknowledgement deadline to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "below the WAL acknowledgement deadline" > /dev/null
	@# The deadline itself, which the message above now names as the accepted
	@# minimum. Without this case `lt` and `le` render alike on every value the
	@# repository covers, and the operator could change either way unnoticed.
	@rendered=$$(helm template icegate config/helm/icegate --set ingest.authProxy.enabled=true \
		--set ingest.tenant.mode=multi \
		--set ingest.authProxy.upstreamTimeout=3s \
		--set ingest.authProxy.jwt.issuer=https://api.example \
		--set ingest.authProxy.jwt.audience=icegate-ingest \
		--set ingest.authProxy.jwt.jwksUri=https://api.example/.well-known/jwks.json \
		--set ingest.authProxy.tls.secretName=ingest-tls) || { \
		echo "an upstreamTimeout equal to the WAL acknowledgement deadline must render"; \
		exit 1; \
	}; \
	[ "$$(printf '%s\n' "$$rendered" | grep -c -F 'timeout: "3s"')" = "2" ] || { \
		echo "the accepted upstreamTimeout must reach the route of both proxy listeners"; \
		exit 1; \
	}
	@# The tag is `required` in the template alone: values.schema.json puts no
	@# constraint on ingest.authProxy.image, so nothing else refuses an empty one,
	@# and a render without it pins no Envoy release at all.
	@if error=$$(helm template icegate config/helm/icegate --set ingest.authProxy.enabled=true \
		--set ingest.tenant.mode=multi \
		--set ingest.authProxy.image.tag=null \
		--set ingest.authProxy.jwt.issuer=https://api.example \
		--set ingest.authProxy.jwt.audience=icegate-ingest \
		--set ingest.authProxy.jwt.jwksUri=https://api.example/.well-known/jwks.json \
		--set ingest.authProxy.tls.secretName=ingest-tls 2>&1 > /dev/null); then \
		echo "expected an empty authProxy.image.tag to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "ingest.authProxy.image.tag is required" > /dev/null
	@if error=$$(helm template icegate config/helm/icegate --set ingest.authProxy.enabled=true \
		--set ingest.authProxy.jwt.issuer=https://api.example \
		--set ingest.authProxy.jwt.audience=icegate-ingest \
		--set ingest.authProxy.jwt.jwksUri=https://api.example/.well-known/jwks.json \
		--set ingest.authProxy.tls.secretName=ingest-tls 2>&1 > /dev/null); then \
		echo "expected authProxy.enabled on the default tenant to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "ingest.tenant is still the chart default" > /dev/null
	@if error=$$(helm template icegate config/helm/icegate --set ingest.authProxy.enabled=true \
		--set ingest.tenant.mode=multi \
		--set ingest.ingress.enabled=true \
		--set ingest.authProxy.jwt.issuer=https://api.example \
		--set ingest.authProxy.jwt.audience=icegate-ingest \
		--set ingest.authProxy.jwt.jwksUri=https://api.example/.well-known/jwks.json \
		--set ingest.authProxy.tls.secretName=ingest-tls 2>&1 > /dev/null); then \
		echo "expected authProxy.enabled together with ingress.enabled to fail rendering"; \
		exit 1; \
	fi; \
	printf '%s\n' "$$error" | grep -F "ingest.ingress.enabled together with ingest.authProxy.enabled" > /dev/null
	@rendered=$$(helm template icegate config/helm/icegate --set ingest.authProxy.enabled=true \
		--set ingest.tenant.mode=multi \
		--set ingest.authProxy.jwt.issuer=https://api.example \
		--set ingest.authProxy.jwt.audience=icegate-ingest \
		--set ingest.authProxy.jwt.jwksUri=https://api.example/.well-known/jwks.json \
		--set ingest.authProxy.tls.secretName=ingest-tls) || exit 1; \
	printf '%s\n' "$$rendered" | grep -F 'host: "127.0.0.1"' > /dev/null || { \
		echo "authProxy.enabled must move icegate onto the pod loopback"; \
		exit 1; \
	}; \
	printf '%s\n' "$$rendered" | grep -F "port: 14318" > /dev/null || { \
		echo "authProxy.enabled must move OTLP HTTP onto the upstream port"; \
		exit 1; \
	}; \
	printf '%s\n' "$$rendered" | grep -F "name: auth-proxy" > /dev/null || { \
		echo "authProxy.enabled must add the proxy container"; \
		exit 1; \
	}
	@# Port names are unique within a pod: the published OTLP names belong either
	@# to the proxy or to icegate, never to both. A second declaration renders and
	@# lints clean; the API server refuses it as a Duplicate value, so the release
	@# does not roll out. Counted on the deployment alone, because the Service
	@# declares the same two names and a whole-chart render would count them too.
	@rendered=$$(helm template icegate config/helm/icegate --show-only templates/deployment-ingest.yaml \
		--set ingest.authProxy.enabled=true \
		--set ingest.tenant.mode=multi \
		--set ingest.authProxy.jwt.issuer=https://api.example \
		--set ingest.authProxy.jwt.audience=icegate-ingest \
		--set ingest.authProxy.jwt.jwksUri=https://api.example/.well-known/jwks.json \
		--set ingest.authProxy.tls.secretName=ingest-tls) || exit 1; \
	for port in otlp-http otlp-grpc; do \
		count=$$(printf '%s\n' "$$rendered" | grep -c -F -e "- name: $$port"); \
		[ "$$count" = "1" ] || { \
			echo "with authProxy.enabled the pod declares $$port $$count times: port names are unique within a pod, so the published OTLP ports belong to the proxy alone"; \
			exit 1; \
		}; \
	done; \
	[ "$$(printf '%s\n' "$$rendered" | grep -c -F -e "- name: auth-proxy")" = "1" ] || { \
		echo "authProxy.enabled must add the proxy container exactly once"; \
		exit 1; \
	}
	@rendered=$$(helm template icegate config/helm/icegate --show-only templates/deployment-ingest.yaml) || exit 1; \
	for port in otlp-http otlp-grpc; do \
		count=$$(printf '%s\n' "$$rendered" | grep -c -F -e "- name: $$port"); \
		[ "$$count" = "1" ] || { \
			echo "without the proxy icegate publishes the OTLP ports itself and declares $$port $$count times instead of once"; \
			exit 1; \
		}; \
	done; \
	[ "$$(printf '%s\n' "$$rendered" | grep -c -F -e "- name: auth-proxy")" = "0" ] || { \
		echo "the default render must carry no proxy container"; \
		exit 1; \
	}

# A rendering mistake inside the proxy's envoy.yaml is invisible to the render
# checks above: they compare substrings, and Envoy is the only thing that knows
# whether the document loads. Not part of `ci`, which is the local shorthand for
# the cargo and helm-render checks; this one pulls the Envoy image and is run by
# .github/workflows/deploy-config.yml. Run it after touching the filter chain or
# either listener.
#
# The listeners terminate TLS from a Secret that exists only in the cluster, so a
# throwaway certificate is generated for the validation: without a readable key
# the load fails on the mount, not on the configuration under test.
#
# Both validations run as the invoking user, because the image's entrypoint drops
# to its own `envoy` account (uid 101) and the generated directory, the private
# key inside it and a checkout with a restrictive umask are all readable by their
# owner alone. Under the image's account Envoy reports `Invalid path` for the
# mounted document, which reads as a broken configuration and is a permission on
# the host.
#
# `--user` alone does not settle it: the entrypoint drops the process when
# ENVOY_UID is not 0 *and* the container started as uid 0, so a `make` run by
# root keeps the starting uid at 0 and is dropped to 101 anyway. ENVOY_UID=0
# falsifies the first half whatever uid the container starts as, and leaves a
# non-root `--user` untouched.
#
# The stand's compose file names the same Envoy release as the chart. Both are
# pinned by hand, and validating the stand's configuration with the chart's image
# is what makes a drift between them invisible — so the two tags are compared
# here before either configuration is loaded.
#
# The stand's loopback ports are compared the same way and for the same reason:
# config/docker/ingest-proxy.yaml binds them and config/docker/auth-proxy/envoy.yaml
# proxies to them, and a change to one of the two files leaves a stand that starts
# clean and refuses every OTLP request with a connection refused upstream. The
# chart needs no such check — there both numbers come from
# ingest.authProxy.upstream in values.yaml.
#
# The route timeout is compared across the two copies for the same reason again:
# the stand repeats the chart's default as a literal, and authproxy-test.sh runs
# both configurations on a value of its own, so nothing else looks at the number
# the stand ships. All four routes are read — both listeners of both copies —
# because that script rewrites the stand's two together and would hide a listener
# that drifted from its neighbour.
helm-envoy-test:
	@bind_http=$$(awk '/^otlp_http:/ { s = 1 } s && /^ *port:/ { print $$2; exit }' config/docker/ingest-proxy.yaml); \
	bind_grpc=$$(awk '/^otlp_grpc:/ { s = 1 } s && /^ *port:/ { print $$2; exit }' config/docker/ingest-proxy.yaml); \
	proxy_http=$$(awk '/- name: icegate_otlp_http$$/ { f = 1 } f && match($$0, /port_value: *[0-9]+/) { print substr($$0, RSTART, RLENGTH); exit }' config/docker/auth-proxy/envoy.yaml | tr -dc '0-9'); \
	proxy_grpc=$$(awk '/- name: icegate_otlp_grpc$$/ { f = 1 } f && match($$0, /port_value: *[0-9]+/) { print substr($$0, RSTART, RLENGTH); exit }' config/docker/auth-proxy/envoy.yaml | tr -dc '0-9'); \
	for pair in "OTLP HTTP:$$bind_http:$$proxy_http" "OTLP gRPC:$$bind_grpc:$$proxy_grpc"; do \
		signal=$${pair%%:*}; bound=$$(echo "$$pair" | cut -d: -f2); proxied=$$(echo "$$pair" | cut -d: -f3); \
		[ -n "$$bound" ] && [ -n "$$proxied" ] || { \
			echo "the stand configs name no $$signal loopback port (ingest-proxy.yaml: '$$bound', auth-proxy/envoy.yaml: '$$proxied')"; \
			exit 1; \
		}; \
		[ "$$bound" = "$$proxied" ] || { \
			echo "ingest-proxy.yaml binds $$signal on $$bound, auth-proxy/envoy.yaml proxies to $$proxied"; \
			exit 1; \
		}; \
	done
	@rendered=$$(helm template icegate config/helm/icegate --set ingest.authProxy.enabled=true \
		--set ingest.tenant.mode=multi \
		--set ingest.authProxy.jwt.issuer=https://api.example \
		--set ingest.authProxy.jwt.audience=icegate-ingest \
		--set ingest.authProxy.jwt.jwksUri=https://api.example/.well-known/jwks.json \
		--set ingest.authProxy.tls.secretName=ingest-tls) || exit 1; \
	dir=$$(mktemp -d); trap 'rm -rf "$$dir"' EXIT; \
	printf '%s\n' "$$rendered" | awk '/^  envoy\.yaml: \|/ { c = 1; next } c && /^    / { sub(/^    /, ""); print; next } c && NF { exit }' > "$$dir/envoy.yaml"; \
	[ -s "$$dir/envoy.yaml" ] || { echo "the chart render carried no envoy.yaml"; exit 1; }; \
	image=$$(printf '%s\n' "$$rendered" | awk '/image: envoyproxy\/envoy/ { print $$2; exit }'); \
	[ -n "$$image" ] || { echo "the render named no proxy image"; exit 1; }; \
	stand_image=$$(awk '/image: envoyproxy\/envoy/ { print $$2; exit }' config/docker/docker-compose.proxy.yml); \
	[ "$$stand_image" = "$$image" ] || { \
		echo "docker-compose.proxy.yml runs $$stand_image, the chart deploys $$image"; \
		exit 1; \
	}; \
	for route in "chart OTLP HTTP:$$dir/envoy.yaml:icegate_otlp_http" \
		"chart OTLP gRPC:$$dir/envoy.yaml:icegate_otlp_grpc" \
		"stand OTLP HTTP:config/docker/auth-proxy/envoy.yaml:icegate_otlp_http" \
		"stand OTLP gRPC:config/docker/auth-proxy/envoy.yaml:icegate_otlp_grpc"; do \
		name=$$(echo "$$route" | cut -d: -f1); file=$$(echo "$$route" | cut -d: -f2); cluster=$$(echo "$$route" | cut -d: -f3); \
		seconds=$$(awk -v cluster="$$cluster" '$$0 ~ ("cluster: " cluster) { near = 1 } near && ++lines <= 3 && match($$0, /timeout: *"?[0-9]+s/) { print substr($$0, RSTART, RLENGTH); exit }' "$$file" | tr -dc '0-9'); \
		[ -n "$$seconds" ] || { echo "the $$name route carries no timeout"; exit 1; }; \
		[ -z "$$timeout" ] && timeout=$$seconds && timeout_name=$$name; \
		[ "$$seconds" = "$$timeout" ] || { \
			echo "the $$name route times out after $${seconds}s, the $$timeout_name one after $${timeout}s"; \
			exit 1; \
		}; \
	done; \
	openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=validate \
		-keyout "$$dir/tls.key" -out "$$dir/tls.crt" 2>/dev/null; \
	docker run --rm --user "$$(id -u):$$(id -g)" -e ENVOY_UID=0 \
		-v "$$dir:/cfg:ro" -v "$$dir/tls.crt:/etc/icegate/tls/tls.crt:ro" \
		-v "$$dir/tls.key:/etc/icegate/tls/tls.key:ro" "$$image" \
		/usr/local/bin/envoy --mode validate --config-path /cfg/envoy.yaml && \
	docker run --rm --user "$$(id -u):$$(id -g)" -e ENVOY_UID=0 \
		-v "$$PWD/config/docker/auth-proxy:/cfg:ro" "$$image" \
		/usr/local/bin/envoy --mode validate --config-path /cfg/envoy.yaml

# End-to-end check of the proxy's rules against a throwaway token issuer: the
# client's tenant header is dropped, the tenant comes from the token claim, the
# payload type marker is required, and `exp` is honoured within the configured
# skew. See the script's header for what each case pins. Outside `ci` and run by
# deploy-config.yml for the same reason as helm-envoy-test.
authproxy-test:
	scripts/authproxy-test.sh

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

# Every artifact checked here is a copy of something else in the repo: the image
# annotations copy the bake targets, the values schema copies the shape of
# values.yaml, the README copies the install command, and _helpers.tpl copies the
# WAL acknowledgement deadline it checks ingest.authProxy.upstreamTimeout against.
# A copy goes stale silently, and these particular failures are invisible from
# inside the repo — a wrong image name simply means Artifact Hub scans nothing and
# the security badge disappears.
#
# Needs helm-docs on PATH to regenerate the README and diff it:
#   scripts/install-helm-docs.sh
helm-metadata-test:
	python3 scripts/helm-metadata-test.py

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
