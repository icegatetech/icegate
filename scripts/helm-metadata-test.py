#!/usr/bin/env python3
"""Assert every copy the chart carries still matches the source it came from.

Each artifact checked here is a copy of something else in the repo — the Artifact
Hub metadata, the values schema, the generated README, and the WAL acknowledgement
deadline the auth proxy example must not undercut — so each check names the source
it must agree with. A failure means the copy drifted, not that the source is wrong.

The auth proxy example is also checked against itself, for the values no source in
this repo holds: the chart refused an empty issuer, audience or JWKS uri while the
proxy was a chart feature, and nothing refuses them now that it is deployed
alongside. That check is here rather than in the chart because the example is the
only file that carries them.
"""
import re
import subprocess
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from chartlib import (  # noqa: E402  (path set above so this file runs from the repo root)
    CHART_DIR,
    fail,
    load_document,
    ok,
    read_configmap_file,
    read_envoy_route_timeouts,
    report_failures,
)

CHART = CHART_DIR / "Chart.yaml"
EXAMPLE_CONFIGMAP = Path("config/helm/auth-proxy/configmap-envoy.yaml")
BAKE = Path("config/docker/docker-bake.hcl")
RELEASE = Path(".github/workflows/release.yml")
WAL_WRITER = Path("crates/icegate-ingest/src/wal/writer.rs")

chart = load_document(CHART)
envoy_document = read_configmap_file(EXAMPLE_CONFIGMAP, "envoy.yaml")
annotations = chart.get("annotations", {})

# 1. Images match the bake targets and carry appVersion's tag. A stale image name
#    means Artifact Hub scans nothing and the security badge silently disappears —
#    a failure that is invisible from inside the repo.
targets = set(re.findall(r'^target "([a-z]+)"', BAKE.read_text(), re.M)) - {"_common"}
raw_images = annotations.get("artifacthub.io/images")
if not raw_images:
    fail("Chart.yaml has no artifacthub.io/images annotation")
else:
    images = yaml.safe_load(raw_images)
    names = {i["name"] for i in images}
    if names != targets:
        fail(f"artifacthub.io/images {sorted(names)} != bake targets {sorted(targets)}")
    else:
        ok(f"images match bake targets: {sorted(names)}")

    app_version = str(chart["appVersion"])
    tags_ok = True
    for image in images:
        ref = image["image"]
        # The repository suffix must equal the entry's name, or an entry can claim
        # `name: catalog` while pointing at an image that was never built.
        expected_repo = f"ghcr.io/icegatetech/icegate-{image['name']}"
        if not ref.startswith(f"{expected_repo}:"):
            fail(f"image {ref} does not match its name: expected {expected_repo}:<tag>")
            tags_ok = False
        tag = ref.rsplit(":", 1)[-1]
        if tag != app_version:
            fail(f"image {ref} tag {tag!r} != appVersion {app_version!r}")
            tags_ok = False
        if image.get("platforms") != ["linux/amd64", "linux/arm64"]:
            fail(f"image {ref} platforms {image.get('platforms')} != the bake file's")
            tags_ok = False
    if tags_ok:
        ok(f"image tags match appVersion {app_version}")

# 2. Version placeholders still match what release.yml's sed rewrites. A reformat
#    here breaks stamping silently, and only at tag time.
release = RELEASE.read_text()
for field in ("version", "appVersion"):
    if f"s/^{field}:.*/" not in release:
        fail(f"release.yml no longer stamps {field}; update this check or the workflow")
    if not re.search(rf"^{field}:", CHART.read_text(), re.M):
        fail(f"Chart.yaml has no line starting with {field}: for release.yml to rewrite")
ok("release.yml stamp patterns still match Chart.yaml")

# 3. The values schema accepts everything that installs today.
values_files = sorted(Path("config/kustomize/overlays").glob("*/values-icegate.yaml"))
if not values_files:
    fail("no overlay values files found; the schema would go effectively untested")
for extra in [None, *values_files]:
    cmd = ["helm", "lint", str(CHART_DIR)]
    if extra:
        cmd += ["-f", str(extra)]
    result = subprocess.run(cmd, capture_output=True, text=True)
    label = str(extra) if extra else "default values"
    if result.returncode != 0:
        fail(f"values schema rejects {label}:\n{result.stdout}{result.stderr}")
    else:
        ok(f"schema accepts {label}")

# 4. The README's generated table is current.
before = (CHART_DIR / "README.md").read_text()
regen = subprocess.run(
    ["helm-docs", "--chart-search-root", str(CHART_DIR), "--template-files", "README.md.gotmpl"],
    capture_output=True,
    text=True,
)
if regen.returncode != 0:
    fail(f"helm-docs failed:\n{regen.stdout}{regen.stderr}")
elif (CHART_DIR / "README.md").read_text() != before:
    (CHART_DIR / "README.md").write_text(before)
    fail("README.md is stale — run helm-docs and commit the result")
else:
    ok("README.md is up to date")

# 5. The route timeout of the auth proxy example against the WAL acknowledgement
#    deadline it must not undercut. icegate answers an OTLP request only once the
#    batch is durable, so a proxy that gives up first reports a failure for a
#    write that still commits, and the sender's retry writes the batch twice.
#    Nothing in a cluster compares the two: the proxy is handed a timeout icegate
#    never sees.
writer_deadline = re.search(r"WAL_ACK_TIMEOUT: Duration = Duration::from_secs\((\d+)\)", WAL_WRITER.read_text())
# Read per route rather than by the key alone: `timeout` also ends
# `connect_timeout`, and `remote_jwks.http_uri` carries a `timeout` of its own, so
# a search for the key would report a neighbour. A duration Envoy accepts and this
# pattern does not — `0.25s`, say — drops out of the list rather than ending the
# run, and the count check below then names the file that carries it.
example_timeouts = [
    int(match.group(1))
    for match in (re.fullmatch(r"(\d+)s", value) for value in read_envoy_route_timeouts(envoy_document).values())
    if match
]
if not writer_deadline or not example_timeouts:
    fail(
        f"the WAL acknowledgement deadline is no longer readable from "
        f"{'writer.rs' if not writer_deadline else EXAMPLE_CONFIGMAP}; update this check or the definition"
    )
elif len(example_timeouts) != 2:
    fail(f"{EXAMPLE_CONFIGMAP} carries {len(example_timeouts)} route timeouts, expected one per listener")
elif min(example_timeouts) < int(writer_deadline.group(1)):
    fail(
        f"{EXAMPLE_CONFIGMAP} times a route out after {min(example_timeouts)}s, "
        f"below the {writer_deadline.group(1)}s WAL acknowledgement deadline of writer.rs: "
        f"the sender retries a write that still commits, and the batch lands twice"
    )
else:
    ok(f"the auth proxy example outlasts the {writer_deadline.group(1)}s WAL acknowledgement deadline")


# 6. The three values of the example's jwt_authn provider that decide whether a
#    token is checked at all. An empty `issuer` is not a configuration Envoy
#    refuses — it is a provider that verifies no `iss`, so every token signed by a
#    key of the same JWKS and carrying the same audience is admitted. The same
#    holds for an empty audience list and for a JWKS uri fetching from nowhere.
def read_jwt_authn_providers(document: dict) -> list[tuple[str, dict]]:
    """Every `ingest_token` provider of the document, each with its listener name.

    Every listener is walked rather than the first alone: the two share one filter
    chain through a YAML anchor today, and a chain written out per listener is an
    ordinary edit that would leave a second provider nothing reads. A shared chain
    is reported once, under the listener that carries it first.
    """
    found: list[tuple[str, dict]] = []
    for listener in document.get("static_resources", {}).get("listeners", []):
        for chain in listener.get("filter_chains", []):
            for network_filter in chain.get("filters", []):
                manager = network_filter.get("typed_config", {})
                for http_filter in manager.get("http_filters", []):
                    if http_filter.get("name") != "envoy.filters.http.jwt_authn":
                        continue
                    provider = http_filter.get("typed_config", {}).get("providers", {}).get("ingest_token")
                    if provider is None or any(provider is seen for _, seen in found):
                        continue
                    found.append((listener.get("name", "<unnamed>"), provider))
    return found


jwt_authn_providers = read_jwt_authn_providers(envoy_document)
if not jwt_authn_providers:
    fail(f"{EXAMPLE_CONFIGMAP} carries no ingest_token provider; update this check or the example")
for listener_name, provider in jwt_authn_providers:
    stated = {
        "issuer": provider.get("issuer"),
        "remote_jwks.http_uri.uri": provider.get("remote_jwks", {}).get("http_uri", {}).get("uri"),
    }
    # Every audience, not the first: an empty one further down the list is
    # accepted by Envoy and admits a token carrying no audience of its own.
    # A provider with no list at all reports the same way, as `audiences[0]`.
    for position, audience in enumerate(provider.get("audiences") or [None]):
        stated[f"audiences[{position}]"] = audience
    empty = sorted(name for name, value in stated.items() if not (isinstance(value, str) and value.strip()))
    if empty:
        fail(
            f"{EXAMPLE_CONFIGMAP} leaves {', '.join(empty)} of the {listener_name} listener empty: "
            f"the provider then checks nothing that field stands for, and admits tokens it exists "
            f"to refuse"
        )
    else:
        ok(f"the {listener_name} provider names an issuer, its audiences and a JWKS uri")

sys.exit(report_failures())
