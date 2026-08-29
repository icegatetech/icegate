#!/usr/bin/env python3
"""Assert the chart's Artifact Hub metadata still matches its sources.

Each artifact checked here is a copy of something else in the repo, so each
check names the source it must agree with. A failure means the copy drifted,
not that the source is wrong.
"""
import re
import subprocess
import sys
from pathlib import Path

import yaml

CHART_DIR = Path("config/helm/icegate")
CHART = CHART_DIR / "Chart.yaml"
BAKE = Path("config/docker/docker-bake.hcl")
RELEASE = Path(".github/workflows/release.yml")

failures: list[str] = []


def fail(msg: str) -> None:
    failures.append(msg)
    print(f"FAIL: {msg}", file=sys.stderr)


def ok(msg: str) -> None:
    print(f"ok: {msg}")


chart = yaml.safe_load(CHART.read_text())
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

sys.exit(1 if failures else 0)
