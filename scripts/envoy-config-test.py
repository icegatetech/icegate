#!/usr/bin/env python3
"""Compare the two auth proxy copies against each other, then load both with Envoy.

A rendering mistake inside a proxy's `envoy.yaml` is invisible to the chart render
checks: they compare values, and Envoy is the only thing that knows whether the
document loads. This script pulls the Envoy image and runs containers, so it is
outside `make ci` and run by .github/workflows/deploy-config.yml. Run it after
touching the filter chain or either listener of the example or the stand.

Two copies exist and neither generates the other: the example the chart ships
alongside it (config/helm/auth-proxy) and the compose stand
(config/docker/auth-proxy). What is compared, and why each comparison exists:

- The loopback ports, within each copy. One file binds the receiver, its neighbour
  proxies to it, and a change to one of the two leaves a deployment that starts
  clean and refuses every OTLP request with a connection refused upstream.
- The published ports, for the example alone, because the Service is a resource
  only the chart has: `ingest.service.otlpHttpPort` / `otlpGrpcPort` name the port
  a sender addresses and the listeners accept it. The stand publishes the same two
  through the `ports` of its `ingest` service, which the proxy container joins by
  network namespace.
- The Envoy release, across the copies. Both are pinned by hand, and validating the
  stand's configuration with the example's image is what would make a drift between
  them invisible — so the tags are compared before either document is loaded.
- The route timeout, across the copies. Both repeat the value as a literal, and
  scripts/authproxy-test.sh runs either configuration on a value of its own, so
  nothing else looks at the numbers they ship. `helm-metadata-test.py` checks the
  example's against the WAL acknowledgement deadline it must not undercut; this
  one checks that the stand did not drift from it.

The example's listeners terminate TLS from a Secret that exists only in the
cluster, so a throwaway certificate is generated for the validation: without a
readable key the load fails on the mount, not on the configuration under test.

Both validations run as the invoking user, because the image's entrypoint drops to
its own `envoy` account (uid 101) and the generated directory, the private key
inside it and a checkout with a restrictive umask are all readable by their owner
alone. Under the image's account Envoy reports `Invalid path` for the mounted
document, which reads as a broken configuration and is a permission on the host.
`--user` alone does not settle it: the entrypoint drops the process when ENVOY_UID
is not 0 *and* the container started as uid 0, so a run by root keeps the starting
uid at 0 and is dropped to 101 anyway. ENVOY_UID=0 falsifies the first half
whatever uid the container starts as, and leaves a non-root `--user` untouched.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from chartlib import (  # noqa: E402  (path set above so this file runs from the repo root)
    fail,
    load_document,
    ok,
    read_configmap_file,
    read_configmap_text,
    read_envoy_cluster_port,
    read_envoy_listener_port,
    read_envoy_route_timeouts,
    report_failures,
)

STAND_INGEST = Path("config/docker/ingest-proxy.yaml")
STAND_ENVOY = Path("config/docker/auth-proxy/envoy.yaml")
STAND_COMPOSE = Path("config/docker/docker-compose.proxy.yml")
EXAMPLE_VALUES = Path("config/helm/auth-proxy/values-authproxy.yaml")
EXAMPLE_CONFIGMAP = Path("config/helm/auth-proxy/configmap-envoy.yaml")

PROXY_CONTAINER = "auth-proxy"

# The two OTLP signals as the four vocabularies name them: for a reader, for the
# chart values, for the Envoy listener, and for the cluster behind it.
SIGNALS = [
    ("OTLP HTTP", "otlpHttp", "otlp_http", "icegate_otlp_http"),
    ("OTLP gRPC", "otlpGrpc", "otlp_grpc", "icegate_otlp_grpc"),
]


def check_stand_loopback_ports(envoy: dict) -> None:
    """The stand's receiver binds against the ports its proxy sends to."""
    ingest = load_document(STAND_INGEST)
    for label, _, section, cluster in SIGNALS:
        bound = ingest.get(section, {}).get("port")
        proxied = read_envoy_cluster_port(envoy, cluster)
        if bound is None or proxied is None:
            fail(
                f"the stand configs name no {label} loopback port "
                f"({STAND_INGEST.name}: {bound}, {STAND_ENVOY}: {proxied})"
            )
        elif bound != proxied:
            fail(f"{STAND_INGEST.name} binds {label} on {bound}, {STAND_ENVOY} proxies to {proxied}")
        else:
            ok(f"the stand binds and proxies {label} on {bound}")


def check_example_loopback_ports(values: dict, envoy: dict) -> None:
    """The example's receiver binds against the ports its proxy sends to."""
    for label, values_key, _, cluster in SIGNALS:
        bound = values.get("ingest", {}).get(values_key, {}).get("port")
        proxied = read_envoy_cluster_port(envoy, cluster)
        if bound is None or proxied is None:
            fail(
                f"the example names no {label} loopback port "
                f"({EXAMPLE_VALUES.name}: {bound}, {EXAMPLE_CONFIGMAP.name}: {proxied})"
            )
        elif bound != proxied:
            fail(f"{EXAMPLE_VALUES.name} binds {label} on {bound}, {EXAMPLE_CONFIGMAP.name} proxies to {proxied}")
        else:
            ok(f"the example binds and proxies {label} on {bound}")


def check_example_published_ports(values: dict, envoy: dict) -> None:
    """What the example's Service publishes against what its proxy accepts on."""
    for label, values_key, listener, _ in SIGNALS:
        published = values.get("ingest", {}).get("service", {}).get(f"{values_key}Port")
        listened = read_envoy_listener_port(envoy, listener)
        if published is None or listened is None:
            fail(
                f"the example names no published {label} port "
                f"({EXAMPLE_VALUES.name}: {published}, {EXAMPLE_CONFIGMAP.name}: {listened})"
            )
        elif published != listened:
            fail(f"the Service publishes {label} on {published}, the proxy listens on {listened}")
        else:
            ok(f"the example publishes and listens {label} on {published}")


def read_example_image(values: dict) -> str | None:
    """The Envoy image the example's sidecar runs."""
    for container in values.get("ingest", {}).get("extraContainers", []):
        if container.get("name") == PROXY_CONTAINER:
            return container.get("image")
    return None


def read_stand_image() -> str | None:
    """The Envoy image the compose stand runs."""
    return load_document(STAND_COMPOSE).get("services", {}).get(PROXY_CONTAINER, {}).get("image")


def check_route_timeouts(example: dict, stand: dict) -> None:
    """Every route timeout of both copies, which must be one value."""
    timeouts_by_route: dict[str, str] = {}
    for label, document in (("example", example), ("stand", stand)):
        timeouts = read_envoy_route_timeouts(document)
        for _, _, _, cluster in SIGNALS:
            if cluster not in timeouts:
                fail(f"the {label} route to {cluster} carries no timeout")
            else:
                timeouts_by_route[f"{label} {cluster}"] = timeouts[cluster]

    distinct_timeouts = set(timeouts_by_route.values())
    if len(distinct_timeouts) > 1:
        fail(f"the routes time out after different values: {timeouts_by_route}")
    elif distinct_timeouts:
        ok(f"every route of both copies times out after {distinct_timeouts.pop()}")


def validate_with_envoy(image: str, example_envoy_text: str) -> None:
    """Load both documents with Envoy itself, which is the only reader that counts."""
    with tempfile.TemporaryDirectory() as directory:
        example_dir = Path(directory)
        (example_dir / "envoy.yaml").write_text(example_envoy_text)
        certificate = subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=validate",
                "-keyout",
                str(example_dir / "tls.key"),
                "-out",
                str(example_dir / "tls.crt"),
            ],
            capture_output=True,
            text=True,
        )
        if certificate.returncode != 0:
            fail(f"could not generate the throwaway certificate:\n{certificate.stderr}")
            return

        user = f"{os.getuid()}:{os.getgid()}"
        validations = [
            (
                "the example",
                [
                    "-v",
                    f"{example_dir}:/cfg:ro",
                    "-v",
                    f"{example_dir / 'tls.crt'}:/etc/icegate/tls/tls.crt:ro",
                    "-v",
                    f"{example_dir / 'tls.key'}:/etc/icegate/tls/tls.key:ro",
                ],
            ),
            ("the stand", ["-v", f"{Path.cwd() / STAND_ENVOY.parent}:/cfg:ro"]),
        ]
        for label, mounts in validations:
            result = subprocess.run(
                [
                    "docker",
                    "run",
                    "--rm",
                    "--user",
                    user,
                    "-e",
                    "ENVOY_UID=0",
                    *mounts,
                    image,
                    "/usr/local/bin/envoy",
                    "--mode",
                    "validate",
                    "--config-path",
                    "/cfg/envoy.yaml",
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                fail(f"Envoy refused {label} configuration:\n{result.stdout}{result.stderr}")
            else:
                ok(f"Envoy loads {label} configuration")


example_values = load_document(EXAMPLE_VALUES)
example_envoy = read_configmap_file(EXAMPLE_CONFIGMAP, "envoy.yaml")
stand_envoy = load_document(STAND_ENVOY)

if not example_envoy:
    sys.exit(f"{EXAMPLE_CONFIGMAP} carried no envoy.yaml")

check_stand_loopback_ports(stand_envoy)
check_example_loopback_ports(example_values, example_envoy)
check_example_published_ports(example_values, example_envoy)
check_route_timeouts(example_envoy, stand_envoy)

example_image = read_example_image(example_values)
stand_image = read_stand_image()
if not example_image:
    fail(f"{EXAMPLE_VALUES.name} names no {PROXY_CONTAINER} image")
elif example_image != stand_image:
    fail(f"{STAND_COMPOSE.name} runs {stand_image}, the example deploys {example_image}")
else:
    ok(f"both copies run {example_image}")
    validate_with_envoy(example_image, read_configmap_text(EXAMPLE_CONFIGMAP, "envoy.yaml"))

sys.exit(report_failures())
