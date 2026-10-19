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
- The TLS wiring of the example, against the mounts of its own values. Envoy reads
  the SDS file, icegate's certificate and the CA at the paths the configuration
  names, and the validation below mounts each of them exactly there, so a path
  that names no mount of the sidecar loads clean here and leaves the pod without
  the file. Both listeners take icegate's Secret through the SDS file the
  ConfigMap ships, watching the directory the Secret is mounted at; both upstream
  clusters reach icegate over TLS, trusting the CA ConfigMap and verifying the
  one name they also send as SNI, and offering the one ALPN protocol their own codec
  names: `h2` for a cluster that sets `http2_protocol_options`, `http/1.1` for any
  other.

The certificates exist only in the cluster, so a throwaway CA and a server
certificate it signs, for the name the upstream clusters verify, are generated for
the validation: without a readable key the load fails on the mount, not on the
configuration under test.

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

# The file names inside the objects the sidecar mounts: the two keys of a
# kubernetes.io/tls Secret, the kind `ingest.tls.secretName` names, and the key
# values.yaml states `tls.caConfigMapName` holds its CA under.
SECRET_CERTIFICATE_KEY = "tls.crt"
SECRET_PRIVATE_KEY_KEY = "tls.key"
CA_CERTIFICATE_KEY = "ca.crt"

UPSTREAM_TLS_CONTEXT = "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext"

# The key under which a cluster states the codec it speaks to its upstream.
HTTP_PROTOCOL_OPTIONS = "envoy.extensions.upstreams.http.v3.HttpProtocolOptions"

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


def read_proxy_container(values: dict) -> dict:
    """The example's sidecar, or an empty mapping when the values carry none."""
    containers = values.get("ingest", {}).get("extraContainers", [])
    return next((container for container in containers if container.get("name") == PROXY_CONTAINER), {})


def read_example_image(values: dict) -> str | None:
    """The Envoy image the example's sidecar runs."""
    return read_proxy_container(values).get("image")


def read_proxy_mount_paths(values: dict) -> dict[tuple[str, str], str]:
    """Where the sidecar mounts each ConfigMap and Secret, keyed `("configMap" | "secret", name)`.

    Keyed by the object rather than by the volume: the configuration names a file of an
    object the values name elsewhere (`tls.caConfigMapName`, `ingest.tls.secretName`), and
    the volume carrying it is called whatever the values call it.
    """
    sources: dict[str, tuple[str, str]] = {}
    for volume in values.get("ingest", {}).get("extraVolumes", []):
        if "configMap" in volume:
            sources[volume["name"]] = ("configMap", volume["configMap"].get("name"))
        elif "secret" in volume:
            sources[volume["name"]] = ("secret", volume["secret"].get("secretName"))
    return {
        sources[mount["name"]]: mount["mountPath"]
        for mount in read_proxy_container(values).get("volumeMounts", [])
        if mount.get("name") in sources
    }


def read_example_mount_directory(
    mount_paths: dict, source: tuple[str, str], named_by: str, purpose: str
) -> str | None:
    """The directory the sidecar mounts `source` at, recording a failure when it mounts none."""
    directory = mount_paths.get(source)
    if not directory:
        kind, name = source
        fail(f"the {PROXY_CONTAINER} sidecar mounts no {kind} named by {named_by} ({name!r}): {purpose}")
    return directory


def find_named(envoy: dict, section: str, name: str) -> dict:
    """The listener or cluster of `section` called `name`, or an empty mapping."""
    entries = envoy.get("static_resources", {}).get(section, [])
    return next((entry for entry in entries if entry.get("name") == name), {})


def check_example_listener_certificates(
    envoy: dict, configmap: dict, config_dir: str | None, secret_dir: str | None
) -> None:
    """Both listeners serve icegate's Secret through the SDS file the ConfigMap ships.

    The SDS resource reads both files of the directory it watches, and that directory is
    the Secret's mount: it is where a renewed Secret appears, and the rotation case of
    scripts/authproxy-test.sh is what proves a renewal there reaches new connections. A
    directory the values do not mount has already failed the run, so only the comparisons
    that need it are skipped.
    """
    for label, _, listener_name, _ in SIGNALS:
        chains = find_named(envoy, "listeners", listener_name).get("filter_chains", [])
        for chain in chains or [{}]:
            context = chain.get("transport_socket", {}).get("typed_config", {}).get("common_tls_context", {})
            if context.get("tls_certificates"):
                fail(
                    f"the {label} listener names its certificate in tls_certificates rather than "
                    "through the SDS file the rotation case of authproxy-test.sh reloads"
                )
            references = context.get("tls_certificate_sds_secret_configs", [])
            if len(references) != 1:
                fail(f"the {label} listener takes {len(references)} certificates through SDS, expected one")
                continue
            sds_path = references[0].get("sds_config", {}).get("path_config_source", {}).get("path", "")
            sds_dir, _, sds_key = sds_path.rpartition("/")
            if sds_key not in configmap.get("data", {}) or (config_dir and sds_dir != config_dir):
                fail(
                    f"the {label} listener reads SDS from {sds_path!r}, which is no key of "
                    f"{EXAMPLE_CONFIGMAP.name} under its mount {config_dir}"
                )
                continue
            secret_name = references[0].get("name")
            resources = read_configmap_file(EXAMPLE_CONFIGMAP, sds_key).get("resources", [])
            secret = next((resource for resource in resources if resource.get("name") == secret_name), {})
            certificate = secret.get("tls_certificate", {})
            files = (
                certificate.get("certificate_chain", {}).get("filename"),
                certificate.get("private_key", {}).get("filename"),
            )
            watched_dir = certificate.get("watched_directory", {}).get("path")
            if not watched_dir or files != (
                f"{watched_dir}/{SECRET_CERTIFICATE_KEY}",
                f"{watched_dir}/{SECRET_PRIVATE_KEY_KEY}",
            ):
                fail(
                    f"the SDS secret {secret_name!r} of {sds_key} reads {files} and watches {watched_dir!r}; "
                    "it reads both files of the Secret out of the directory it watches"
                )
            elif secret_dir and watched_dir != secret_dir:
                fail(f"the SDS secret {secret_name!r} watches {watched_dir}, the Secret is mounted at {secret_dir}")
            else:
                ok(f"the {label} listener serves the Secret through {sds_path}, watching {watched_dir}")


def check_upstream_alpn(cluster: dict, tls: dict) -> None:
    """The cluster offers the protocol its own codec speaks, and nothing else.

    The codec is read from the cluster: one that sets `http2_protocol_options` speaks
    HTTP/2 and offers exactly `h2`, any other speaks HTTP/1.1. Neither protocol name is
    stated per cluster here, so a cluster that changes its codec without its ALPN, or the
    reverse, is the mismatch this reports.
    """
    name = cluster.get("name")
    options = cluster.get("typed_extension_protocol_options", {}).get(HTTP_PROTOCOL_OPTIONS, {})
    speaks_http2 = "http2_protocol_options" in options.get("explicit_http_config", {})
    expected = ["h2"] if speaks_http2 else ["http/1.1"]
    offered = tls.get("common_tls_context", {}).get("alpn_protocols")
    if offered != expected:
        fail(
            f"the {name} cluster speaks {'HTTP/2' if speaks_http2 else 'HTTP/1.1'} "
            f"and offers the ALPN protocols {offered}, expected {expected}"
        )
    else:
        ok(f"the {name} cluster offers {expected} for the codec it speaks")


def check_example_upstream_tls(envoy: dict, ca_dir: str | None) -> list[str]:
    """Both icegate clusters reach icegate over TLS, verifying its certificate by one name.

    Returns the names the clusters verify, which the throwaway certificate of the
    validation is issued for. A CA the values do not mount has already failed the run, so
    only the comparison against its mount is skipped.
    """
    verified_names = []
    for label, _, _, cluster_name in SIGNALS:
        cluster = find_named(envoy, "clusters", cluster_name)
        tls = cluster.get("transport_socket", {}).get("typed_config", {})
        if tls.get("@type") != UPSTREAM_TLS_CONTEXT:
            fail(f"the {cluster_name} cluster reaches icegate's {label} listener without an UpstreamTlsContext")
            continue
        check_upstream_alpn(cluster, tls)
        sni = tls.get("sni")
        validation = tls.get("common_tls_context", {}).get("validation_context", {})
        trusted_ca = validation.get("trusted_ca", {}).get("filename")
        san_names = [
            matcher.get("matcher", {}).get("exact")
            for matcher in validation.get("match_typed_subject_alt_names", [])
            if matcher.get("san_type") == "DNS"
        ]
        if not sni or san_names != [sni]:
            fail(
                f"the {cluster_name} cluster sends the SNI {sni!r} and verifies the DNS SANs {san_names}; "
                "both are the one name icegate's certificate carries"
            )
        elif not trusted_ca or (ca_dir and trusted_ca != f"{ca_dir}/{CA_CERTIFICATE_KEY}"):
            fail(
                f"the {cluster_name} cluster trusts {trusted_ca!r}, "
                f"the CA is mounted as {ca_dir}/{CA_CERTIFICATE_KEY}"
            )
        else:
            verified_names.append(sni)
            ok(f"the {cluster_name} cluster verifies icegate as {sni} against {trusted_ca}")
    return verified_names


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


def load_with_envoy(image: str, label: str, mounts: list[str], config_path: str) -> None:
    """Load one document with Envoy itself, which is the only reader that counts."""
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            f"{os.getuid()}:{os.getgid()}",
            "-e",
            "ENVOY_UID=0",
            *mounts,
            image,
            "/usr/local/bin/envoy",
            "--mode",
            "validate",
            "--config-path",
            config_path,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        fail(f"Envoy refused {label} configuration:\n{result.stdout}{result.stderr}")
    else:
        ok(f"Envoy loads {label} configuration")


def issue_server_certificate(directory: Path, names: list[str]) -> str | None:
    """Write a throwaway CA under `ca/` and a server certificate it signs for `names` under `tls/`.

    Each directory holds what the object mounted in its place holds: the CA ConfigMap's one
    file, the Secret's two. Returns openssl's complaint when a step fails, else `None`.
    """
    ca_dir, tls_dir = directory / "ca", directory / "tls"
    ca_dir.mkdir()
    tls_dir.mkdir()
    extensions = directory / "server.ext"
    extensions.write_text(f"subjectAltName={','.join(f'DNS:{name}' for name in dict.fromkeys(names))}\n")
    # `x509 -req` rather than `req -x509 -CA`, which OpenSSL added in 3.0: the two-step
    # form signs with the LibreSSL a macOS checkout finds first on its PATH as well.
    commands = [
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=validate-ca"]
        + ["-keyout", str(directory / "ca.key"), "-out", str(ca_dir / CA_CERTIFICATE_KEY)],
        ["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=validate"]
        + ["-keyout", str(tls_dir / SECRET_PRIVATE_KEY_KEY), "-out", str(directory / "server.csr")],
        ["openssl", "x509", "-req", "-days", "1", "-set_serial", "1", "-in", str(directory / "server.csr")]
        + ["-CA", str(ca_dir / CA_CERTIFICATE_KEY), "-CAkey", str(directory / "ca.key")]
        + ["-extfile", str(extensions), "-out", str(tls_dir / SECRET_CERTIFICATE_KEY)],
    ]
    for command in commands:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            return result.stderr
    return None


def validate_example_with_envoy(
    image: str, configmap: dict, mount_directories: tuple[str, str, str], verified_names: list[str]
) -> None:
    """Load the example with every file it names mounted where the pod mounts it.

    `mount_directories` is where the sidecar mounts the ConfigMap, icegate's Secret and the
    CA, in that order. The whole ConfigMap is mounted, not `envoy.yaml` alone, because the
    SDS file it names is another key of it.
    """
    config_dir, secret_dir, ca_dir = mount_directories
    with tempfile.TemporaryDirectory() as directory:
        example_dir = Path(directory)
        (example_dir / "envoy").mkdir()
        for key, text in configmap.get("data", {}).items():
            (example_dir / "envoy" / key).write_text(text)
        error = issue_server_certificate(example_dir, verified_names)
        if error is not None:
            fail(f"could not generate the throwaway certificates:\n{error}")
            return
        mounts = [
            "-v",
            f"{example_dir / 'envoy'}:{config_dir}:ro",
            "-v",
            f"{example_dir / 'tls'}:{secret_dir}:ro",
            "-v",
            f"{example_dir / 'ca'}:{ca_dir}:ro",
        ]
        load_with_envoy(image, "the example", mounts, f"{config_dir}/envoy.yaml")


example_values = load_document(EXAMPLE_VALUES)
example_configmap = load_document(EXAMPLE_CONFIGMAP)
example_envoy = read_configmap_file(EXAMPLE_CONFIGMAP, "envoy.yaml")
stand_envoy = load_document(STAND_ENVOY)

if not example_envoy:
    sys.exit(f"{EXAMPLE_CONFIGMAP} carried no envoy.yaml")

check_stand_loopback_ports(stand_envoy)
check_example_loopback_ports(example_values, example_envoy)
check_example_published_ports(example_values, example_envoy)
check_route_timeouts(example_envoy, stand_envoy)

example_mount_paths = read_proxy_mount_paths(example_values)
example_config_dir = read_example_mount_directory(
    example_mount_paths,
    ("configMap", example_configmap.get("metadata", {}).get("name")),
    f"the metadata.name of {EXAMPLE_CONFIGMAP.name}",
    "the Envoy configuration and the SDS file it reads",
)
example_secret_dir = read_example_mount_directory(
    example_mount_paths,
    ("secret", example_values.get("ingest", {}).get("tls", {}).get("secretName")),
    "ingest.tls.secretName",
    "the certificate icegate and the proxy both serve",
)
example_ca_dir = read_example_mount_directory(
    example_mount_paths,
    ("configMap", example_values.get("tls", {}).get("caConfigMapName")),
    "tls.caConfigMapName",
    "the CA the proxy verifies icegate's certificate against",
)
check_example_listener_certificates(example_envoy, example_configmap, example_config_dir, example_secret_dir)
example_verified_names = check_example_upstream_tls(example_envoy, example_ca_dir)

example_image = read_example_image(example_values)
stand_image = read_stand_image()
if not example_image:
    fail(f"{EXAMPLE_VALUES.name} names no {PROXY_CONTAINER} image")
elif example_image != stand_image:
    fail(f"{STAND_COMPOSE.name} runs {stand_image}, the example deploys {example_image}")
else:
    ok(f"both copies run {example_image}")
    # The example loads only once every file it names has a mount and a name to be issued
    # for; the checks above have already failed the run otherwise.
    if example_config_dir and example_secret_dir and example_ca_dir and example_verified_names:
        validate_example_with_envoy(
            example_image,
            example_configmap,
            (example_config_dir, example_secret_dir, example_ca_dir),
            example_verified_names,
        )
    load_with_envoy(
        stand_image, "the stand", ["-v", f"{Path.cwd() / STAND_ENVOY.parent}:/cfg:ro"], "/cfg/envoy.yaml"
    )

sys.exit(report_failures())
