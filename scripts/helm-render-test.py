#!/usr/bin/env python3
"""Render the chart the ways a default render does not cover, and read the result.

The tenant policy, the OTLP bind addresses and the operational listener are the
three places where a rendering mistake fails open rather than loudly: a `single`
policy with no id rejects every request, a published OTLP port with nothing in
front of it is writable by any neighbour in the namespace, and a loopback
`ingest.metrics.host` answers the container alone, so both probes fail with nothing
to say beyond that. None of the three is visible in the default render, so each is
rendered here on purpose.

Both components carry the tenant policy and the operational listener the probes
address, so those two checks run per component off `COMPONENTS`. The rest is
ingest-only, because the sidecar split is: only ingest has listener hosts,
published-port keys, extraContainers and a NOTES port-forward command.

Every check states the message it expects a guard to fail with. That message is the
contract with the operator who hits the guard, so a guard that starts refusing for
another reason is a failure here even when it still refuses.
"""

import copy
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from chartlib import (  # noqa: E402  (path set above so this file runs from the repo root)
    capture_render_error,
    fail,
    iter_service_ports,
    load_documents,
    ok,
    read_component_config,
    read_container_port,
    read_container_port_names,
    read_service_port,
    read_service_port_entry,
    render_chart,
    render_notes,
    report_failures,
    restricted_violations,
)

EXAMPLE_VALUES = "config/helm/auth-proxy/values-authproxy.yaml"

# The components carrying a tenant policy and an operational listener of their own.
COMPONENTS = ("ingest", "query")

# The components that serve TLS from a mounted Secret. Wider than `COMPONENTS` because
# maintain has an operational listener of its own, but no tenant policy, and is off by
# default.
TLS_COMPONENTS = ("ingest", "query", "maintain")

# Where the chart mounts the Secret and which files of it the config names. Stated here
# rather than read from the chart: these paths are the contract between the Secret an
# operator provides and the config the pod loads, so a check that read them back from the
# render would pass whatever they were changed to.
TLS_MOUNT_PATH = "/etc/icegate-tls"
TLS_CERTIFICATE_PATH = f"{TLS_MOUNT_PATH}/tls.crt"
TLS_PRIVATE_KEY_PATH = f"{TLS_MOUNT_PATH}/tls.key"

# The CA the scrape is verified against, as the ConfigMap that holds it and the key inside.
TLS_CA_CONFIGMAP = "icegate-ca"
TLS_CA_KEY = "ca.crt"

# A sidecar declaring the two published OTLP port names, which is what a loopback
# receiver bind requires of ingest.extraContainers. Stated once because several
# cases below render such a bind, and the image is any image: the chart reads
# `ports[].name` of these entries and nothing else.
LOOPBACK_SIDECAR = [
    {
        "name": "auth-proxy",
        "image": "example/proxy:1",
        "ports": [
            {"name": "otlp-http", "containerPort": 4318},
            {"name": "otlp-grpc", "containerPort": 4317},
        ],
    }
]

# The two OTLP signals, as the four vocabularies that name them: the values key,
# the Kubernetes port name, the section of the rendered ingest.yaml, and the label
# NOTES.txt announces them under.
SIGNALS = [
    ("otlpHttp", "otlp-http", "otlp_http", "OTLP HTTP"),
    ("otlpGrpc", "otlp-grpc", "otlp_grpc", "OTLP gRPC"),
]

# The split a proxy sidecar creates, as `(published, bound)` per signal: the sender
# keeps addressing the published port while the receiver moves to the loopback port
# the proxy sends to. Stated once because two checks render this split and read both
# numbers back out of the render.
SPLIT_PORTS = {"otlpHttp": (4318, 14318), "otlpGrpc": (4317, 14317)}

# Every Service port that takes an `appProtocol`, as the values key under
# `<component>.service.appProtocol`, the name the Service renders the port under, and
# the template that renders it. A row per port rather than per Service because each
# call site of `icegate.appProtocol` names its own key, and a swapped pair is
# invisible until every key carries a value of its own.
APP_PROTOCOL_PORTS = (  # (component, values key, rendered port name, service template)
    ("ingest", "otlpGrpc", "otlp-grpc", "templates/service-ingest.yaml"),
    ("ingest", "otlpHttp", "otlp-http", "templates/service-ingest.yaml"),
    ("ingest", "metrics", "metrics", "templates/service-ingest.yaml"),
    ("query", "loki", "loki", "templates/service-query.yaml"),
    ("query", "prometheus", "prometheus", "templates/service-query.yaml"),
    ("query", "tempo", "tempo", "templates/service-query.yaml"),
    ("query", "flightSql", "flight-sql", "templates/service-query.yaml"),
    ("query", "metrics", "metrics", "templates/service-query.yaml"),
    ("maintain", "metrics", "metrics", "templates/service-maintain.yaml"),
    ("catalogServer", "http", "http", "templates/service-catalog.yaml"),
)

# The `--set` flags under which all four Services render every port of
# `APP_PROTOCOL_PORTS`: maintain and the catalog server are off by default, maintain
# refuses to render without a tracing endpoint, and the catalog server refuses to
# render on any backend but s3.
SERVICE_PORT_ARGS = (
    "--set",
    "maintain.enabled=true",
    "--set",
    "maintain.tracing.otlpEndpoint=http://otel:4317",
    "--set",
    "catalogServer.enabled=true",
    "--set",
    "catalog.backend=s3",
    "--set",
    "query.flightSql.enabled=true",
)

# Every values path that names an image, with the workload and container the render
# must show it on and the repository it must name. A workload is identified by its
# `app.kubernetes.io/component` label, which does not depend on the release name.
# `check_image_digests` gives each row a digest of its own and reads it back from the
# container the row names, so a call site reading a sibling's values, or two call
# sites swapped, leave a container carrying another row's digest. The helper-image
# pairs are separate rows because each component renders init containers of its own.
IMAGE_DIGEST_KEYS = (
    # (values path, component label, container name, repository)
    ("ingest.image", "ingest", "ingest", "ghcr.io/icegatetech/icegate-ingest"),
    ("query.image", "query", "query", "ghcr.io/icegatetech/icegate-query"),
    ("migrate.image", "migrate", "migrate", "ghcr.io/icegatetech/icegate-maintain"),
    ("maintain.image", "maintain", "maintain", "ghcr.io/icegatetech/icegate-maintain"),
    ("catalogServer.image", "catalog", "catalog", "ghcr.io/icegatetech/icegate-catalog"),
    ("ingest.waitForDependencies.image", "ingest", "wait-for-deps", "busybox"),
    ("ingest.waitForDependencies.awsCliImage", "ingest", "wait-for-buckets", "amazon/aws-cli"),
    ("query.waitForDependencies.image", "query", "wait-for-deps", "busybox"),
    ("query.waitForDependencies.awsCliImage", "query", "wait-for-buckets", "amazon/aws-cli"),
    ("migrate.waitForDependencies.image", "migrate", "wait-for-deps", "busybox"),
    ("migrate.waitForDependencies.awsCliImage", "migrate", "wait-for-buckets", "amazon/aws-cli"),
    ("maintain.waitForDependencies.image", "maintain", "wait-for-deps", "busybox"),
    ("maintain.waitForDependencies.awsCliImage", "maintain", "wait-for-buckets", "amazon/aws-cli"),
)

# One edit per rule of `restricted_violations`, applied to the pod spec of the default
# ingest render, which has a single container and no init container. Each must yield
# the violations listed, in order, one fragment per message, so a rule that stops being
# checked leaves the profile check below with nothing to prove. A pod-level value that
# breaks a rule is reported for the pod and for the one container that inherits it, hence
# two fragments; a `pop` edit leaves a setting unset, which the pod does not answer for.
RESTRICTED_VIOLATION_CASES = (  # (description, edit of the pod spec, expected message fragments)
    ("a pod on the host network", lambda pod: pod.update(hostNetwork=True), ("pod: hostNetwork",)),
    ("a pod in the host PID namespace", lambda pod: pod.update(hostPID=True), ("pod: hostPID",)),
    ("a pod in the host IPC namespace", lambda pod: pod.update(hostIPC=True), ("pod: hostIPC",)),
    (
        "a container that may escalate privileges",
        lambda pod: pod["containers"][0]["securityContext"].update(allowPrivilegeEscalation=True),
        ("allowPrivilegeEscalation",),
    ),
    (
        "a container that leaves allowPrivilegeEscalation unset",
        lambda pod: pod["containers"][0]["securityContext"].pop("allowPrivilegeEscalation"),
        ("allowPrivilegeEscalation",),
    ),
    (
        "a container dropping no capabilities",
        lambda pod: pod["containers"][0]["securityContext"]["capabilities"].update(drop=[]),
        ("capabilities.drop",),
    ),
    (
        "a container adding a capability",
        lambda pod: pod["containers"][0]["securityContext"]["capabilities"].update(add=["SYS_ADMIN"]),
        ("capabilities.add",),
    ),
    (
        "a privileged container",
        lambda pod: pod["containers"][0]["securityContext"].update(privileged=True),
        ("privileged",),
    ),
    (
        "a container opting out of runAsNonRoot",
        lambda pod: pod["containers"][0]["securityContext"].update(runAsNonRoot=False),
        ("container ingest: runAsNonRoot",),
    ),
    (
        "a container running as user 0",
        lambda pod: pod["containers"][0]["securityContext"].update(runAsUser=0),
        ("container ingest: runAsUser",),
    ),
    (
        "a pod opting out of runAsNonRoot, inherited by its container",
        lambda pod: pod["securityContext"].update(runAsNonRoot=False),
        ("pod: runAsNonRoot", "container ingest: runAsNonRoot"),
    ),
    (
        "a pod opting out of runAsNonRoot while its container states runAsNonRoot itself",
        lambda pod: (
            pod["securityContext"].update(runAsNonRoot=False),
            pod["containers"][0]["securityContext"].update(runAsNonRoot=True),
        ),
        ("pod: runAsNonRoot",),
    ),
    (
        "a pod leaving runAsNonRoot unset",
        lambda pod: pod["securityContext"].pop("runAsNonRoot"),
        ("container ingest: runAsNonRoot",),
    ),
    (
        "a pod running as user 0, inherited by its container",
        lambda pod: pod["securityContext"].update(runAsUser=0),
        ("pod: runAsUser", "container ingest: runAsUser"),
    ),
    (
        "a pod with an unconfined seccomp profile, inherited by its container",
        lambda pod: pod["securityContext"]["seccompProfile"].update(type="Unconfined"),
        ("pod: seccompProfile", "container ingest: seccompProfile"),
    ),
    (
        "a pod leaving the seccomp profile unset",
        lambda pod: pod["securityContext"].pop("seccompProfile"),
        ("container ingest: seccompProfile",),
    ),
    (
        "a container publishing a host port",
        lambda pod: pod["containers"][0]["ports"][0].update(hostPort=8080),
        ("hostPort",),
    ),
    (
        "a hostPath volume",
        lambda pod: pod["volumes"].append({"name": "host", "hostPath": {"path": "/"}}),
        ("volume host: type hostPath",),
    ),
    (
        "an init container with no securityContext",
        lambda pod: pod.update(initContainers=[{"name": "probe", "image": "x:1"}]),
        ("init container probe: allowPrivilegeEscalation", "init container probe: capabilities.drop"),
    ),
)

# `ingest.extraContainers` and `ingest.extraVolumes` entries that
# `icegate.validateRestrictedExtensions` must refuse once `podSecurity.checkRestricted`
# is on, each breaking one rule and no other, with the message that names the entry.
# Rendered again with the guard off, where every one of them must be admitted: the
# guard is opt-in.
RESTRICTED_GUARD_CASES = (  # (description, --set-json args, expected message fragment)
    (
        "a sidecar allowing privilege escalation",
        'ingest.extraContainers=[{"name":"side","image":"x:1","securityContext":{"capabilities":{"drop":["ALL"]}}}]',
        "ingest.extraContainers[side]: allowPrivilegeEscalation",
    ),
    (
        "a sidecar keeping its capabilities",
        'ingest.extraContainers=[{"name":"side","image":"x:1","securityContext":{"allowPrivilegeEscalation":false}}]',
        "ingest.extraContainers[side]: capabilities.drop",
    ),
    (
        "a sidecar running as root",
        'ingest.extraContainers=[{"name":"side","image":"x:1","securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"runAsUser":0}}]',
        "ingest.extraContainers[side]: runAsUser",
    ),
    (
        "a privileged sidecar",
        'ingest.extraContainers=[{"name":"side","image":"x:1","securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"privileged":true}}]',
        "ingest.extraContainers[side]: privileged",
    ),
    (
        "a hostPath volume",
        'ingest.extraVolumes=[{"name":"host","hostPath":{"path":"/"}}]',
        "ingest.extraVolumes[host]: volume type hostPath",
    ),
    (
        "a sidecar opting out of runAsNonRoot",
        'ingest.extraContainers=[{"name":"side","image":"x:1","securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"runAsNonRoot":false}}]',
        "ingest.extraContainers[side]: runAsNonRoot",
    ),
    (
        "a sidecar with an unconfined seccomp profile",
        'ingest.extraContainers=[{"name":"side","image":"x:1","securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"seccompProfile":{"type":"Unconfined"}}}]',
        "ingest.extraContainers[side]: seccompProfile.type",
    ),
    (
        "a sidecar adding a capability",
        'ingest.extraContainers=[{"name":"side","image":"x:1","securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"],"add":["SYS_ADMIN"]}}}]',
        "ingest.extraContainers[side]: capabilities.add",
    ),
    (
        "a sidecar publishing a host port",
        'ingest.extraContainers=[{"name":"side","image":"x:1","ports":[{"containerPort":8080,"hostPort":8080}],"securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]}}}]',
        "ingest.extraContainers[side]: hostPort",
    ),
)

# A sidecar the guard must admit and `restricted_violations` must find nothing in. It adds
# the one capability the level allows and states its own `runAsNonRoot`, a non-zero
# `runAsUser` and seccomp profile, so it is judged by what it states rather than by the pod.
RESTRICTED_ACCEPTED_SIDECAR = 'ingest.extraContainers=[{"name":"side","image":"x:1","securityContext":{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"],"add":["NET_BIND_SERVICE"]},"runAsNonRoot":true,"runAsUser":10001,"seccompProfile":{"type":"RuntimeDefault"}}}]'


# The job-storage blocks of the rendered configs: (config file, section holding the block's
# `jobsmanager.storage`, values path of that same block).
JOB_STORAGE_BLOCKS = (
    ("ingest.yaml", "shift", "ingest.shift.jobsmanager.storage"),
    ("maintain.yaml", "compaction", "maintain.compaction.jobsmanager.storage"),
    ("maintain.yaml", "gc", "maintain.gc.jobsmanager.storage"),
    ("maintain.yaml", "wal_cleanup", "maintain.walCleanup.jobsmanager.storage"),
    ("maintain.yaml", "pricing", "maintain.pricing.jobsmanager.storage"),
)


def digest_for_row(position: int) -> str:
    """A well-formed digest that no other row of `IMAGE_DIGEST_KEYS` shares.

    `position` counts from 1, the rows' order. The digest stands in for one an operator
    copies from a registry; no image is pulled by it, the render only prints it.
    """
    return f"sha256:{position:064x}"


def all_workloads_args() -> tuple[str, ...]:
    """The `--set` flags that render every workload and every init container.

    Each row of `IMAGE_DIGEST_KEYS` then has a container to read, and a check of every
    pod has all of them to read. maintain needs a tracing endpoint to render at all, the
    catalog server needs the s3 backend, and a bucket gate needs the S3 endpoint it polls.
    """
    settings = [
        "maintain.enabled=true",
        "maintain.tracing.otlpEndpoint=http://otel:4317",
        "catalogServer.enabled=true",
        "catalog.backend=s3",
        "storage.s3.endpoint=http://s3:9000",
    ]
    for component in ("ingest", "query", "migrate", "maintain"):
        settings += [
            f"{component}.waitForDependencies.enabled=true",
            f"{component}.waitForDependencies.endpoints={{http://dependency/health}}",
            f"{component}.waitForDependencies.buckets={{warehouse}}",
        ]
    return tuple(flag for setting in settings for flag in ("--set", setting))


def split_port_args() -> tuple[str, ...]:
    """The `--set` flags that render the split stated in `SPLIT_PORTS`."""
    args: list[str] = []
    for values_key, (published, bound) in SPLIT_PORTS.items():
        args += ["--set", f"ingest.{values_key}.port={bound}"]
        args += ["--set", f"ingest.service.{values_key}Port={published}"]
    return tuple(args)


def check_render_refuses(description: str, expected: str, *args: str) -> None:
    """Assert the render fails, and fails with the message that names the key."""
    error = capture_render_error(*args)
    if error is None:
        fail(f"expected {description} to fail rendering")
    elif expected not in error:
        fail(f"{description} failed the render for another reason: {error}")
    else:
        ok(f"{description} is refused, naming {expected!r}")


def check_tenant_policy(component: str) -> None:
    """The tagged union the pod loads, and the four ways of asking for a broken one."""
    check_render_refuses(
        f"{component}.tenant.mode=single without tenant.id",
        f"{component}.tenant.id is required",
        "--set",
        f"{component}.tenant.mode=single",
        "--set",
        f"{component}.tenant.id=",
    )
    # An unknown mode is caught by the values schema, which names the path it
    # rejected rather than the chart's own message.
    check_render_refuses(
        f"an unknown {component}.tenant.mode",
        f"/{component}/tenant/mode",
        "--set",
        f"{component}.tenant.mode=bogus",
    )
    # A misspelt key would otherwise render silently and leave the component on
    # the chart's default policy. The quoted path is the object itself, not a key
    # under it, so the refusal is for the unknown key and not for `mode`.
    check_render_refuses(
        f"an unknown key under {component}.tenant",
        f"'/{component}/tenant': additional properties 'Mode' not allowed",
        "--set",
        f"{component}.tenant.Mode=multi",
    )
    check_render_refuses(
        f"an absent {component}.tenant.mode",
        f"{component}.tenant.mode must be single or multi",
        "--set",
        f"{component}.tenant.mode=null",
    )

    template = f"templates/configmap-{component}.yaml"
    if "tenant: !multi" not in render_chart("--set", f"{component}.tenant.mode=multi", show_only=template):
        fail(f"{component}.tenant.mode=multi must render the !multi tag")
    else:
        ok(f"{component}.tenant.mode=multi renders the !multi tag")

    rendered = render_chart(show_only=template)
    if "tenant: !single" not in rendered:
        fail(f"the default {component} render must carry the !single tag")
    elif 'id: "default"' not in rendered:
        fail(f"the default {component} render must name the chart's tenant id")
    else:
        ok(f"the default {component} render carries !single and the chart's tenant id")

    named = render_chart("--set", f"{component}.tenant.id=acme", show_only=template)
    if 'id: "acme"' not in named:
        fail(f"{component}.tenant.id must reach the rendered tenant section")
    else:
        ok(f"{component}.tenant.id reaches the rendered tenant section")


def check_probe_target(component: str) -> None:
    """The port the probes address, its number, and that disabling metrics does not take it away.

    The operational listener carries `/health` whatever `metrics.enabled` says, so
    the port has to stay declared: a probe naming a port no container declares is
    a restart loop on a pod that serves correctly.
    """
    rendered = render_chart(
        "--set", f"{component}.metrics.enabled=false", show_only=f"templates/deployment-{component}.yaml"
    )
    if "metrics" not in read_container_port_names(rendered):
        fail(
            f"the {component} metrics port must be declared even with "
            f"{component}.metrics.enabled=false: the probes address it by name"
        )
    elif rendered.count("port: metrics") != 2:
        fail(f"both {component} probes must still address the metrics port by name")
    elif rendered.count("path: /health") != 2:
        fail(f"both {component} probes must address /health on the operational listener")
    else:
        ok(f"{component}.metrics.enabled=false keeps the port declared and both probes on /health")

    # The probes address the port by name, so they reach the operational listener
    # only while the number declared under that name is the one the component
    # binds. A non-default number, because a literal in either template renders the
    # default exactly like the value does.
    port = 19091
    port_args = ("--set", f"{component}.metrics.port={port}")
    declared = read_container_port(
        render_chart(*port_args, show_only=f"templates/deployment-{component}.yaml"), "metrics"
    )
    bound = (
        read_component_config(
            render_chart(*port_args, show_only=f"templates/configmap-{component}.yaml"), f"{component}.yaml"
        )
        .get("metrics", {})
        .get("port")
    )
    if declared != port:
        fail(f"{component}.metrics.port must reach the metrics containerPort: expected {port}, got {declared}")
    elif bound != port:
        fail(f"{component}.metrics.port must reach metrics.port of {component}.yaml: expected {port}, got {bound}")
    else:
        ok(f"{component}.metrics.port reaches both the metrics containerPort and {component}.yaml")


def check_operational_host() -> None:
    """The ingest binds that would leave the probes blind.

    Ingest-only: `query.metrics.host` is not a value, so the query operational
    listener cannot be moved anywhere and has no guard to exercise.
    """
    # Every spelling the guard names, the bracketed IPv6 loopback included: that is
    # the form run_operational_server parses out of `{host}:{port}`, so a guard
    # blind to it would refuse the roundabout spellings alone.
    for host in ("127.0.0.1", "localhost", "::1", "[::1]"):
        check_render_refuses(
            f"the loopback ingest.metrics.host {host}",
            "is a loopback address",
            "--set",
            f"ingest.metrics.host={host}",
        )

    # A `null` in an overlay removes the key rather than setting one, and an empty
    # string renders a listener the ingest pod refuses on load. Both are refused by
    # a message that names the key: read straight, the guard would abort on a type
    # error addressing a line of _helpers.tpl instead.
    for host in ("null", ""):
        check_render_refuses(
            f"ingest.metrics.host={host!r}",
            "ingest.metrics.host is required",
            "--set",
            f"ingest.metrics.host={host}",
        )


def check_otlp_port_names() -> None:
    """Who declares the published OTLP port names, on each of the two binds."""
    # Port names are unique within a pod, and the Service addresses them by name. A
    # second declaration renders and lints clean; the API server refuses it as a
    # Duplicate value, so the release does not roll out. Counted on the deployment
    # alone, because the Service declares the same two names.
    declared = read_container_port_names(render_chart(show_only="templates/deployment-ingest.yaml"))
    for _, port_name, _, _ in SIGNALS:
        count = declared.count(port_name)
        if count != 1:
            fail(
                f"on the default binds icegate publishes the OTLP ports itself "
                f"and declares {port_name} {count} times instead of once"
            )
        else:
            ok(f"the default bind declares {port_name} exactly once")

    # A loopback OTLP bind publishes nothing: the containerPort would name an
    # address the Service cannot reach, and the port name belongs to whichever
    # container actually listens on it. The sidecar the bind requires declares both
    # names, so each is present once and neither is icegate's own.
    loopback_args = (
        "--set",
        "ingest.otlpHttp.host=127.0.0.1",
        "--set",
        "ingest.otlpGrpc.host=127.0.0.1",
        "--set-json",
        f"ingest.extraContainers={json.dumps(LOOPBACK_SIDECAR)}",
    )
    names = read_container_port_names(render_chart(*loopback_args, show_only="templates/deployment-ingest.yaml"))
    for _, port_name, _, _ in SIGNALS:
        if names.count(port_name) != 1:
            fail(
                f"a loopback bind must leave {port_name} to the sidecar alone; "
                f"the render declares it {names.count(port_name)} times"
            )
        else:
            ok(f"a loopback bind leaves {port_name} to the sidecar alone")

    # And the config the pod loads carries that host, so the two cannot disagree.
    config = read_component_config(render_chart(*loopback_args, show_only="templates/configmap-ingest.yaml"), "ingest.yaml")
    for values_key, _, section, _ in SIGNALS:
        if config.get(section, {}).get("host") != "127.0.0.1":
            fail(f"ingest.{values_key}.host must reach the rendered {section} section")
        else:
            ok(f"ingest.{values_key}.host reaches the rendered {section} section")


def check_otlp_host_guards() -> None:
    """The loopback bind nothing declares, and the OTLP hosts that are not there."""
    # A loopback bind with nothing declaring that port name is the one way this pair
    # fails silently: the pod is Ready, the Service names a targetPort no container
    # declares, its EndpointSlice carries none, and a sender is refused the
    # connection with nothing in any log to name the cause.
    for values_key, _, _, _ in SIGNALS:
        check_render_refuses(
            f"a loopback ingest.{values_key}.host without a container declaring its port",
            "ingest.extraContainers",
            "--set",
            f"ingest.{values_key}.host=127.0.0.1",
        )

    # And the example that does declare them renders, so the guard admits the
    # deployment it exists for rather than only refusing the mistake. Checked
    # through `capture_render_error` rather than `render_chart`, which would end
    # the run on a broken example instead of recording it beside the guards it
    # belongs with.
    error = capture_render_error("-f", EXAMPLE_VALUES)
    if error is not None:
        fail(f"{EXAMPLE_VALUES} must render, the guard exists for it:\n{error}")
    else:
        ok(f"{EXAMPLE_VALUES} renders")

    # Neither OTLP host may be absent or empty, for the two reasons
    # ingest.metrics.host is checked for above. Unlike that one, a loopback value is
    # legitimate here, so only presence is checked.
    for values_key, _, _, _ in SIGNALS:
        for host in ("null", ""):
            check_render_refuses(
                f"ingest.{values_key}.host={host!r}",
                f"ingest.{values_key}.host is required",
                "--set",
                f"ingest.{values_key}.host={host}",
            )


def check_service_ports() -> None:
    """What the Service publishes against what the receiver binds, on both settings."""
    # Left unset, the published port follows the receiver port: the two are one
    # number while icegate is the published listener itself. Both are read out of
    # the render, so this case does not become a second copy of the ports
    # values.yaml ships.
    service = render_chart(show_only="templates/service-ingest.yaml")
    config = read_component_config(render_chart(show_only="templates/configmap-ingest.yaml"), "ingest.yaml")
    for values_key, port_name, section, _ in SIGNALS:
        published = read_service_port(service, port_name)
        bound = config.get(section, {}).get("port")
        if published is None or bound is None:
            fail(f"the default render names no {values_key} port (Service: {published}, ConfigMap: {bound})")
        elif published != bound:
            fail(
                f"with ingest.service.{values_key}Port unset the Service must publish "
                f"the receiver port {bound}, it publishes {published}"
            )
        else:
            ok(f"unset, ingest.service.{values_key}Port follows the receiver port {bound}")

    # And a stated one publishes what senders address while the receiver stays where
    # the sidecar proxies to it — the whole point of the pair being two keys.
    split_args = split_port_args()
    service = render_chart(*split_args, show_only="templates/service-ingest.yaml")
    config = read_component_config(render_chart(*split_args, show_only="templates/configmap-ingest.yaml"), "ingest.yaml")
    for values_key, port_name, section, _ in SIGNALS:
        want_published, want_bound = SPLIT_PORTS[values_key]
        published = read_service_port(service, port_name)
        bound = config.get(section, {}).get("port")
        if published != want_published:
            fail(f"ingest.service.{values_key}Port must reach the Service: expected {want_published}, got {published}")
        elif bound != want_bound:
            fail(
                f"ingest.service.{values_key}Port must leave the receiver port alone: "
                f"expected {want_bound}, got {bound}"
            )
        else:
            ok(f"ingest.service.{values_key}Port publishes {want_published} while the receiver stays on {want_bound}")


def check_notes() -> None:
    """The port NOTES.txt tells an operator to address.

    Its port-forward command names a port of the Service, so built from the
    receiver port it would name one that exists only inside the pod — and it is the
    first text an operator of the sidecar example reads. `helm template` renders no
    NOTES, hence the client-side dry run.
    """
    notes = render_notes(*split_port_args())
    if not notes.strip():
        fail("the dry run printed no NOTES to check")
        return

    for values_key, _, _, label in SIGNALS:
        published, bound = SPLIT_PORTS[values_key]
        if f"{label} (port {published})" not in notes:
            fail(f"NOTES must announce {label} on the published port {published}")
        elif f"ingest {published}:{published}" not in notes:
            fail(f"the NOTES port-forward command for {label} must address the Service on {published}")
        elif str(bound) in notes:
            fail(f"NOTES names the {label} receiver port {bound}, which no Service publishes")
        else:
            ok(f"NOTES announces {label} on the published port {published} alone")


def check_pod_extensions() -> None:
    """extraContainers and extraVolumes reach the pod as written.

    The chart neither reads nor validates what an operator puts there, beyond the
    `ports[].name` a loopback bind requires, so this is a passthrough check.
    """
    rendered = render_chart(
        "--set-json",
        'ingest.extraContainers=[{"name":"sidecar","image":"example/image:1"}]',
        "--set-json",
        'ingest.extraVolumes=[{"name":"extra","emptyDir":{}}]',
        show_only="templates/deployment-ingest.yaml",
    )
    for line in ("name: sidecar", "image: example/image:1", "name: extra"):
        if line not in rendered:
            fail(f"ingest.extraContainers and ingest.extraVolumes must reach the pod as written; no {line!r}")
            return
    ok("ingest.extraContainers and ingest.extraVolumes reach the pod as written")


def read_job_storage(manifests: str, config_file: str, section: str) -> dict | None:
    """The `jobsmanager.storage` block of one section of a rendered config, if it has one."""
    jobsmanager = (read_component_config(manifests, config_file).get(section) or {}).get("jobsmanager") or {}
    return jobsmanager.get("storage")


def check_jobs_storage_endpoint() -> None:
    """A job-storage block names an endpoint only when one is configured.

    The services read an absent endpoint as "the regional AWS endpoint". The chart used
    to spell the unset case as `endpoint: ""`, which is a value to be told apart from
    "not configured"; with nothing configured the key has to be left out of every block,
    and with an endpoint configured it has to reach every block: the shared
    `storage.s3.endpoint`, or the block's own, which wins.
    """
    maintain_args = ("--set", "maintain.enabled=true", "--set", "maintain.tracing.otlpEndpoint=http://otel:4317")
    shared_endpoint = "http://s3:9000"
    own_endpoint = "http://own-s3:9000"

    for description, args, want in (
        ("no endpoint configured", (), None),
        ("storage.s3.endpoint configured", ("--set", f"storage.s3.endpoint={shared_endpoint}"), shared_endpoint),
    ):
        manifests = render_chart(*maintain_args, *args)
        for config_file, section, _ in JOB_STORAGE_BLOCKS:
            label = f"{config_file} {section}.jobsmanager.storage with {description}"
            storage = read_job_storage(manifests, config_file, section)
            if storage is None:
                fail(f"{label}: the render carries no such block to check")
            elif want is None and "endpoint" in storage:
                fail(f"{label} must carry no endpoint key, but renders {storage['endpoint']!r}")
            elif want is not None and storage.get("endpoint") != want:
                fail(f"{label} must carry endpoint {want!r}, but renders {storage.get('endpoint')!r}")
            else:
                ok(f"{label} renders {'no endpoint' if want is None else f'endpoint {want}'}")

    for config_file, section, values_key in JOB_STORAGE_BLOCKS:
        label = f"{values_key}.endpoint over storage.s3.endpoint"
        manifests = render_chart(
            *maintain_args,
            "--set",
            f"storage.s3.endpoint={shared_endpoint}",
            "--set",
            f"{values_key}.endpoint={own_endpoint}",
        )
        storage = read_job_storage(manifests, config_file, section)
        got = None if storage is None else storage.get("endpoint")
        if got != own_endpoint:
            fail(f"{label} must win in {config_file} {section}.jobsmanager.storage, but it renders {got!r}")
        else:
            ok(f"{label} wins in {config_file} {section}.jobsmanager.storage")


def read_workload_images(*args: str) -> dict[tuple[str, str], str]:
    """The image of every container and init container of a render, by where it runs.

    Keyed by the component label of the Deployment or Job and the container's name, so
    a caller can say which container an image belongs to and not only that the render
    contains it.
    """
    images: dict[tuple[str, str], str] = {}
    for document in load_documents(render_chart(*args)):
        if document.get("kind") not in ("Deployment", "Job"):
            continue
        component = document["metadata"]["labels"]["app.kubernetes.io/component"]
        pod = document["spec"]["template"]["spec"]
        for container in [*pod.get("initContainers", []), *pod["containers"]]:
            images[(component, container["name"])] = container["image"]
    return images


def check_image_digests() -> None:
    """Each image key's own digest reaches the container it names, and none is pinned unasked.

    Unset, the digest must leave the reference at `repository:tag`: pinning is the
    operator's choice, so a render that pins unasked changes what every existing
    install references. Set, every key carries a different digest and the container
    each key belongs to must carry exactly that key's pinned image, which is what proves
    every call site reads its own values and not a sibling's.
    """
    args = all_workloads_args()
    unpinned = read_workload_images(*args)
    pinned_unasked = [image for image in unpinned.values() if "@sha256:" in image]
    if len(unpinned) != len(IMAGE_DIGEST_KEYS):
        fail(f"expected {len(IMAGE_DIGEST_KEYS)} rendered images, one per key, got {len(unpinned)}: {unpinned}")
        return
    if pinned_unasked:
        fail(f"an image is pinned with no digest set: {pinned_unasked}")
        return
    ok(f"with no digest set, all {len(unpinned)} rendered images are referenced by tag alone")

    # The tag each image renders with is read from the unpinned render rather than stated
    # here: the component images follow the chart's appVersion.
    expected = []
    for position, (key, component, container, repository) in enumerate(IMAGE_DIGEST_KEYS, start=1):
        reference = unpinned.get((component, container))
        if reference is None or reference.rsplit(":", 1)[0] != repository:
            fail(f"{key} must render {repository} on {container} of {component}, got {reference}")
            continue
        expected.append((key, component, container, f"{reference}@{digest_for_row(position)}"))
    if len(expected) != len(IMAGE_DIGEST_KEYS):
        return

    digest_args = [
        flag
        for position, (key, *_) in enumerate(IMAGE_DIGEST_KEYS, start=1)
        for flag in ("--set", f"{key}.digest={digest_for_row(position)}")
    ]
    rendered = read_workload_images(*args, *digest_args)
    misplaced = [
        f"{key} must reach {container} of {component} as {want}, got {rendered.get((component, container))}"
        for key, component, container, want in expected
        if rendered.get((component, container)) != want
    ]
    if misplaced:
        for message in misplaced:
            fail(message)
    else:
        ok(f"each of the {len(expected)} image keys renders its own digest on the container it names")


def check_digest_format_refused() -> None:
    """A digest of the wrong shape is refused at render time, naming the key that holds it."""
    check_render_refuses(
        "a digest that is not sha256 plus 64 hex characters",
        "ingest.image.digest",
        "--set",
        "ingest.image.digest=latest",
    )


def check_helper_image_string_refused() -> None:
    """A helper image given as a string, which is what the chart took before it had a digest key.

    Indexing a string as a map fails inside the template with a message about a line
    of it, not about the values key an operator has to change, so the chart names the
    key and the shape instead.
    """
    check_render_refuses(
        "a waitForDependencies image given as a string",
        "ingest.waitForDependencies.image must be a map with repository, tag and digest",
        "--set",
        "ingest.waitForDependencies.enabled=true",
        "--set-json",
        'ingest.waitForDependencies.endpoints=["http://x/health"]',
        "--set",
        "ingest.waitForDependencies.image=busybox:1.37.0",
    )


def check_app_protocol() -> None:
    """`appProtocol` is absent unless set, and a value reaches its own port and no other."""
    # Left unset, no Service port carries the field: it names the protocol a port
    # speaks to whatever reads Services, and the chart picks none.
    templates = tuple(dict.fromkeys(template for *_, template in APP_PROTOCOL_PORTS))
    for template in templates:
        ports = list(iter_service_ports(render_chart(*SERVICE_PORT_ARGS, show_only=template)))
        pinned = [port.get("name") for port in ports if "appProtocol" in port]
        if not ports:
            fail(f"{template} renders no Service port, so its appProtocol default is not covered")
        elif pinned:
            fail(f"with no appProtocol set, {template} sets one on {pinned}")
        else:
            ok(f"with no appProtocol set, {template} sets none ({len(ports)} port entries read)")

    # Each key is given a value of its own, then read back from the one port it names.
    # Every other port of the same Service must stay bare: a call site reading a
    # sibling's key would put the value on the wrong port, which a single shared value
    # could not show.
    for component, values_key, port_name, template in APP_PROTOCOL_PORTS:
        value = f"example.com/proto-{values_key}"
        rendered = render_chart(
            *SERVICE_PORT_ARGS,
            "--set",
            f"{component}.service.appProtocol.{values_key}={value}",
            show_only=template,
        )
        entry = read_service_port_entry(rendered, port_name)
        strays = [
            port.get("name")
            for port in iter_service_ports(rendered)
            if port.get("name") != port_name and "appProtocol" in port
        ]
        if entry is None:
            fail(f"{template} renders no port named {port_name}")
        elif entry.get("appProtocol") != value:
            fail(
                f"{component}.service.appProtocol.{values_key} must reach port {port_name}: "
                f"expected {value}, got {entry.get('appProtocol')}"
            )
        elif strays:
            fail(f"{component}.service.appProtocol.{values_key} also reached {strays}")
        else:
            ok(f"{component}.service.appProtocol.{values_key} reaches port {port_name} alone")


def check_app_protocol_disabled_port() -> None:
    """A value for a port whose listener is disabled renders, and adds no port."""
    rendered = render_chart(
        "--set",
        "query.flightSql.enabled=false",
        "--set",
        "query.service.appProtocol.flightSql=example.com/proto-flightSql",
        show_only="templates/service-query.yaml",
    )
    if read_service_port_entry(rendered, "flight-sql") is not None:
        fail("query.flightSql.enabled=false must remove the flight-sql port whatever its appProtocol says")
    else:
        ok("an appProtocol for a disabled flight-sql listener adds no port")


def check_app_protocol_null_map() -> None:
    """A `null` appProtocol map, which an overlay uses to remove the key, renders no field."""
    services = dict.fromkeys((component, template) for component, _, _, template in APP_PROTOCOL_PORTS)
    for component, template in services:
        null_args = ("--set", f"{component}.service.appProtocol=null")
        rendered = render_chart(*SERVICE_PORT_ARGS, *null_args, show_only=template)
        pinned = [port.get("name") for port in iter_service_ports(rendered) if "appProtocol" in port]
        if pinned:
            fail(f"a null {component}.service.appProtocol must set no field, {template} sets one on {pinned}")
        else:
            ok(f"a null {component}.service.appProtocol sets no field")


def read_pod_spec(component: str, *args: str) -> dict:
    """The pod spec of the Deployment of `component` that a render with `args` produces."""
    deployment = load_documents(render_chart(*args, show_only=f"templates/deployment-{component}.yaml"))[0]
    return deployment["spec"]["template"]["spec"]


def read_ingest_pod_spec(*args: str) -> dict:
    """The pod spec of the ingest Deployment a render with `args` produces."""
    return read_pod_spec("ingest", *args)


def read_extension_label(set_json: str) -> str:
    """The label `restricted_violations` gives the one entry a guard case adds to the pod.

    Read out of the case's own `--set-json` value, so the cases stay the strings they are
    and this check cannot name a different entry than the one rendered.
    """
    key, _, value = set_json.partition("=")
    kind = "container" if key == "ingest.extraContainers" else "volume"
    return f"{kind} {json.loads(value)[0]['name']}"


def check_restricted_violations() -> None:
    """`restricted_violations` admits the default ingest pod and names each rule it breaks.

    The profile check below is only as strong as this function, so each rule is shown to
    fire here, on one edit of a pod spec the chart really renders.
    """
    base = read_ingest_pod_spec()
    unexpected = restricted_violations(base)
    if unexpected:
        fail(f"the default ingest pod must meet the restricted level, restricted_violations reports {unexpected}")
    else:
        ok("restricted_violations reports nothing for the default ingest pod")

    for description, edit, expected in RESTRICTED_VIOLATION_CASES:
        pod_spec = copy.deepcopy(base)
        edit(pod_spec)
        violations = restricted_violations(pod_spec)
        if len(violations) != len(expected) or not all(
            fragment in violation for fragment, violation in zip(expected, violations)
        ):
            fail(f"{description} must yield violations naming {list(expected)}, got {violations}")
        else:
            ok(f"{description} yields {len(violations)} violation(s) naming {list(expected)}")


def check_restricted_profile(*extra_args: str) -> None:
    """Every pod the chart renders meets the restricted level, with every workload on.

    `extra_args` are further `helm template` flags, so a feature that changes the pods
    (TLS, say) can be proven against the same rules by calling this again with it on.
    The render holds every workload and every init container, plus the auth proxy
    example, whose sidecar is the one container leaving `runAsNonRoot` and the seccomp
    profile to the pod. A workload missing from the render fails the check, so that a
    renamed label cannot leave it examining nothing.
    """
    workloads = [
        document
        for document in load_documents(
            render_chart(*all_workloads_args(), "-f", EXAMPLE_VALUES, *extra_args)
        )
        if document.get("kind") in ("Deployment", "Job")
    ]
    expected = {component for _, component, _, _ in IMAGE_DIGEST_KEYS}
    rendered = {workload["metadata"]["labels"]["app.kubernetes.io/component"] for workload in workloads}
    if rendered != expected:
        fail(f"the render must hold a workload for each of {sorted(expected)}, it holds {sorted(rendered)}")
        return

    for workload in workloads:
        violations = restricted_violations(workload["spec"]["template"]["spec"])
        name = f"{workload['kind']} {workload['metadata']['name']}"
        if violations:
            fail(f"{name} breaks the restricted Pod Security level: {'; '.join(violations)}")
        else:
            ok(f"{name} meets the restricted Pod Security level")


def check_restricted_guard() -> None:
    """`podSecurity.checkRestricted` refuses each case naming the entry, and only when on."""
    for description, set_json, expected in RESTRICTED_GUARD_CASES:
        entry_args = ("--set-json", set_json)
        check_render_refuses(
            f"{description} with podSecurity.checkRestricted=true",
            expected,
            "--set",
            "podSecurity.checkRestricted=true",
            *entry_args,
        )
        error = capture_render_error(*entry_args)
        if error is not None:
            fail(f"{description} must render while podSecurity.checkRestricted is off:\n{error}")
            continue
        ok(f"{description} renders while podSecurity.checkRestricted is off")

        # The guard and `restricted_violations` hold two copies of one rule list. The pod the
        # guard-off render produces must therefore break a rule by the oracle's count too, in
        # the entry the guard refused.
        label = read_extension_label(set_json)
        violations = restricted_violations(read_ingest_pod_spec(*entry_args))
        if not any(label in violation for violation in violations):
            fail(f"the guard refuses {description}, but restricted_violations names no {label}: {violations}")
        else:
            ok(f"restricted_violations also names the {label} of {description}")


def check_restricted_guard_accepts_example() -> None:
    """The guard admits what it exists to protect, and `restricted_violations` agrees.

    The cases are the default values, the auth proxy example and a sidecar stating its own
    compliant settings. The example's sidecar sets neither `runAsNonRoot` nor a seccomp
    profile and relies on the pod-level ones, so it is accepted only if the guard counts
    that inheritance. For each, the ingest pod it renders must also be one the oracle finds
    nothing in: the two copies of the rule list admit the same pods.
    """
    for description, values_args in (
        ("the default values", ()),
        (EXAMPLE_VALUES, ("-f", EXAMPLE_VALUES)),
        ("a sidecar stating its own compliant settings", ("--set-json", RESTRICTED_ACCEPTED_SIDECAR)),
    ):
        args = ("--set", "podSecurity.checkRestricted=true", *values_args)
        error = capture_render_error(*args)
        if error is not None:
            fail(f"podSecurity.checkRestricted=true must admit {description}:\n{error}")
            continue
        violations = restricted_violations(read_ingest_pod_spec(*args))
        if violations:
            fail(f"the guard admits {description}, but restricted_violations reports {violations}")
        else:
            ok(f"podSecurity.checkRestricted=true and restricted_violations both admit {description}")


def tls_args(component: str) -> tuple[str, ...]:
    """The `--set` flags that serve TLS from `component`, rendering it if it is off by default."""
    settings = [f"{component}.tls.enabled=true", f"{component}.tls.secretName={component}-tls"]
    if component == "maintain":
        settings += ["maintain.enabled=true", "maintain.tracing.otlpEndpoint=http://otel:4317"]
    return tuple(flag for setting in settings for flag in ("--set", setting))


def read_main_container(component: str, pod_spec: dict) -> dict:
    """The container of `pod_spec` that runs `component`, as opposed to an init or sidecar one."""
    return next(container for container in pod_spec["containers"] if container["name"] == component)


def check_tls_default_render() -> None:
    """TLS is absent unless asked for: no mount, no https probe, no https scrape.

    A Secret name alone asks for nothing: `tls.enabled` is the switch, so a name left behind
    by an operator who turned TLS off must not mount a Secret the pod would then wait for.
    """
    secret_args = tuple(
        flag for component in TLS_COMPONENTS for flag in ("--set", f"{component}.tls.secretName={component}-tls")
    )
    for description, args in (("the default render", ()), ("a render holding Secret names but TLS off", secret_args)):
        rendered = render_chart(*all_workloads_args(), "--set", "serviceMonitor.enabled=true", *args)
        found = [text for text in (TLS_MOUNT_PATH, "scheme: HTTPS", "scheme: https", "tlsConfig") if text in rendered]
        if found:
            fail(f"{description} must not render TLS, but contains {found}")
        else:
            ok(f"{description} renders no TLS mount, probe scheme or scrape config")


def check_tls_secret_mount(component: str, pod_spec: dict) -> None:
    """The Secret reaches the pod as a read-only directory mount of the main container."""
    secret_name = f"{component}-tls"
    volumes = [v for v in pod_spec["volumes"] if v.get("secret", {}).get("secretName") == secret_name]
    container = read_main_container(component, pod_spec)
    mounts = [mount for mount in container["volumeMounts"] if mount["mountPath"] == TLS_MOUNT_PATH]
    if len(volumes) != 1:
        fail(f"{component}.tls.secretName must reach exactly one Secret volume, the pod has {volumes}")
    elif len(mounts) != 1:
        fail(f"the {component} container must mount the TLS Secret at {TLS_MOUNT_PATH} once, it mounts {mounts}")
    elif mounts[0]["name"] != volumes[0]["name"]:
        fail(f"the {component} mount at {TLS_MOUNT_PATH} names volume {mounts[0]['name']}, not {volumes[0]['name']}")
    elif mounts[0].get("readOnly") is not True:
        fail(f"the {component} mount at {TLS_MOUNT_PATH} must be read-only")
    elif "subPath" in mounts[0]:
        # The component re-reads both files in place, so the directory is mounted whole.
        fail(f"the {component} mount at {TLS_MOUNT_PATH} must have no subPath, it has {mounts[0]['subPath']}")
    else:
        ok(f"{component}.tls.secretName is mounted read-only at {TLS_MOUNT_PATH} as a directory")


def check_tls_probes(component: str, pod_spec: dict) -> None:
    """Every httpGet probe of the main container addresses the operational listener over https."""
    container = read_main_container(component, pod_spec)
    kinds = ("livenessProbe", "readinessProbe", "startupProbe")
    probes = {kind: container[kind] for kind in kinds if kind in container}
    http_probes = {kind: probe["httpGet"] for kind, probe in probes.items() if "httpGet" in probe}
    plain = [kind for kind, http_get in http_probes.items() if http_get.get("scheme") != "HTTPS"]
    if not http_probes:
        fail(f"the {component} render holds no httpGet probe, so its probe scheme is not covered")
    elif plain:
        fail(f"with {component}.tls.enabled the {plain} of {component} must use scheme HTTPS")
    else:
        ok(f"with {component}.tls.enabled the {sorted(http_probes)} of {component} use scheme HTTPS")


def check_tls_scrape(component: str, args: tuple[str, ...]) -> None:
    """The ServiceMonitor scrapes over https, verifying against the CA at the Service's DNS name."""
    monitor = load_documents(render_chart(*args, show_only=f"templates/servicemonitor-{component}.yaml"))[0]
    endpoint = monitor["spec"]["endpoints"][0]
    expected = {
        "scheme": "https",
        "tlsConfig": {
            "ca": {"configMap": {"name": TLS_CA_CONFIGMAP, "key": TLS_CA_KEY}},
            "serverName": f"icegate-{component}.default.svc",
        },
    }
    actual = {key: endpoint.get(key) for key in expected}
    if actual != expected:
        fail(f"the {component} ServiceMonitor endpoint must carry {expected}, it carries {actual}")
    else:
        ok(f"the {component} ServiceMonitor scrapes over https against {TLS_CA_CONFIGMAP}/{TLS_CA_KEY}")


def check_tls_wiring() -> None:
    """The Secret mounted, the config naming its files, the probes and the scrape on https."""
    check_tls_default_render()

    for component in TLS_COMPONENTS:
        # maintain ships no probe, and without one its probe scheme would go unchecked.
        probe_args = (
            ("--set-json", 'maintain.livenessProbe={"httpGet":{"path":"/health","port":"metrics"}}')
            if component == "maintain"
            else ()
        )
        args = (
            *tls_args(component),
            "--set",
            f"tls.caConfigMapName={TLS_CA_CONFIGMAP}",
            "--set",
            "serviceMonitor.enabled=true",
            *probe_args,
        )
        pod_spec = read_pod_spec(component, *args)
        check_tls_secret_mount(component, pod_spec)

        config = read_component_config(
            render_chart(*args, show_only=f"templates/configmap-{component}.yaml"), f"{component}.yaml"
        )
        expected = {"certificate_path": TLS_CERTIFICATE_PATH, "private_key_path": TLS_PRIVATE_KEY_PATH}
        if config.get("tls") != expected:
            fail(f"{component}.yaml must carry tls {expected} and no reload interval, got {config.get('tls')}")
        else:
            ok(f"{component}.yaml names the mounted certificate and key and leaves the reload interval to the code")

        check_tls_probes(component, pod_spec)
        check_tls_scrape(component, args)


def check_tls_reload_interval() -> None:
    """A stated reload interval reaches the config of every component."""
    for component in TLS_COMPONENTS:
        args = (*tls_args(component), "--set", f"{component}.tls.reloadIntervalSecs=7")
        config = read_component_config(
            render_chart(*args, show_only=f"templates/configmap-{component}.yaml"), f"{component}.yaml"
        )
        interval = config.get("tls", {}).get("reload_interval_secs")
        if interval != 7:
            fail(f"{component}.tls.reloadIntervalSecs must reach tls.reload_interval_secs: expected 7, got {interval}")
        else:
            ok(f"{component}.tls.reloadIntervalSecs reaches tls.reload_interval_secs")


def check_tls_reload_interval_digits() -> None:
    """A large reload interval is rendered in plain digits, not in exponent form.

    A number a values file hands the template is a float, and a float of seven digits or more
    prints as `1e+06`, which the component's YAML reader takes for a string rather than an
    integer. `--set-json` hands the chart a float as a values file does; `--set` hands it an
    integer and would prove nothing.
    """
    for component in TLS_COMPONENTS:
        args = (*tls_args(component), "--set-json", f"{component}.tls.reloadIntervalSecs=1000000")
        manifests = render_chart(*args, show_only=f"templates/configmap-{component}.yaml")
        body = next(
            document["data"][f"{component}.yaml"]
            for document in load_documents(manifests)
            if document.get("kind") == "ConfigMap"
        )
        lines = [line.strip() for line in body.splitlines()]
        if "reload_interval_secs: 1000000" not in lines:
            rendered = [line for line in lines if line.startswith("reload_interval_secs")]
            fail(f"{component}.tls.reloadIntervalSecs=1000000 must render as plain digits, got {rendered}")
        else:
            ok(f"{component}.tls.reloadIntervalSecs=1000000 renders as plain digits")


def check_tls_probe_passthrough() -> None:
    """A probe the chart has no default for is rendered as the operator wrote it."""
    base = tls_args("ingest")
    # An operator who states a scheme gets it, even a plaintext one: the chart adds a
    # default, it does not overrule a choice.
    container = read_main_container(
        "ingest", read_ingest_pod_spec(*base, "--set", "ingest.livenessProbe.httpGet.scheme=HTTP")
    )
    schemes = tuple(container[kind]["httpGet"].get("scheme") for kind in ("livenessProbe", "readinessProbe"))
    if schemes != ("HTTP", "HTTPS"):
        fail(f"an httpGet probe stating scheme HTTP must keep it while the other gains HTTPS, got {schemes}")
    else:
        ok("an httpGet probe stating its scheme keeps it, and the other probe gains HTTPS")

    # A probe that is not an httpGet has no scheme to set. `httpGet: null` removes the
    # chart's default key, which Helm would otherwise merge the new probe into.
    tcp_probe = '{"httpGet":null,"tcpSocket":{"port":"metrics"}}'
    container = read_main_container(
        "ingest", read_ingest_pod_spec(*base, "--set-json", f"ingest.livenessProbe={tcp_probe}")
    )
    probe = container["livenessProbe"]
    if "httpGet" in probe or probe.get("tcpSocket") != {"port": "metrics"}:
        fail(f"a tcpSocket probe must be rendered as written, got {probe}")
    else:
        ok("a tcpSocket probe is rendered as written")


def check_tls_render_refusals() -> None:
    """TLS without the Secret to serve, and a TLS scrape without the CA to verify it."""
    check_render_refuses(
        "TLS without a Secret", "ingest.tls.secretName is required", "--set", "ingest.tls.enabled=true"
    )
    check_render_refuses(
        "a TLS scrape without a CA",
        "tls.caConfigMapName is required",
        "--set",
        "ingest.tls.enabled=true",
        "--set",
        "ingest.tls.secretName=x",
        "--set",
        "serviceMonitor.enabled=true",
    )
    # The same guard on the other two components, each by its own name. An empty name and
    # a removed key are refused as well as an absent one.
    check_render_refuses(
        "query TLS without a Secret", "query.tls.secretName is required", "--set", "query.tls.enabled=true"
    )
    for secret_name in ("null", ""):
        check_render_refuses(
            f"maintain TLS with secretName={secret_name!r}",
            "maintain.tls.secretName is required",
            "--set",
            "maintain.enabled=true",
            "--set",
            "maintain.tracing.otlpEndpoint=http://otel:4317",
            "--set",
            "maintain.tls.enabled=true",
            "--set",
            f"maintain.tls.secretName={secret_name}",
        )

    # A misspelt switch would otherwise be ignored, and a component the operator believes
    # is serving TLS would keep serving plaintext. The values schema names the object.
    check_render_refuses(
        "an unknown key under ingest.tls",
        "'/ingest/tls': additional properties 'enable' not allowed",
        "--set",
        "ingest.tls.enable=true",
    )

    # The component refuses a zero interval when it starts; the schema refuses it at the
    # key, so the install fails before any pod is asked to.
    check_render_refuses(
        "a zero reload interval",
        "'/ingest/tls/reloadIntervalSecs': minimum: got 0, want 1",
        *tls_args("ingest"),
        "--set",
        "ingest.tls.reloadIntervalSecs=0",
    )

    # The CA is asked for where a ServiceMonitor is rendered, not wherever the global
    # switch is on: with its metrics endpoint off, a component has no scrape to verify.
    error = capture_render_error(
        *tls_args("ingest"), "--set", "ingest.metrics.enabled=false", "--set", "serviceMonitor.enabled=true"
    )
    if error is not None:
        fail(f"TLS with no ServiceMonitor to render must not ask for a CA:\n{error}")
    else:
        ok("TLS with ingest.metrics.enabled=false asks for no CA, as no ServiceMonitor is rendered")


for component_name in COMPONENTS:
    check_tenant_policy(component_name)
    check_probe_target(component_name)

check_operational_host()
check_otlp_port_names()
check_otlp_host_guards()
check_service_ports()
check_notes()
check_pod_extensions()
check_jobs_storage_endpoint()
check_image_digests()
check_digest_format_refused()
check_helper_image_string_refused()
check_app_protocol()
check_app_protocol_disabled_port()
check_app_protocol_null_map()
check_restricted_violations()
check_restricted_profile()
check_restricted_guard()
check_restricted_guard_accepts_example()
check_tls_wiring()
check_tls_reload_interval()
check_tls_reload_interval_digits()
check_tls_probe_passthrough()
check_tls_render_refusals()
# The Secret volume is a new volume type in every pod that serves TLS; the level allows
# `secret`, and this proves the pods rendered with it on still meet the level.
check_restricted_profile(*(flag for component in TLS_COMPONENTS for flag in tls_args(component)))

sys.exit(report_failures())
