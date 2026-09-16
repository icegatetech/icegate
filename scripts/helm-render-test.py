#!/usr/bin/env python3
"""Render the chart the ways a default render does not cover, and read the result.

The tenant policy, the OTLP bind addresses and the ingest operational listener are
the three places where a rendering mistake fails open rather than loudly: a
`single` policy with no id rejects every batch, a published OTLP port with nothing
in front of it is writable by any neighbour in the namespace, and a loopback
`ingest.metrics.host` answers the container alone, so both probes fail with nothing
to say beyond that. None of the three is visible in the default render, so each is
rendered here on purpose.

Every check states the message it expects a guard to fail with. That message is the
contract with the operator who hits the guard, so a guard that starts refusing for
another reason is a failure here even when it still refuses.
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from chartlib import (  # noqa: E402  (path set above so this file runs from the repo root)
    capture_render_error,
    fail,
    ok,
    read_container_port_names,
    read_ingest_config,
    read_service_port,
    render_chart,
    render_notes,
    report_failures,
)

EXAMPLE_VALUES = "config/helm/auth-proxy/values-authproxy.yaml"

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


def check_tenant_policy() -> None:
    """The tagged union the pod loads, and the three ways of asking for a broken one."""
    check_render_refuses(
        "tenant.mode=single without tenant.id",
        "ingest.tenant.id is required",
        "--set",
        "ingest.tenant.mode=single",
        "--set",
        "ingest.tenant.id=",
    )
    # An unknown mode is caught by the values schema, which names the path it
    # rejected rather than the chart's own message.
    check_render_refuses("an unknown tenant.mode", "/ingest/tenant/mode", "--set", "ingest.tenant.mode=bogus")
    check_render_refuses(
        "an absent tenant.mode",
        "ingest.tenant.mode must be single or multi",
        "--set",
        "ingest.tenant.mode=null",
    )

    if "tenant: !multi" not in render_chart("--set", "ingest.tenant.mode=multi"):
        fail("tenant.mode=multi must render the !multi tag")
    else:
        ok("tenant.mode=multi renders the !multi tag")

    rendered = render_chart(show_only="templates/configmap-ingest.yaml")
    if "tenant: !single" not in rendered:
        fail("the default render must carry the !single tag")
    elif 'id: "default"' not in rendered:
        fail("the default render must name the chart's tenant id")
    else:
        ok("the default render carries !single and the chart's tenant id")

    named = render_chart("--set", "ingest.tenant.id=acme", show_only="templates/configmap-ingest.yaml")
    if 'id: "acme"' not in named:
        fail("ingest.tenant.id must reach the rendered tenant section")
    else:
        ok("ingest.tenant.id reaches the rendered tenant section")


def check_operational_listener() -> None:
    """The listener the probes address, and the binds that would leave them blind."""
    rendered = render_chart("--set", "ingest.metrics.enabled=false", show_only="templates/deployment-ingest.yaml")
    if "metrics" not in read_container_port_names(rendered):
        fail("the metrics port must be declared even with ingest.metrics.enabled=false: the probes address it by name")
    elif rendered.count("port: metrics") != 2:
        fail("both probes must still address the metrics port by name")
    else:
        ok("ingest.metrics.enabled=false keeps the port declared and both probes addressing it")

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
    config = read_ingest_config(render_chart(*loopback_args, show_only="templates/configmap-ingest.yaml"))
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
    config = read_ingest_config(render_chart(show_only="templates/configmap-ingest.yaml"))
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
    config = read_ingest_config(render_chart(*split_args, show_only="templates/configmap-ingest.yaml"))
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


check_tenant_policy()
check_operational_listener()
check_otlp_port_names()
check_otlp_host_guards()
check_service_ports()
check_notes()
check_pod_extensions()

sys.exit(report_failures())
