#!/usr/bin/env python3
"""Shared helpers for the chart and proxy checks in this directory.

Three scripts assert that something in the repo still agrees with something else:
`helm-render-test.py` renders the chart and reads the result, `envoy-config-test.py`
compares the two auth proxy copies against each other, and `helm-metadata-test.py`
compares the chart's metadata against the sources it copies. All three run `helm`,
read YAML out of what comes back, and report a list of failures rather than
stopping at the first one — an operator fixing a chart wants every stale copy named
in one run, not one per invocation.

Failures accumulate in a module-level list, so a script collects them by calling
`fail` and ends with `sys.exit(report_failures())`.
"""

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - environment, not logic
    sys.exit(
        "PyYAML is required by the chart checks and is not installed.\n"
        "Run `make install-deps`, which builds .venv and installs it there."
    )

CHART_DIR = Path("config/helm/icegate")
RELEASE_NAME = "icegate"

failures: list[str] = []


class _TolerantLoader(yaml.SafeLoader):
    """A loader that keeps an unknown tag instead of refusing the document.

    `ingest.yaml` carries `tenant: !single`, a tag serde understands and PyYAML
    does not. Without this the whole config would be unreadable here for the sake
    of one section that is checked as text anyway, the tag being the thing under
    test there.
    """


def _keep_unknown_tag(loader: yaml.Loader, suffix: str, node: yaml.Node) -> dict:
    del suffix  # the tag itself is checked as text, not through this loader
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return {}


_TolerantLoader.add_multi_constructor("!", _keep_unknown_tag)


def fail(message: str) -> None:
    """Record a failure and print it, leaving the run to continue."""
    failures.append(message)
    print(f"FAIL: {message}", file=sys.stderr)


def ok(message: str) -> None:
    """Print a passing check, so a green run states what it covered."""
    print(f"ok: {message}")


def report_failures() -> int:
    """The exit code for the accumulated failures: 1 when any were recorded."""
    return 1 if failures else 0


def render_chart(*args: str, show_only: str | None = None) -> str:
    """Render the chart and return the manifests.

    `args` are passed to `helm template` as written, so a caller states its own
    `--set` and `-f` flags. `show_only` names a single template, which is how a
    check counts something the whole-chart render carries more than once.

    Exits the process on a failed render: every caller of this function expects
    the render to succeed, so a failure here is not a check result but a broken
    chart, and continuing would report it as a missing string instead.
    """
    command = ["helm", "template", RELEASE_NAME, str(CHART_DIR), *args]
    if show_only:
        command += ["--show-only", show_only]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"the render {' '.join(args)} failed unexpectedly:\n{result.stderr}")
    return result.stdout


def capture_render_error(*args: str) -> str | None:
    """Render the chart expecting it to fail, and return the message it failed with.

    Returns `None` when the render succeeded, which is itself the failure the
    caller is checking for: these renders exercise the guards, and a guard that
    admits what it exists to refuse produces no message to match.
    """
    command = ["helm", "template", RELEASE_NAME, str(CHART_DIR), *args]
    result = subprocess.run(command, capture_output=True, text=True)
    return None if result.returncode == 0 else result.stderr


def render_notes(*args: str) -> str:
    """The NOTES an install would print, through a client-side dry run.

    `helm template` renders no NOTES, so the text an operator reads first is
    reachable only this way.

    Exits the process when the dry run fails, for the reason `render_chart` does.
    """
    command = ["helm", "install", RELEASE_NAME, str(CHART_DIR), "--dry-run=client", *args]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        sys.exit(f"the dry run {' '.join(args)} failed unexpectedly:\n{result.stderr}")
    _, _, notes = result.stdout.partition("NOTES:")
    return notes


def load_documents(manifests: str) -> list[dict]:
    """Every non-empty YAML document of a render, in order."""
    return [document for document in yaml.load_all(manifests, Loader=_TolerantLoader) if document]


def load_document(path: Path) -> dict:
    """The single YAML document of `path`."""
    return yaml.load(path.read_text(), Loader=_TolerantLoader) or {}


def read_configmap_text(path: Path, key: str) -> str:
    """The file a ConfigMap embeds under `data.<key>`, as the text it will be mounted as.

    A ConfigMap carries a whole file as one string, so the text comes back without
    the block indentation it is written with — which is what a consumer of the file
    has to be handed.
    """
    return load_document(path).get("data", {}).get(key, "") or ""


def read_configmap_file(path: Path, key: str) -> dict:
    """The YAML document a ConfigMap carries under `data.<key>`."""
    return yaml.load(read_configmap_text(path, key) or "{}", Loader=_TolerantLoader) or {}


def iter_service_ports(manifests: str) -> Iterator[dict]:
    """Every `ports[]` entry of every Service of a render, in order."""
    for document in load_documents(manifests):
        if document.get("kind") == "Service":
            yield from document.get("spec", {}).get("ports", [])


def read_service_port_entry(manifests: str, port_name: str) -> dict | None:
    """The whole entry a rendered Service publishes under `port_name`, or `None`.

    The entry rather than its number, for a check of a field other than `port`, such
    as `appProtocol`.
    """
    return next((port for port in iter_service_ports(manifests) if port.get("name") == port_name), None)


def read_service_port(manifests: str, port_name: str) -> int | None:
    """The port a rendered Service publishes under `port_name`.

    Read out of the render rather than stated here, so a check comparing it with
    something else does not become a second copy of the ports `values.yaml` ships.
    """
    return (read_service_port_entry(manifests, port_name) or {}).get("port")


def iter_container_ports(manifests: str) -> Iterator[dict]:
    """Every `ports[]` entry of every container of a rendered Deployment, in order."""
    for document in load_documents(manifests):
        if document.get("kind") != "Deployment":
            continue
        for container in document.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
            yield from container.get("ports", [])


def read_container_port_names(manifests: str) -> list[str]:
    """Every `ports[].name` of every container of a rendered Deployment, with repeats.

    Repeats are kept because that is what the checks are about: a name is unique
    within a Pod, the Service addresses its `targetPort` by it, and a second
    declaration renders and lints clean while the API server refuses the Pod.
    """
    return [port["name"] for port in iter_container_ports(manifests) if "name" in port]


def read_container_port(manifests: str, port_name: str) -> int | None:
    """The `containerPort` a rendered Deployment declares under `port_name`.

    Read out of the render for the reason `read_service_port` is: a check comparing
    it with the port a component binds must not become a copy of `values.yaml`.
    """
    return next(
        (port.get("containerPort") for port in iter_container_ports(manifests) if port.get("name") == port_name),
        None,
    )


# The rules `restricted_violations` enforces. They are this check's own list, stated once
# here: `icegate.validateRestrictedExtensions` in the chart applies the same rules to the
# entries an operator adds, and `helm-render-test.py` shows each of them firing.
RESTRICTED_HOST_NAMESPACES = ("hostNetwork", "hostPID", "hostIPC")
RESTRICTED_SECCOMP_TYPES = ("RuntimeDefault", "Localhost")
RESTRICTED_ADDABLE_CAPABILITIES = frozenset({"NET_BIND_SERVICE"})
RESTRICTED_VOLUME_TYPES = frozenset(
    {"configMap", "csi", "downwardAPI", "emptyDir", "ephemeral", "persistentVolumeClaim", "projected", "secret"}
)


def read_inherited_setting(container_context: dict, pod_context: dict, setting: str) -> object:
    """The value of `setting` a container runs with: its own when it states one, else the pod's.

    Only `None` counts as not stated, so a container's explicit `false` or `0` overrides the
    pod's rather than falling through to it.
    """
    own = container_context.get(setting)
    return pod_context.get(setting) if own is None else own


def collect_identity_violations(
    label: str, run_as_non_root: object, run_as_user: object, seccomp: object, *, is_unset_allowed: bool
) -> list[str]:
    """The runAsNonRoot, runAsUser and seccomp rules, applied to the values `label` runs with.

    A container is judged by the values it runs with, so an unset one breaks its rule
    (`is_unset_allowed` false). The pod is judged by what it states, so an unset one is left
    to the containers that inherit it (`is_unset_allowed` true).
    """
    violations = []
    is_non_root_judged = run_as_non_root is not None or not is_unset_allowed
    if is_non_root_judged and run_as_non_root is not True:
        violations.append(f"{label}: runAsNonRoot is not true")
    if run_as_user == 0:
        violations.append(f"{label}: runAsUser is 0")
    is_seccomp_judged = seccomp is not None or not is_unset_allowed
    if is_seccomp_judged and (seccomp or {}).get("type") not in RESTRICTED_SECCOMP_TYPES:
        violations.append(f"{label}: seccompProfile.type is not one of {', '.join(RESTRICTED_SECCOMP_TYPES)}")
    return violations


def collect_container_violations(label: str, container: dict, pod_context: dict) -> list[str]:
    """The restricted-level rules one container breaks, each as a message led by `label`."""
    context = container.get("securityContext") or {}
    capabilities = context.get("capabilities") or {}
    violations = []

    if context.get("allowPrivilegeEscalation") is not False:
        violations.append(f"{label}: allowPrivilegeEscalation is not false")
    if "ALL" not in (capabilities.get("drop") or []):
        violations.append(f"{label}: capabilities.drop does not contain ALL")
    added = sorted(set(capabilities.get("add") or []) - RESTRICTED_ADDABLE_CAPABILITIES)
    if added:
        violations.append(f"{label}: capabilities.add holds {added}, only NET_BIND_SERVICE may be added")
    if context.get("privileged"):
        violations.append(f"{label}: privileged is true")
    violations += collect_identity_violations(
        label,
        read_inherited_setting(context, pod_context, "runAsNonRoot"),
        read_inherited_setting(context, pod_context, "runAsUser"),
        read_inherited_setting(context, pod_context, "seccompProfile"),
        is_unset_allowed=False,
    )
    violations.extend(
        f"{label}: hostPort {port['hostPort']} is published"
        for port in container.get("ports") or []
        if port.get("hostPort")
    )
    return violations


def restricted_violations(pod_spec: dict) -> list[str]:
    """The rules of the restricted Pod Security level that `pod_spec` breaks, one message each.

    `pod_spec` is the `spec` of a pod, or the `spec.template.spec` of a workload. An empty
    list means it meets every rule this check enforces: no host namespace, only the volume
    types of `RESTRICTED_VOLUME_TYPES`, and per container and init container no privilege
    escalation, every capability dropped, `runAsNonRoot` true under a non-zero user, a seccomp
    profile of `RESTRICTED_SECCOMP_TYPES` and no host port. `runAsNonRoot`, `runAsUser` and
    the seccomp profile are the pod's for every container that does not state its own, so a
    pod-level value that breaks a rule is reported twice: once as `pod:` and once for each
    container that inherits it. A pod-level value that is set is judged on its own as well,
    because a container stating a compliant value of its own does not make the pod's one
    compliant; one that is unset is judged only through the containers that inherit it.
    """
    pod_context = pod_spec.get("securityContext") or {}
    violations = [f"pod: {field} is true" for field in RESTRICTED_HOST_NAMESPACES if pod_spec.get(field)]
    violations += collect_identity_violations(
        "pod",
        pod_context.get("runAsNonRoot"),
        pod_context.get("runAsUser"),
        pod_context.get("seccompProfile"),
        is_unset_allowed=True,
    )
    for volume in pod_spec.get("volumes") or []:
        violations.extend(
            f"volume {volume.get('name')}: type {volume_type} is not allowed"
            for volume_type in volume
            if volume_type != "name" and volume_type not in RESTRICTED_VOLUME_TYPES
        )
    for kind, key in (("init container", "initContainers"), ("container", "containers")):
        for container in pod_spec.get(key) or []:
            violations += collect_container_violations(f"{kind} {container.get('name')}", container, pod_context)
    return violations


def read_component_config(manifests: str, key: str) -> dict:
    """The config file the rendered ConfigMap carries under `data.<key>`, e.g. `ingest.yaml`.

    The tenant section is a YAML tagged union (`!single` / `!multi`), which
    `safe_load` refuses, so the document is read with a loader that keeps an
    unknown tag as the tag name and its value. That is enough for every caller:
    they read plain sections such as `otlp_http.port`, and the tenant section is
    checked as text, where the tag is the thing under test.
    """
    for document in load_documents(manifests):
        if document.get("kind") != "ConfigMap":
            continue
        body = document.get("data", {}).get(key)
        if body:
            return yaml.load(body, Loader=_TolerantLoader) or {}
    return {}


def read_envoy_cluster_port(document: dict, cluster_name: str) -> int | None:
    """The upstream port an Envoy cluster proxies to.

    This is the loopback port its neighbour must bind: one file binds the
    receiver, the other proxies to it, and a change to one of the two leaves a
    deployment that starts clean and refuses every OTLP request upstream.
    """
    for cluster in document.get("static_resources", {}).get("clusters", []):
        if cluster.get("name") != cluster_name:
            continue
        for endpoint in cluster.get("load_assignment", {}).get("endpoints", []):
            for lb_endpoint in endpoint.get("lb_endpoints", []):
                address = lb_endpoint.get("endpoint", {}).get("address", {}).get("socket_address", {})
                if "port_value" in address:
                    return address["port_value"]
    return None


def read_envoy_listener_port(document: dict, listener_name: str) -> int | None:
    """The port an Envoy listener accepts on — what a sender addresses."""
    for listener in document.get("static_resources", {}).get("listeners", []):
        if listener.get("name") != listener_name:
            continue
        address = listener.get("address", {}).get("socket_address", {})
        if "port_value" in address:
            return address["port_value"]
    return None


def read_envoy_route_timeouts(document: dict) -> dict[str, str]:
    """Every route's timeout, keyed by the cluster the route sends to.

    Scoped to the route rather than to the key: `timeout` also ends
    `connect_timeout`, and `remote_jwks.http_uri` carries a `timeout` of its own,
    so a search for the key alone would report a neighbour.
    """
    timeouts: dict[str, str] = {}
    for listener in document.get("static_resources", {}).get("listeners", []):
        for chain in listener.get("filter_chains", []):
            for network_filter in chain.get("filters", []):
                route_config = network_filter.get("typed_config", {}).get("route_config", {})
                for virtual_host in route_config.get("virtual_hosts", []):
                    for route in virtual_host.get("routes", []):
                        action = route.get("route", {})
                        cluster = action.get("cluster")
                        if cluster and "timeout" in action:
                            timeouts[cluster] = action["timeout"]
    return timeouts
