#!/usr/bin/env python3
"""Refuse a release whose version already exists in the container registry.

Usage: release-check-unpublished.py <registry>/<repository>:<tag> [<ref> ...]

A release pushes the images and the chart under one version and signs each by digest.
Pushing that version again would repoint tags away from content that may already be
signed and pulled, so a version that half-published is used up: the next release takes
the next version. This script is the gate that enforces that before anything is pushed.

Exit status:

    0  no ref exists
    1  at least one ref exists; each one is printed as `already published: <ref>`
    2  any other outcome for any ref, printed as `could not check <ref>: <reason>`:
       a malformed ref, a network or token failure, or any HTTP status other than
       200 and 404. A missing ref is never inferred from a failure, so a package the
       anonymous check cannot see (401, 403) stops the release instead of passing it.

Every ref is checked before exiting, so one run names every ref that is in the way. When
some refs exist and others could not be checked the status is 1: a retry cannot free a
ref that exists, so the answer that is actually known wins.

Pass only the tags that name this version. The moving tags a release also pushes
(`<major>.<minor>`, `latest`, `artifacthub.io`) exist after the first release by design
and must never be checked.

The check is anonymous, so it needs no credentials, and it uses the standard library
only, so it needs no dependency install.
"""

import json
import sys
import urllib.error
import urllib.parse
import urllib.request

# Every manifest media type a tag can point at, so a tag is found whatever it names: a
# multi-arch image index or a single manifest, in the OCI or the Docker form.
MANIFEST_MEDIA_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
)

# A hung registry has to fail the release, not stall it until the runner's own limit.
REQUEST_TIMEOUT_SECONDS = 30

EXIT_UNPUBLISHED = 0
EXIT_PUBLISHED = 1
EXIT_UNCHECKED = 2


class RegistryError(Exception):
    """The state of a ref could not be determined; the message is the reason."""


def split_ref(ref: str) -> tuple[str, str, str]:
    """Split `registry/repository:tag` into its three parts.

    The registry ends at the first slash, so a port in it is not mistaken for the tag;
    the tag starts at the last colon of what remains. The repository keeps any slashes.
    """
    registry, _, remainder = ref.partition("/")
    repository, _, tag = remainder.rpartition(":")
    if not (registry and repository and tag):
        raise RegistryError("expected <registry>/<repository>:<tag>")
    return registry, repository, tag


def fetch_pull_token(registry: str, repository: str) -> str:
    """Get an anonymous token that may pull `repository`."""
    query = urllib.parse.urlencode({"scope": f"repository:{repository}:pull"})
    with urllib.request.urlopen(
        f"https://{registry}/token?{query}", timeout=REQUEST_TIMEOUT_SECONDS
    ) as response:
        document = json.load(response)
    token = document.get("token") if isinstance(document, dict) else None
    if not token:
        raise RegistryError("the token response carries no token")
    return token


def manifest_exists(registry: str, repository: str, tag: str, token: str) -> bool:
    """Say whether `tag` names a manifest; any status but 200 and 404 is an error."""
    url = f"https://{registry}/v2/{repository}/manifests/{tag}"
    request = urllib.request.Request(
        url,
        method="HEAD",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": ", ".join(MANIFEST_MEDIA_TYPES),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            status = response.status
    except urllib.error.HTTPError as error:
        # urllib raises for every non-2xx status; the status is what decides below.
        status = error.code
    if status == 200:
        return True
    if status == 404:
        return False
    raise RegistryError(f"HTTP {status} from {url}")


def main(refs: list[str]) -> int:
    """Check every ref and return the exit status described in the module docstring."""
    if not refs:
        # No refs must not read as "nothing is published": an empty list passed by a
        # broken caller would otherwise let the release through.
        print(f"usage: {sys.argv[0]} <registry>/<repository>:<tag> [<ref> ...]", file=sys.stderr)
        return EXIT_UNCHECKED

    any_published = False
    any_unchecked = False
    for ref in refs:
        try:
            registry, repository, tag = split_ref(ref)
            token = fetch_pull_token(registry, repository)
            if manifest_exists(registry, repository, tag, token):
                any_published = True
                print(f"already published: {ref}")
        except RegistryError as error:
            any_unchecked = True
            print(f"could not check {ref}: {error}", file=sys.stderr)
        except Exception as error:  # noqa: BLE001 - see the comment below
            # Whatever else goes wrong (DNS, TLS, timeout, a body that is not JSON) must
            # end as exit 2: an uncaught exception exits 1, which this script reserves
            # for "already published", and a broken check would then read as a finding.
            any_unchecked = True
            print(f"could not check {ref}: {type(error).__name__}: {error}", file=sys.stderr)

    if any_published:
        return EXIT_PUBLISHED
    return EXIT_UNCHECKED if any_unchecked else EXIT_UNPUBLISHED


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
