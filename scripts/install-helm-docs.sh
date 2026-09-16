#!/usr/bin/env bash
# Install the pinned helm-docs release into /usr/local/bin.
#
# The pinned version lives here and nowhere else: helm-docs regenerates the chart
# README both in CI (.github/workflows/deploy-config.yml, which diffs it) and at tag
# time (.github/workflows/release.yml, which ships the result), and two copies of
# a tool version drift into two different READMEs.
#
# Usage: scripts/install-helm-docs.sh
set -euo pipefail

VERSION=1.14.2

case "$(uname -s)" in
    Linux) OS=Linux ;;
    Darwin) OS=Darwin ;;
    *)
        echo "install-helm-docs.sh: unsupported OS $(uname -s); install helm-docs manually" >&2
        exit 2
        ;;
esac

case "$(uname -m)" in
    x86_64 | amd64) ARCH=x86_64 ;;
    arm64 | aarch64) ARCH=arm64 ;;
    *)
        echo "install-helm-docs.sh: unsupported architecture $(uname -m)" >&2
        exit 2
        ;;
esac

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

curl -fsSL -o "${TMP}/helm-docs.tar.gz" \
    "https://github.com/norwoodj/helm-docs/releases/download/v${VERSION}/helm-docs_${VERSION}_${OS}_${ARCH}.tar.gz"
tar -xzf "${TMP}/helm-docs.tar.gz" -C "$TMP" helm-docs
sudo install -m 0755 "${TMP}/helm-docs" /usr/local/bin/helm-docs

command -v helm-docs
