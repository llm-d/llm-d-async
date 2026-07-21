#!/usr/bin/env bash
# Patch the Helm chart for a git release tag, package it, append the chart digest
# to SHA256SUMS, and push the chart to GHCR as OCI.
#
# Required environment:
#   VERSION       Git tag (e.g. v1.0.0)
#   GITHUB_TOKEN  Token for helm registry login to ghcr.io
#   GITHUB_ACTOR  Username for registry login (e.g. github.actor in Actions)
#
# Chart is always pushed to oci://ghcr.io/llm-d/charts (not configurable).
#
# Requires: helm, yq (mikefarah). Run after make package-release so release/ exists.

## Copied from https://github.com/llm-d/llm-d-batch-gateway


set -euo pipefail

VERSION="${VERSION:?VERSION is required (e.g. v1.0.0)}"
IMAGE_TAG="${IMAGE_TAG:-${VERSION}}"
export VERSION
export IMAGE_TAG

HELM_OCI_REGISTRY='oci://ghcr.io/llm-d/charts'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

command -v yq >/dev/null 2>&1 || {
  echo "yq is required (https://github.com/mikefarah/yq)" >&2
  exit 1
}
command -v helm >/dev/null 2>&1 || {
  echo "helm is required" >&2
  exit 1
}

yq -i '.ap.image.tag = strenv(IMAGE_TAG)' charts/llm-d-async/values.yaml
# Chart version matches VERSION directly, preserving leading "v" if present (following the llm-d-router pattern).
# appVersion matches the image tag (IMAGE_TAG) so that any fallback to .Chart.AppVersion resolves to a container image tag that actually exists.
yq -i '.version = strenv(VERSION) | .appVersion = strenv(IMAGE_TAG)' charts/llm-d-async/Chart.yaml

helm package charts/llm-d-async -d release/

(cd release && sha256sum "llm-d-async-${VERSION}.tgz" >> SHA256SUMS && cat SHA256SUMS)

printf '%s' "${GITHUB_TOKEN}" | helm registry login ghcr.io -u "${GITHUB_ACTOR}" --password-stdin
helm push "release/llm-d-async-${VERSION}.tgz" "${HELM_OCI_REGISTRY}"

echo "Helm chart published: ${HELM_OCI_REGISTRY}/llm-d-async:${VERSION}"
