#!/bin/bash
# ==============================================================================
# Vendors the xsl/ directory from the Library of Congress marc2bibframe2
# project (https://github.com/lcnetdev/marc2bibframe2) at a pinned release
# tag into ils_middleware/tasks/general/xslt/marc2bibframe2/.
#
# Usage: scripts/update-marc2bibframe2.sh [tag]
#   tag defaults to the version currently recorded in the vendored VERSION file.
# ------------------------------------------------------------------------------
set -euo pipefail

REPO="lcnetdev/marc2bibframe2"
DEST="ils_middleware/tasks/general/xslt/marc2bibframe2"
TAG="${1:-$(awk -F': ' '/^tag:/ {print $2}' "${DEST}/VERSION" 2>/dev/null || echo v3.1.0)}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "${WORKDIR}"' EXIT

echo "Fetching ${REPO}@${TAG}..."
SHA="$(curl -sf "https://api.github.com/repos/${REPO}/git/refs/tags/${TAG}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["object"]["sha"])')"
curl -sfL "https://github.com/${REPO}/archive/${SHA}.tar.gz" -o "${WORKDIR}/src.tar.gz"
tar -xzf "${WORKDIR}/src.tar.gz" -C "${WORKDIR}"
SRC_DIR="$(find "${WORKDIR}" -maxdepth 1 -type d -name "marc2bibframe2-*")"

rm -rf "${DEST}/xsl"
mkdir -p "${DEST}"
cp -R "${SRC_DIR}/xsl" "${DEST}/xsl"
cp "${SRC_DIR}/LICENSE" "${DEST}/LICENSE"

cat > "${DEST}/VERSION" <<EOF
source: https://github.com/${REPO}
tag: ${TAG}
commit: ${SHA}
EOF

echo "Vendored ${REPO}@${TAG} (${SHA}) into ${DEST}"
