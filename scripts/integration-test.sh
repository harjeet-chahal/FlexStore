#!/usr/bin/env bash
# Runs the integration suite against a running FlexStore cluster.
#
# Prefers a native Go toolchain. Falls back to a containerised runner (with the
# Docker socket mounted, because the failure tests really do stop and restart
# cluster containers) so the suite works on a machine with Docker but no Go.
set -euo pipefail

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
GATEWAY_URL=${FLEXSTORE_GATEWAY_URL:-http://localhost:8080}
RUNNER_IMAGE=${RUNNER_IMAGE:-flexstore/test-runner:dev}
TIMEOUT=${TIMEOUT:-25m}

# Extra args (e.g. -short, -run TestFoo) pass straight through to `go test`.
# `set -u` makes an empty array expansion an error on bash 3 (macOS default),
# so guard the expansion rather than the assignment.
EXTRA_ARGS=("$@")
if [ $# -eq 0 ]; then EXTRA_ARGS=(); fi

if command -v go >/dev/null 2>&1; then
  echo "==> using the native Go toolchain"
  cd "${REPO_ROOT}"
  FLEXSTORE_GATEWAY_URL="${GATEWAY_URL}" \
    exec go test -tags=integration -count=1 -v -timeout="${TIMEOUT}" \
      ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} ./tests/integration/...
fi

echo "==> no local Go; building the containerised test runner"
docker build -q -f "${REPO_ROOT}/deploy/docker/Dockerfile.testrunner" -t "${RUNNER_IMAGE}" "${REPO_ROOT}" >/dev/null

# host.docker.internal reaches the host's published gateway port from inside
# the runner; --add-host makes that name resolve on Linux too.
CONTAINER_GATEWAY_URL=${GATEWAY_URL/localhost/host.docker.internal}
CONTAINER_GATEWAY_URL=${CONTAINER_GATEWAY_URL/127.0.0.1/host.docker.internal}

echo "==> running integration tests against ${CONTAINER_GATEWAY_URL}"
exec docker run --rm \
  -v "${REPO_ROOT}:/src" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v flexstore-gocache:/root/.cache/go-build \
  -v flexstore-gomodcache:/go/pkg/mod \
  --add-host=host.docker.internal:host-gateway \
  -e FLEXSTORE_GATEWAY_URL="${CONTAINER_GATEWAY_URL}" \
  -w /src \
  "${RUNNER_IMAGE}" \
  go test -tags=integration -count=1 -v -timeout="${TIMEOUT}" ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"} ./tests/integration/...
