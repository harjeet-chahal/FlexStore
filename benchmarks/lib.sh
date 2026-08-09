#!/usr/bin/env bash
# Shared helpers for the FlexStore benchmark scripts.
#
# Sourced, not executed. Everything here is deliberately side-effect free until
# called, so `shellcheck benchmarks/*.sh` stays clean.

BENCH_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "${BENCH_DIR}/.." && pwd)
RESULTS_DIR="${RESULTS_DIR:-${BENCH_DIR}/results}"
GATEWAY="${FLEXSTORE_GATEWAY_URL:-http://localhost:8080}"

B=$(printf '\033[1m'); R=$(printf '\033[0m')
CY=$(printf '\033[36m'); GR=$(printf '\033[32m'); YE=$(printf '\033[33m'); RD=$(printf '\033[31m')

say()  { printf '\n%s%s==> %s%s\n' "$B" "$CY" "$*" "$R"; }
info() { printf '    %s\n' "$*"; }
ok()   { printf '    %s✓%s %s\n' "$GR" "$R" "$*"; }
warn() { printf '    %s!%s %s\n' "$YE" "$R" "$*"; }
die()  { printf '\n%sFAILED:%s %s\n' "$RD" "$R" "$*" >&2; exit 1; }

compose() { docker compose --project-directory "$REPO_ROOT" "$@"; }

# ---------------------------------------------------------------------------
# Where the load generator runs
#
# CLIENT=network (default) runs the driver in a throwaway container attached to
# the compose network, talking to http://gateway:8080. CLIENT=host runs the
# native binary against the published port.
#
# The default is "network" because on macOS the published port goes through
# Docker Desktop's userspace forwarder, which caps throughput well below what
# the cluster can do. Benchmarking through it would measure Docker Desktop,
# not FlexStore.
# ---------------------------------------------------------------------------
CLIENT="${CLIENT:-network}"
GO_IMAGE="${GO_IMAGE:-golang:1.25}"
BENCH_NETWORK="${BENCH_NETWORK:-flexstore_default}"

# shellcheck disable=SC2034  # read by run.sh, which sources this file.
case "$CLIENT" in
  network) BENCH_GATEWAY="http://gateway:8080" ;;
  host)    BENCH_GATEWAY="$GATEWAY" ;;
  *) echo "CLIENT must be 'network' or 'host', got '$CLIENT'" >&2; exit 1 ;;
esac

# Host facts are collected once and handed to the driver, because a driver
# running inside a container cannot see them and must not guess.
host_env() {
  local dirty=false
  if [ -n "$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null)" ]; then
    dirty=true
  fi
  printf '%s\n' \
    "FLEXBENCH_CPU_MODEL=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -p)" \
    "FLEXBENCH_MEMORY_BYTES=$(sysctl -n hw.memsize 2>/dev/null || echo 0)" \
    "FLEXBENCH_OS_VERSION=$(sw_vers -productVersion 2>/dev/null | sed 's/^/macOS /' || uname -sr)" \
    "FLEXBENCH_DOCKER_VERSION=$(docker --version 2>/dev/null)" \
    "FLEXBENCH_GIT_COMMIT=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null)" \
    "FLEXBENCH_GIT_DIRTY=${dirty}"
}

BENCH_BIN="${REPO_ROOT}/bin/flexbench"
BENCH_IMAGE="flexstore/bench:dev"

build_driver() {
  say "Building the load driver (CLIENT=${CLIENT})"
  if [ "$CLIENT" = "network" ]; then
    docker build -q -t "$BENCH_IMAGE" -f "${BENCH_DIR}/Dockerfile" "$REPO_ROOT" >/dev/null \
      || die "could not build the bench image"
    ok "bench image ${BENCH_IMAGE} built"
  else
    # A native Go toolchain wins when present (CI has one, and so does any
    # machine with Go installed). The container is only the fallback for a
    # Docker-only host, where scripts/gotool.sh would emit Linux binaries --
    # hence the explicit cross-compile. -buildvcs=false in the container
    # because the bind-mounted repo is owned by a different uid than the
    # container's root, and git refuses to read it ("dubious ownership"); the
    # driver stamps its commit via FLEXBENCH_GIT_COMMIT, not the build info.
    if command -v go >/dev/null 2>&1; then
      (cd "$REPO_ROOT" && CGO_ENABLED=0 go build -trimpath -o bin/flexbench ./benchmarks/flexbench) \
        || die "could not build the native driver"
      ok "native bin/flexbench built with the host toolchain"
    else
      local goos goarch
      goos=$(uname -s | tr '[:upper:]' '[:lower:]')
      goarch=$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')
      docker run --rm \
        -v "${REPO_ROOT}:/src" \
        -v flexstore-gocache:/root/.cache/go-build \
        -v flexstore-gomodcache:/go/pkg/mod \
        -e "GOOS=${goos}" -e "GOARCH=${goarch}" -e CGO_ENABLED=0 \
        -w /src "$GO_IMAGE" go build -trimpath -buildvcs=false -o bin/flexbench ./benchmarks/flexbench \
        || die "could not cross-compile the native driver"
      ok "native bin/flexbench built for ${goos}/${goarch}"
    fi
  fi
}

# flexbench <args...> -- run the driver wherever CLIENT says, with the results
# directory writable either way.
flexbench() {
  if [ "$CLIENT" = "network" ]; then
    local envs=()
    while IFS= read -r line; do envs+=(-e "$line"); done < <(host_env)
    docker run --rm \
      --network "$BENCH_NETWORK" \
      -v "${RESULTS_DIR}:/results" \
      -e FLEXBENCH_CLIENT_LOCATION=compose-network \
      "${envs[@]}" \
      "$BENCH_IMAGE" "$@"
  else
    FLEXBENCH_CLIENT_LOCATION=host "$BENCH_BIN" "$@"
  fi
}

# wait_for_quiet -- block until no repair or reconciliation work is outstanding,
# so a benchmark never measures a cluster that is still healing from the
# previous one.
wait_for_quiet() {
  local deadline=$((SECONDS + 300))
  while [ $SECONDS -lt $deadline ]; do
    if curl -fsS "${GATEWAY}/admin/replication" 2>/dev/null | python3 -c '
import json, sys
d = json.load(sys.stdin)
busy = d["under_replicated_chunks"] + d["repair_jobs_pending"] + d["repair_jobs_running"]
sys.exit(0 if busy == 0 else 1)
'; then
      return 0
    fi
    sleep 2
  done
  warn "cluster still had repair work outstanding after 5 minutes; continuing"
}

require_cluster() {
  curl -fsS "${GATEWAY}/admin/health" >/dev/null 2>&1 \
    || die "no FlexStore cluster at ${GATEWAY} -- run 'make up' first"
}
