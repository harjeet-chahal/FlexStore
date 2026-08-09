#!/usr/bin/env bash
#
# Run the FlexStore throughput benchmark against whichever cluster is up.
#
# Two representative object sizes rather than a matrix:
#
#   8 MiB    exactly one chunk -- the size the system is tuned around
#   128 MiB  sixteen chunks -- measures the streaming path and whether replica
#            fan-out actually overlaps
#
# Both run at concurrency 8 (a small application's worth of parallel clients).
# Operation counts are chosen so each cell moves roughly 0.5-2 GiB, enough
# samples for a meaningful p95.
#
#   benchmarks/run.sh                       # both sizes into results/
#   PROFILE=quick benchmarks/run.sh         # one small cell for smoke-checking
#   RESULTS_DIR=/tmp/x benchmarks/run.sh
set -euo pipefail
# shellcheck source=benchmarks/lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/lib.sh"

PROFILE="${PROFILE:-full}"
TAG="${TAG:-$(date -u +%Y%m%dT%H%M%SZ)}"
LABEL="${LABEL:-}"

# size:concurrency:ops
case "$PROFILE" in
  full)  CELLS=( "8MiB:8:80" "128MiB:8:16" ) ;;
  quick) CELLS=( "8MiB:8:16" ) ;;
  *) die "unknown PROFILE '$PROFILE' (expected full or quick)" ;;
esac

require_cluster
build_driver
mkdir -p "$RESULTS_DIR"

say "Throughput benchmark (${#CELLS[@]} cells, profile=${PROFILE}, tag=${TAG})"
NODES=$(curl -fsS "${GATEWAY}/admin/nodes" | python3 -c 'import json,sys;print(json.load(sys.stdin)["healthy"])')
RF=$(curl -fsS "${GATEWAY}/admin/replication" | python3 -c 'import json,sys;print(json.load(sys.stdin)["replication_factor"])')
info "cluster: ${NODES} healthy node(s), replication factor ${RF}"
info "client:  ${CLIENT} -> ${BENCH_GATEWAY}"

for cell in "${CELLS[@]}"; do
  IFS=: read -r SIZE CONC OPS <<<"$cell"
  NAME="${TAG}_n${NODES}_rf${RF}_${SIZE}_c${CONC}_${CLIENT}.json"
  say "size=${SIZE} concurrency=${CONC} ops=${OPS}"
  wait_for_quiet
  # The driver writes to /results inside the container and to RESULTS_DIR on the
  # host; both resolve to the same directory because it is bind-mounted.
  if [ "$CLIENT" = "network" ]; then OUT="/results/${NAME}"; else OUT="${RESULTS_DIR}/${NAME}"; fi
  flexbench \
    -gateway "$BENCH_GATEWAY" \
    -size "$SIZE" \
    -concurrency "$CONC" \
    -ops "$OPS" \
    -label "${LABEL:-nodes=${NODES} rf=${RF}}" \
    -out "$OUT"
done

say "Done"
ok "$(find "$RESULTS_DIR" -name "${TAG}_*.json" | wc -l | tr -d ' ') result files in ${RESULTS_DIR}"
