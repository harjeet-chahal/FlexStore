#!/usr/bin/env bash
#
# Deliberately corrupt a chunk replica, to prove FlexStore detects it.
#
# WHY THIS IS A SCRIPT AND NOT AN API ENDPOINT
# --------------------------------------------
# A "corrupt this chunk" endpoint would be a data-destruction primitive living
# inside the production binary, one config flag away from being reachable. The
# usual mitigations (a debug build tag, an env guard) all still ship the code.
#
# This script instead corrupts bytes from *outside* the system, through
# `docker compose exec` on the storage container. It requires shell access to
# the host running the cluster -- anyone with that can already do far worse --
# and there is no code path inside FlexStore that can be enabled to reach it.
# The server therefore has no self-corruption capability at all, which is a
# stronger guarantee than a well-guarded one.
#
# Usage:
#   scripts/corrupt-chunk.sh --bucket B --key K                # first chunk, first replica
#   scripts/corrupt-chunk.sh --bucket B --key K --index 2      # a specific chunk
#   scripts/corrupt-chunk.sh --bucket B --key K --node storage-node-3
#   scripts/corrupt-chunk.sh --bucket B --key K --all-replicas # every copy (read must fail)
#   scripts/corrupt-chunk.sh --bucket B --key K --mode truncate
#
# Modes:
#   overwrite  (default) replace leading bytes in place; size is preserved, so
#              only the checksum detects it
#   truncate   shorten the file; detected by both the size and the digest
set -euo pipefail

GATEWAY="${FLEXSTORE_GATEWAY_URL:-http://localhost:8080}"
REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

BUCKET=""; KEY=""; INDEX=0; NODE=""; ALL=0; MODE="overwrite"; QUIET=0

while [ $# -gt 0 ]; do
  case "$1" in
    --bucket) BUCKET="$2"; shift 2 ;;
    --key) KEY="$2"; shift 2 ;;
    --index) INDEX="$2"; shift 2 ;;
    --node) NODE="$2"; shift 2 ;;
    --all-replicas) ALL=1; shift ;;
    --mode) MODE="$2"; shift 2 ;;
    --quiet) QUIET=1; shift ;;
    -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$BUCKET" ] || [ -z "$KEY" ]; then
  echo "--bucket and --key are required" >&2
  exit 2
fi

say() { [ "$QUIET" = "1" ] || printf '\033[36m==>\033[0m %s\n' "$*"; }
die() { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# Resolve the chunk and its replica locations from the admin API. Deriving the
# path from live metadata (rather than hard-coding it) means this script keeps
# working if the on-disk layout ever changes.
LAYOUT=$(curl -fsS "${GATEWAY}/admin/objects/${BUCKET}/${KEY}") \
  || die "cannot read layout for ${BUCKET}/${KEY} from ${GATEWAY}"

read -r CHUNK_ID NODES <<EOF
$(printf '%s' "$LAYOUT" | python3 -c '
import json, sys
layout = json.load(sys.stdin)
index = int(sys.argv[1])
want_node = sys.argv[2]

match = next((c for c in layout["chunks"] if c["index"] == index), None)
if match is None:
    sys.exit(f"object has no chunk with index {index}")

live = [r["node_id"] for r in match["replicas"] if r["state"] == "AVAILABLE"]
if want_node:
    if want_node not in live:
        sys.exit(f"chunk {index} has no AVAILABLE replica on {want_node} (live: {live})")
    live = [want_node]
if not live:
    sys.exit(f"chunk {index} has no AVAILABLE replica to corrupt")

print(match["chunk_id"], " ".join(live))
' "$INDEX" "$NODE")
EOF

[ -n "${CHUNK_ID:-}" ] || die "could not resolve a chunk to corrupt"

if [ "$ALL" = "0" ]; then
  NODES=$(printf '%s' "$NODES" | awk '{print $1}')
fi

# Mirrors internal/storage.RelativePath: two levels of two hex characters taken
# from the dash-stripped UUID.
FLAT=$(printf '%s' "$CHUNK_ID" | tr -d '-')
PATH_ON_NODE="/data/data/${FLAT:0:2}/${FLAT:2:2}/${CHUNK_ID}.chunk"

say "object      ${BUCKET}/${KEY}"
say "chunk       ${CHUNK_ID} (index ${INDEX})"
say "path        ${PATH_ON_NODE}"
say "mode        ${MODE}"

for node in $NODES; do
  # Verify the file is where metadata says before mutating anything, so a
  # layout change turns into a loud failure rather than a silent no-op.
  docker compose --project-directory "$REPO_ROOT" exec -T "$node" test -f "$PATH_ON_NODE" \
    || die "chunk file not found on ${node} at ${PATH_ON_NODE}"

  case "$MODE" in
    overwrite)
      # conv=notrunc keeps the file length identical, so the size check passes
      # and only the SHA-256 catches it -- the purest test of checksumming.
      docker compose --project-directory "$REPO_ROOT" exec -T "$node" sh -c \
        "printf 'CORRUPTED-BY-FLEXSTORE-TEST' | dd of='${PATH_ON_NODE}' bs=1 seek=0 conv=notrunc status=none"
      ;;
    truncate)
      docker compose --project-directory "$REPO_ROOT" exec -T "$node" sh -c \
        ": > '${PATH_ON_NODE}'"
      ;;
    *) die "unknown mode ${MODE} (want overwrite or truncate)" ;;
  esac
  say "corrupted replica on ${node}"
done

printf '\033[32mdone\033[0m — chunk %s corrupted on: %s\n' "$CHUNK_ID" "$(echo "$NODES" | tr '\n' ' ')"
printf '     GET the object to watch detection, failover and repair:\n'
printf '       curl -s -o /dev/null -w "%%{http_code}\\n" %s/objects/%s/%s\n' "$GATEWAY" "$BUCKET" "$KEY"
printf '       curl -s %s/admin/replication | python3 -m json.tool\n' "$GATEWAY"
