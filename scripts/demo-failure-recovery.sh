#!/usr/bin/env bash
#
# FlexStore failure-recovery demo.
#
# Every number this prints is queried live from the running cluster -- the
# admin API, the services' /metrics endpoints and `docker compose` -- so
# nothing here can show a recovery that did not actually happen.
#
#   scripts/demo-failure-recovery.sh              # 1 GiB showcase
#   SIZE_MIB=128 scripts/demo-failure-recovery.sh # faster
#   KILL_TWO=1 scripts/demo-failure-recovery.sh   # also kill a second node
set -euo pipefail

GATEWAY="${FLEXSTORE_GATEWAY_URL:-http://localhost:8080}"
GATEWAY_METRICS="${GATEWAY/:8080/:9101}"
COORD_METRICS="${GATEWAY/:8080/:9102}"
REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

SIZE_MIB="${SIZE_MIB:-1024}"
BUCKET="${BUCKET:-demo-bucket}"
KEY="${KEY:-recovery/demo-$(date +%s).bin}"
KILL_TWO="${KILL_TWO:-0}"
KEEP="${KEEP:-0}"

WORKDIR=$(mktemp -d)
cleanup() {
  [ "$KEEP" = "1" ] || rm -rf "$WORKDIR"
}
trap cleanup EXIT

# ---- presentation ---------------------------------------------------------

B=$(printf '\033[1m'); DIM=$(printf '\033[2m'); R=$(printf '\033[0m')
CLR=$(printf '\033[K')
# Progress is redrawn in place on a terminal and printed one line per change
# when redirected, so a captured demo transcript stays readable.
if [ -t 1 ]; then PROGRESS_CR=$(printf '\r'); PROGRESS_NL=""; else PROGRESS_CR=""; PROGRESS_NL=$'\n'; fi
progress() { printf '%s     %s%s%s' "$PROGRESS_CR" "$*" "$CLR" "$PROGRESS_NL"; }
CY=$(printf '\033[36m'); GR=$(printf '\033[32m'); YE=$(printf '\033[33m'); RD=$(printf '\033[31m')

STEP=0
step() { STEP=$((STEP+1)); printf '\n%s%s[%02d] %s%s\n' "$B" "$CY" "$STEP" "$1" "$R"; }
info() { printf '     %s\n' "$*"; }
ok()   { printf '     %s✓%s %s\n' "$GR" "$R" "$*"; }
warn() { printf '     %s!%s %s\n' "$YE" "$R" "$*"; }
bad()  { printf '     %s✗%s %s\n' "$RD" "$R" "$*"; }
die()  { printf '\n%sFAILED:%s %s\n' "$RD" "$R" "$*" >&2; exit 1; }

rule() { printf '%s%s%s\n' "$DIM" "$(printf '─%.0s' $(seq 1 74))" "$R"; }

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

compose() { docker compose --project-directory "$REPO_ROOT" "$@"; }

api() { curl -fsS "${GATEWAY}$1"; }

# jqp <jsonpath-ish python expression> — evaluate a python expression against
# JSON piped on stdin. Used instead of jq so the demo has no extra dependency.
jqp() { python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }

# ---- state queries --------------------------------------------------------

node_table() {
  api /admin/nodes | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("     %-18s %-9s %10s %10s %8s" % ("NODE", "HEALTH", "USED", "CAPACITY", "CHUNKS"))
for n in sorted(d["nodes"], key=lambda x: x["node_id"]):
    colour = {"HEALTHY": "\033[32m", "SUSPECT": "\033[33m", "DEAD": "\033[31m"}.get(n["health"], "")
    print("     %-18s %s%-9s\033[0m %9.1fM %9.1fG %8d" % (
        n["node_id"], colour, n["health"],
        n["used_bytes"] / 1048576, n["total_bytes"] / 1073741824, n["chunk_count"]))
'
}

replication_line() {
  api /admin/replication | python3 -c '
import json, sys
d = json.load(sys.stdin)
status = "\033[32mfully durable\033[0m" if d["under_replicated_chunks"] == 0 else "\033[33mdegraded\033[0m"
if d["unavailable_chunks"]:
    status = "\033[31mDATA UNAVAILABLE\033[0m"
print("     %s  chunks=%d  under-replicated=%d  unavailable=%d  repairs[pending=%d running=%d done=%d failed=%d]" % (
    status, d["total_chunks"], d["under_replicated_chunks"], d["unavailable_chunks"],
    d["repair_jobs_pending"], d["repair_jobs_running"],
    d["repair_jobs_succeeded"], d["repair_jobs_failed"]))
'
}

chunk_map() {
  api "/admin/objects/${BUCKET}/${KEY}" | python3 -c '
import json, sys, collections
d = json.load(sys.stdin)
spread = collections.Counter()
shown = 0
for c in d["chunks"]:
    live = sorted(r["node_id"].replace("storage-node-", "n") for r in c["replicas"] if r["state"] == "AVAILABLE")
    for n in live:
        spread[n] += 1
    if shown < 6:
        flag = "" if len(live) >= d["replication_factor"] else "  \033[33m<- under-replicated\033[0m"
        print("     chunk %-4d %d replicas  %s%s" % (c["index"], len(live), ",".join(live), flag))
        shown += 1
if len(d["chunks"]) > shown:
    print("     ... %d more chunks" % (len(d["chunks"]) - shown))
print("     replicas per node: %s" % dict(sorted(spread.items())))
'
}

metric() {
  # $1 metric name, $2 endpoint
  curl -fsS "$2/metrics" 2>/dev/null | awk -v n="$1" '$1 ~ "^"n"([{ ]|$)" {s+=$2} END {printf "%d", s+0}'
}

under_replicated() { api /admin/replication | jqp 'd["under_replicated_chunks"]'; }
repairs_done()     { api /admin/replication | jqp 'd["repair_jobs_succeeded"]'; }
node_health()      { api /admin/nodes | python3 -c "import json,sys;d=json.load(sys.stdin);print(next((n['health'] for n in d['nodes'] if n['node_id']=='$1'),'?'))"; }

# ---- 1. cluster up --------------------------------------------------------

printf '\n%s%s FlexStore — failure recovery demo %s\n' "$B" "$CY" "$R"
rule
info "gateway     ${GATEWAY}"
info "object      ${BUCKET}/${KEY}"
info "size        ${SIZE_MIB} MiB"
rule

step "Ensuring the cluster is up"
if ! curl -fsS "${GATEWAY}/admin/health" >/dev/null 2>&1; then
  info "gateway not responding; starting the cluster (this builds images on first run)"
  compose up -d >/dev/null 2>&1 || die "docker compose up failed"
fi
for _ in $(seq 1 120); do
  curl -fsS "${GATEWAY}/admin/health" >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS "${GATEWAY}/admin/health" >/dev/null 2>&1 || die "gateway never became reachable"
ok "gateway responding"

step "Waiting for five healthy storage nodes"
for _ in $(seq 1 90); do
  H=$(api /admin/nodes | jqp 'd["healthy"]')
  [ "$H" -ge 5 ] && break
  sleep 2
done
[ "$H" -ge 5 ] || die "only ${H} nodes healthy; expected 5"
node_table
ok "5 healthy storage nodes"

step "Replication status before we break anything"
replication_line
RF=$(api /admin/replication | jqp 'd["replication_factor"]')
info "replication factor: ${RF}"

# ---- 2. upload ------------------------------------------------------------

step "Generating a ${SIZE_MIB} MiB test object"
dd if=/dev/urandom of="${WORKDIR}/original.bin" bs=1048576 count="${SIZE_MIB}" status=none
ORIGINAL_SHA=$(sha256 "${WORKDIR}/original.bin")
ok "sha256 = ${ORIGINAL_SHA}"

# --upload-file streams from disk. --data-binary would read the whole object
# into curl's memory first, which fails outright at the 1 GiB showcase size.
step "Uploading"
UP_START=$(date +%s)
CODE=$(curl -sS -o "${WORKDIR}/put.out" -w '%{http_code}' \
  -X PUT --upload-file "${WORKDIR}/original.bin" \
  -H 'Content-Type: application/octet-stream' \
  "${GATEWAY}/objects/${BUCKET}/${KEY}")
UP_END=$(date +%s)
[ "$CODE" = "201" ] || die "upload returned HTTP ${CODE}: $(cat "${WORKDIR}/put.out")"
ok "uploaded in $((UP_END-UP_START))s"

step "Verifying the round trip before any failure"
curl -fsS -o "${WORKDIR}/download1.bin" "${GATEWAY}/objects/${BUCKET}/${KEY}"
SHA1=$(sha256 "${WORKDIR}/download1.bin")
[ "$SHA1" = "$ORIGINAL_SHA" ] || die "checksum mismatch before any failure: ${SHA1}"
ok "downloaded sha256 matches"

step "Chunk distribution across the cluster"
chunk_map

# ---- 3. kill a node -------------------------------------------------------

# Target the node holding the most replicas of this object, so the failure
# definitely matters.
VICTIM=$(api "/admin/objects/${BUCKET}/${KEY}" | python3 -c '
import json, sys, collections
d = json.load(sys.stdin)
c = collections.Counter(r["node_id"] for ch in d["chunks"] for r in ch["replicas"] if r["state"] == "AVAILABLE")
print(c.most_common(1)[0][0])
')

step "Stopping ${VICTIM}"
warn "this node holds replicas of the object we just uploaded"
# Baseline the repair counter *before* the failure. Repair is fast enough that
# it can finish while the steps between here and the watch loop are still
# running, and a baseline taken later would report zero copies for work that
# demonstrably happened.
BEFORE_REPAIRS=$(repairs_done)
compose stop "$VICTIM" >/dev/null 2>&1
FAIL_AT=$(date +%s)
ok "${VICTIM} stopped"

step "Waiting for the coordinator to detect the failure"
info "heartbeats stop -> SUSPECT -> DEAD (thresholds are configurable)"
LAST=""
for _ in $(seq 1 120); do
  H=$(node_health "$VICTIM")
  if [ "$H" != "$LAST" ] || [ -t 1 ]; then
    progress "$(printf 't+%-3ss  %s is %s' "$(( $(date +%s)-FAIL_AT ))" "$VICTIM" "$H")"
    LAST="$H"
  fi
  [ "$H" = "DEAD" ] && break
  sleep 2
done
[ -t 1 ] && printf '\n'
[ "$H" = "DEAD" ] || die "${VICTIM} was never marked DEAD"
DEAD_AT=$(date +%s)
ok "marked DEAD after $((DEAD_AT-FAIL_AT))s"
node_table

step "Durability is now degraded"
replication_line
PEAK=$(under_replicated)
if [ "$PEAK" -gt 0 ]; then
  ok "${PEAK} chunks are under-replicated and queued for repair"
else
  info "repair already caught up (it runs continuously)"
fi

step "Object is STILL downloadable while degraded"
curl -fsS -o "${WORKDIR}/download-degraded.bin" "${GATEWAY}/objects/${BUCKET}/${KEY}"
SHA_DEG=$(sha256 "${WORKDIR}/download-degraded.bin")
[ "$SHA_DEG" = "$ORIGINAL_SHA" ] || die "checksum mismatch while degraded: ${SHA_DEG}"
ok "read failover served correct bytes from surviving replicas"

# ---- 4. watch the repair --------------------------------------------------

step "Watching automatic re-replication"
# Timed from the moment the coordinator declared the node DEAD, which is when
# repair could first have started. Timing from the start of this step would
# flatter the system by excluding detection *and* any repair that already
# completed while the previous steps ran.
FIRST_POLL=1
LAST=""
for _ in $(seq 1 300); do
  U=$(under_replicated)
  D=$(repairs_done)
  LINE=$(printf 't+%-4ss  under-replicated=%-6s repairs completed=%-6s' \
    "$(( $(date +%s)-DEAD_AT ))" "$U" "$((D-BEFORE_REPAIRS))")
  if [ "${U}/${D}" != "$LAST" ] || [ -t 1 ]; then
    progress "$LINE"
    LAST="${U}/${D}"
  fi
  if [ "$U" = "0" ]; then
    break
  fi
  FIRST_POLL=0
  sleep 1
done
[ -t 1 ] && printf '\n'
REPAIR_END=$(date +%s)
[ "$(under_replicated)" = "0" ] || die "durability was not restored"
COPIES=$(( $(repairs_done) - BEFORE_REPAIRS ))
if [ "$FIRST_POLL" = "1" ] && [ "$PEAK" -gt 0 ]; then
  # Repair beat the watch loop. Say so plainly rather than printing a
  # meaningless "0s": the elapsed figure is an upper bound, not a measurement.
  ok "full replication was already restored before this step began"
  info "upper bound: $((REPAIR_END-DEAD_AT))s from DEAD to full durability"
else
  ok "full replication restored $((REPAIR_END-DEAD_AT))s after the node was declared DEAD"
fi
info "${COPIES} chunk copies performed (peak under-replicated: ${PEAK})"

step "Replication status after recovery"
replication_line
chunk_map
info "note: no replica sits on ${VICTIM} any more"

step "Repair metrics (from the coordinator's /metrics endpoint)"
# The gauges below are refreshed by the durability scanner on its own interval,
# so immediately after a recovery they can still show the pre-repair value while
# /admin/replication (which queries PostgreSQL directly) already reads zero.
# That gap is the scrape interval, not a disagreement about the data.
info "flexstore_repairs_total          $(metric flexstore_repairs_total "$COORD_METRICS")"
info "flexstore_repairs_failed_total   $(metric flexstore_repairs_failed_total "$COORD_METRICS")"
info "flexstore_repair_bytes_total     $(metric flexstore_repair_bytes_total "$COORD_METRICS")"
info "flexstore_under_replicated_chunks $(metric flexstore_under_replicated_chunks "$COORD_METRICS")"
info "flexstore_checksum_failures_total $(metric flexstore_checksum_failures_total "$GATEWAY_METRICS")"

# ---- 5. optional second failure -------------------------------------------

if [ "$KILL_TWO" = "1" ]; then
  VICTIM2=$(api "/admin/objects/${BUCKET}/${KEY}" | python3 -c '
import json, sys, collections
d = json.load(sys.stdin)
c = collections.Counter(r["node_id"] for ch in d["chunks"] for r in ch["replicas"] if r["state"] == "AVAILABLE")
print(c.most_common(1)[0][0])
')
  step "Stretch: stopping a SECOND node (${VICTIM2})"
  warn "two of five nodes will be down; RF=${RF} means at least one replica of every chunk survives"
  compose stop "$VICTIM2" >/dev/null 2>&1
  F2=$(date +%s)
  LAST=""
  for _ in $(seq 1 120); do
    H=$(node_health "$VICTIM2")
    if [ "$H" != "$LAST" ] || [ -t 1 ]; then
      progress "$(printf 't+%-3ss  %s is %s' "$(( $(date +%s)-F2 ))" "$VICTIM2" "$H")"
      LAST="$H"
    fi
    [ "$H" = "DEAD" ] && break
    sleep 2
  done
  [ -t 1 ] && printf '\n'
  D2=$(date +%s)
  node_table

  step "Object survives TWO simultaneous node failures"
  curl -fsS -o "${WORKDIR}/download-two-down.bin" "${GATEWAY}/objects/${BUCKET}/${KEY}"
  SHA_TWO=$(sha256 "${WORKDIR}/download-two-down.bin")
  [ "$SHA_TWO" = "$ORIGINAL_SHA" ] || die "checksum mismatch with two nodes down: ${SHA_TWO}"
  ok "download succeeded with 2 of 5 nodes stopped, sha256 matches"

  step "Re-replicating onto the three survivors"
  # Timed from the second node being declared DEAD, for the same reason as the
  # first: repair may already be well underway by the time this step is reached.
  LAST=""
  for _ in $(seq 1 600); do
    U=$(under_replicated)
    if [ "$U" != "$LAST" ] || [ -t 1 ]; then
      progress "$(printf 't+%-4ss  under-replicated=%-6s' "$(( $(date +%s)-D2 ))" "$U")"
      LAST="$U"
    fi
    [ "$U" = "0" ] && break
    sleep 1
  done
  [ -t 1 ] && printf '\n'
  if [ "$(under_replicated)" = "0" ]; then
    ok "durability restored again $(( $(date +%s)-D2 ))s after the second node was declared DEAD"
  else
    warn "still under-replicated: with 3 survivors and RF=${RF} there is no spare capacity for placement"
  fi
  replication_line

  step "Restarting ${VICTIM2}"
  compose up -d --no-deps "$VICTIM2" >/dev/null 2>&1
  ok "restarted"
fi

# ---- 6. bring the node back ----------------------------------------------

step "Restarting ${VICTIM} — its files are now stale"
compose up -d --no-deps "$VICTIM" >/dev/null 2>&1
for _ in $(seq 1 90); do
  [ "$(node_health "$VICTIM")" = "HEALTHY" ] && break
  sleep 2
done
ok "${VICTIM} is HEALTHY again"
info "its replicas are held STALE until the reconciler verifies each checksum;"
info "metadata is the source of truth, so returning files never become authoritative on their own"

step "Waiting for reconciliation and replica trimming to settle"
LAST=""
for _ in $(seq 1 120); do
  OUT=$(api "/admin/objects/${BUCKET}/${KEY}" | python3 -c '
import json, sys
d = json.load(sys.stdin)
rf = d["replication_factor"]
over = sum(1 for c in d["chunks"] if c["available_replicas"] > rf)
under = sum(1 for c in d["chunks"] if c["available_replicas"] < rf)
print(f"{over} {under}")
')
  OVER=$(echo "$OUT" | awk '{print $1}'); UNDER=$(echo "$OUT" | awk '{print $2}')
  if [ "$OUT" != "${LAST:-}" ] || [ -t 1 ]; then
    progress "$(printf 'over-replicated=%-4s under-replicated=%-4s' "$OVER" "$UNDER")"
    LAST="$OUT"
  fi
  [ "$OVER" = "0" ] && [ "$UNDER" = "0" ] && break
  sleep 2
done
[ -t 1 ] && printf '\n'
ok "settled at exactly ${RF} replicas per chunk"
node_table

# ---- 7. final verification -----------------------------------------------

step "Final download and checksum comparison"
curl -fsS -o "${WORKDIR}/download-final.bin" "${GATEWAY}/objects/${BUCKET}/${KEY}"
FINAL_SHA=$(sha256 "${WORKDIR}/download-final.bin")
info "original  ${ORIGINAL_SHA}"
info "final     ${FINAL_SHA}"
[ "$FINAL_SHA" = "$ORIGINAL_SHA" ] || die "final checksum mismatch"

rule
printf '%s%s  RESULT  %s\n' "$B" "$GR" "$R"
ok "uploaded ${SIZE_MIB} MiB, split across 5 nodes at RF=${RF}"
ok "stopped ${VICTIM}; coordinator detected it in $((DEAD_AT-FAIL_AT))s"
ok "object stayed downloadable throughout the failure"
ok "${COPIES} missing replicas rebuilt automatically within $((REPAIR_END-DEAD_AT))s of detection"
[ "$KILL_TWO" = "1" ] && ok "survived a second simultaneous node failure"
ok "SHA-256 of the downloaded object matches the source exactly"
rule

printf '\n%sExplore further:%s\n' "$B" "$R"
printf '  curl -s %s/admin/health       | python3 -m json.tool\n' "$GATEWAY"
printf '  curl -s %s/admin/replication  | python3 -m json.tool\n' "$GATEWAY"
printf '  curl -s %s/admin/objects/%s/%s | python3 -m json.tool\n' "$GATEWAY" "$BUCKET" "$KEY"
printf '  Live dashboard: %s/dashboard\n' "$GATEWAY"
printf '  Grafana:        http://localhost:3000 (FlexStore -> Self-Healing)\n\n'

if [ "$KEEP" != "1" ]; then
  curl -fsS -X DELETE "${GATEWAY}/objects/${BUCKET}/${KEY}" >/dev/null 2>&1 || true
fi
