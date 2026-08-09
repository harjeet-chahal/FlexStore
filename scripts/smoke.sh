#!/usr/bin/env bash
# End-to-end smoke test against a running FlexStore cluster.
#
# Uploads a multi-chunk file, downloads it back, compares SHA-256, and asserts
# that every chunk really has replicas on distinct storage nodes. Exits
# non-zero on any mismatch so it is usable as a CI gate.
set -euo pipefail

GATEWAY="${1:-http://localhost:8080}"
BUCKET="${BUCKET:-smoke-bucket}"
KEY="${KEY:-smoke/$(date +%s)-payload.bin}"
SIZE_MIB="${SIZE_MIB:-25}"

WORKDIR=$(mktemp -d)
trap 'rm -rf "${WORKDIR}"' EXIT

say() { printf '\033[36m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[31mFAIL:\033[0m %s\n' "$*" >&2; exit 1; }

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}';
  else shasum -a 256 "$1" | awk '{print $1}'; fi
}

say "generating ${SIZE_MIB} MiB of random data"
dd if=/dev/urandom of="${WORKDIR}/original.bin" bs=1048576 count="${SIZE_MIB}" status=none
ORIGINAL_SHA=$(sha256 "${WORKDIR}/original.bin")
say "original sha256 = ${ORIGINAL_SHA}"

say "uploading to ${GATEWAY}/objects/${BUCKET}/${KEY}"
PUT_STATUS=$(curl -sS -o "${WORKDIR}/put.out" -w '%{http_code}' \
  -X PUT --upload-file "${WORKDIR}/original.bin" \
  -H 'Content-Type: application/octet-stream' \
  "${GATEWAY}/objects/${BUCKET}/${KEY}")
[ "${PUT_STATUS}" = "201" ] || fail "upload returned HTTP ${PUT_STATUS}: $(cat "${WORKDIR}/put.out")"

say "HEAD"
curl -fsS -I "${GATEWAY}/objects/${BUCKET}/${KEY}" | sed 's/^/    /'

say "downloading"
curl -fsS -o "${WORKDIR}/downloaded.bin" "${GATEWAY}/objects/${BUCKET}/${KEY}"
DOWNLOAD_SHA=$(sha256 "${WORKDIR}/downloaded.bin")
say "downloaded sha256 = ${DOWNLOAD_SHA}"

[ "${ORIGINAL_SHA}" = "${DOWNLOAD_SHA}" ] || fail "checksum mismatch: ${ORIGINAL_SHA} != ${DOWNLOAD_SHA}"
say "checksums match"

say "verifying replica placement"
curl -fsS "${GATEWAY}/admin/objects/${BUCKET}/${KEY}" > "${WORKDIR}/layout.json"
if command -v python3 >/dev/null 2>&1; then
  python3 - "${WORKDIR}/layout.json" <<'PY'
import json, sys, collections
layout = json.load(open(sys.argv[1]))
chunks = layout["chunks"]
if not chunks:
    sys.exit("no chunks in layout")
problems = []
for c in chunks:
    nodes = [r["node_id"] for r in c["replicas"] if r["state"] == "AVAILABLE"]
    if len(set(nodes)) != len(nodes):
        problems.append(f"chunk {c['index']} has duplicate nodes: {nodes}")
    if len(nodes) < 2:
        problems.append(f"chunk {c['index']} has only {len(nodes)} available replicas")
spread = collections.Counter(n for c in chunks for n in
                             (r["node_id"] for r in c["replicas"] if r["state"] == "AVAILABLE"))
print(f"    {len(chunks)} chunks, replicas per node: {dict(spread)}")
if problems:
    sys.exit("\n".join(problems))
print("    every chunk has >=2 distinct available replicas")
PY
else
  echo "    python3 unavailable; skipping structural assertions"
  head -c 400 "${WORKDIR}/layout.json"; echo
fi

say "deleting"
DEL_STATUS=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "${GATEWAY}/objects/${BUCKET}/${KEY}")
[ "${DEL_STATUS}" = "204" ] || fail "delete returned HTTP ${DEL_STATUS}"

GET_STATUS=$(curl -sS -o /dev/null -w '%{http_code}' "${GATEWAY}/objects/${BUCKET}/${KEY}")
[ "${GET_STATUS}" = "404" ] || fail "expected 404 after delete, got ${GET_STATUS}"

printf '\033[32mPASS\033[0m smoke test complete\n'
