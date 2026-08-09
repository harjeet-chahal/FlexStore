#!/usr/bin/env sh
# Runs the Go toolchain inside a container so the repo builds identically on a
# machine that has Docker but no local Go (and in CI, where Go is native and
# this script is simply bypassed with GO=go).
#
# The Debian-based image is used rather than Alpine because `go test -race`
# requires cgo, and Alpine's image ships no C toolchain. Release images are
# still built from Alpine with CGO_ENABLED=0 -- see deploy/docker/Dockerfile.
set -eu

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
GO_IMAGE=${GO_IMAGE:-golang:1.25}

exec docker run --rm \
  -v "${REPO_ROOT}:/src" \
  -v flexstore-gocache:/root/.cache/go-build \
  -v flexstore-gomodcache:/go/pkg/mod \
  -e GOFLAGS="${GOFLAGS:-}" \
  -w /src \
  "${GO_IMAGE}" "$@"
