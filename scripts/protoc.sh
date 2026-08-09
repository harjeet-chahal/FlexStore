#!/usr/bin/env sh
# Regenerates Go bindings from proto/. Runs protoc + the two Go plugins inside
# a container so contributors do not have to install protoc locally.
#
# All three tools are PINNED, and they have to be: protoc and both plugins stamp
# their own versions into every generated file, so the version is part of the
# output. An unpinned distro package produces bindings that differ from the
# committed ones by nothing but a comment, and the CI drift check -- correctly --
# fails on it. These versions must match the ones in .github/workflows/ci.yml.
set -eu

REPO_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
GO_IMAGE=${GO_IMAGE:-golang:1.25}
PROTOC_VERSION=${PROTOC_VERSION:-31.1}
PROTOC_GEN_GO_VERSION=${PROTOC_GEN_GO_VERSION:-v1.36.11}
PROTOC_GEN_GO_GRPC_VERSION=${PROTOC_GEN_GO_GRPC_VERSION:-v1.5.1}

# protoc ships per-architecture archives; map the container's arch to their names.
case "$(uname -m)" in
  arm64|aarch64) PROTOC_ARCH=aarch_64 ;;
  *)             PROTOC_ARCH=x86_64 ;;
esac

docker run --rm \
  -v "${REPO_ROOT}:/src" \
  -v flexstore-gocache:/root/.cache/go-build \
  -v flexstore-gomodcache:/go/pkg/mod \
  -w /src \
  "${GO_IMAGE}" sh -euc "
    apt-get update >/dev/null && apt-get install -y --no-install-recommends unzip >/dev/null
    curl -sSLo /tmp/protoc.zip \
      'https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip'
    unzip -qo /tmp/protoc.zip -d /usr/local 'bin/protoc' 'include/*'
    chmod +x /usr/local/bin/protoc
    go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}
    export PATH=\$PATH:/go/bin
    mkdir -p gen
    protoc --version
    protoc -I proto \
      --go_out=. --go_opt=module=github.com/harjeetschahal/flexstore \
      --go-grpc_out=. --go-grpc_opt=module=github.com/harjeetschahal/flexstore \
      proto/flexstore/v1/*.proto
    gofmt -w gen
  "
