# Single multi-stage Dockerfile for all three FlexStore binaries.
#
# One image definition rather than three keeps the build cache shared and makes
# it impossible for the services to drift onto different base images or Go
# versions. The target binary is selected with --build-arg SERVICE.

# --- build stage -------------------------------------------------------------
# The same golang:1.25 image every other build in this repo uses (gotool.sh,
# the bench driver, the test runner), so one toolchain image serves everything.
# The output is a static binary either way (CGO_ENABLED=0).
FROM golang:1.25 AS build

ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /src

# Dependencies first: this layer only invalidates when go.mod/go.sum change,
# so ordinary source edits skip the module download entirely.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a static binary that runs on a scratch-like base.
# -trimpath keeps build paths out of the binary so builds are reproducible.
RUN test -n "$SERVICE" || (echo "SERVICE build-arg is required" >&2; exit 1) && \
    CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/flexstore ./cmd/${SERVICE}

# --- runtime stage -----------------------------------------------------------
FROM alpine:3.20 AS runtime

# ca-certificates for any future outbound TLS; wget is used by the compose
# healthchecks. Both are tiny and avoid baking curl into every image.
RUN apk add --no-cache ca-certificates wget && \
    addgroup -g 10001 -S flexstore && \
    adduser -u 10001 -S -G flexstore flexstore

COPY --from=build /out/flexstore /usr/local/bin/flexstore

# Storage nodes bind-mount a volume here; creating it up front with the right
# owner means the container does not need to run as root to write chunks.
RUN mkdir -p /data && chown flexstore:flexstore /data
VOLUME ["/data"]

USER flexstore:flexstore
ENTRYPOINT ["/usr/local/bin/flexstore"]
