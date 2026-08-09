# FlexStore developer entry points.
#
# Every Go command runs through $(GO). By default that is scripts/gotool.sh,
# which runs the toolchain in a container so the repo builds on a machine with
# Docker but no local Go. Set GO=go to use a native toolchain (CI does this).

SHELL := /bin/bash
.DEFAULT_GOAL := help

GO ?= ./scripts/gotool.sh go
COMPOSE ?= docker compose
BIN_DIR := bin
SERVICES := gateway coordinator storage-node
GATEWAY_URL ?= http://localhost:8080

# Integration tests talk to a real cluster, so they are behind a build tag.
# `make test` therefore stays fast and hermetic.
INTEGRATION_TAG := integration

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

## ---------------------------------------------------------------- build ----

.PHONY: build
build: ## Compile all three binaries into ./bin
	@mkdir -p $(BIN_DIR)
	@for svc in $(SERVICES); do \
		echo "==> building $$svc"; \
		$(GO) build -trimpath -o $(BIN_DIR)/$$svc ./cmd/$$svc || exit 1; \
	done
	@echo "==> binaries in $(BIN_DIR)/"

.PHONY: proto
proto: ## Regenerate Go bindings from proto/
	./scripts/protoc.sh

.PHONY: tidy
tidy: ## Sync go.mod / go.sum
	$(GO) mod tidy

## ----------------------------------------------------------------- test ----

.PHONY: test
test: ## Run unit tests with the race detector (metadata tests skip without a DB)
	$(GO) test -race -count=1 ./...

.PHONY: test-db
test-db: ## Start a throwaway PostgreSQL for the metadata tests
	./scripts/test-db.sh up

.PHONY: test-db-down
test-db-down: ## Stop the throwaway PostgreSQL
	./scripts/test-db.sh down

.PHONY: test-metadata
test-metadata: ## Run the metadata tests against the throwaway PostgreSQL
	@dsn=$$(./scripts/test-db.sh dsn); \
	echo "==> metadata tests against $$dsn"; \
	docker run --rm --network host \
		-v "$(CURDIR):/src" \
		-v flexstore-gocache:/root/.cache/go-build \
		-v flexstore-gomodcache:/go/pkg/mod \
		-e FLEXSTORE_TEST_POSTGRES_DSN="$$dsn" \
		-w /src golang:1.25 go test -race -count=1 ./internal/metadata/...

.PHONY: test-all
test-all: test-db test-metadata test ## Everything: unit tests plus metadata tests

.PHONY: integration-test
integration-test: ## Run the full integration suite against a running cluster (make up first)
	FLEXSTORE_GATEWAY_URL=$(GATEWAY_URL) ./scripts/integration-test.sh

.PHONY: integration-test-short
integration-test-short: ## Integration tests without the container-restart cases
	FLEXSTORE_GATEWAY_URL=$(GATEWAY_URL) ./scripts/integration-test.sh -short

.PHONY: e2e
e2e: up wait integration-test ## Bring the cluster up and run integration tests

## ----------------------------------------------------------------- lint ----

.PHONY: fmt
fmt: ## Format all Go source
	$(GO) fmt ./...
	./scripts/gotool.sh gofmt -s -w ./cmd ./internal ./tests ./migrations

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: vet ## Run golangci-lint (containerised) plus go vet
	@echo "==> gofmt check"
	@out=$$(./scripts/gotool.sh gofmt -l ./cmd ./internal ./tests ./migrations); \
		if [ -n "$$out" ]; then echo "not gofmt-clean:"; echo "$$out"; exit 1; fi
	@echo "==> golangci-lint"
	docker run --rm \
		-v "$(CURDIR):/src" \
		-v flexstore-gocache:/root/.cache/go-build \
		-v flexstore-golangci:/root/.cache/golangci-lint \
		-v flexstore-gomodcache:/go/pkg/mod \
		-w /src golangci/golangci-lint:v2.5.0-alpine \
		golangci-lint run --timeout=5m ./...

## ------------------------------------------------------------- cluster ----

.PHONY: up
up: ## Build images and start the full cluster
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the cluster (volumes preserved)
	$(COMPOSE) down --remove-orphans

.PHONY: reset
reset: ## Stop the cluster and delete ALL data (volumes included)
	$(COMPOSE) down -v --remove-orphans
	@echo "==> all volumes removed"

.PHONY: logs
logs: ## Tail logs from every service
	$(COMPOSE) logs -f --tail=100

.PHONY: ps
ps: ## Show cluster status
	$(COMPOSE) ps

.PHONY: wait
wait: ## Block until the gateway reports ready
	@echo "==> waiting for gateway readiness"
	@for i in $$(seq 1 90); do \
		if curl -fsS $(GATEWAY_URL:8080=9101)/readyz >/dev/null 2>&1 || \
		   curl -fsS http://localhost:9101/readyz >/dev/null 2>&1; then \
			echo "==> gateway ready"; exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "gateway did not become ready in time" >&2; \
	$(COMPOSE) ps; exit 1

.PHONY: cluster-status
cluster-status: ## Print node roster and durability counters
	@curl -fsS $(GATEWAY_URL)/admin/nodes | (jq . 2>/dev/null || cat)

## ---------------------------------------------------------------- demo ----

.PHONY: smoke
smoke: ## Upload, download and verify a 25 MiB file against a running cluster
	./scripts/smoke.sh $(GATEWAY_URL)

.PHONY: demo
demo: ## Failure-recovery demo: kill a node, watch it self-heal, verify SHA-256
	FLEXSTORE_GATEWAY_URL=$(GATEWAY_URL) ./scripts/demo-failure-recovery.sh

.PHONY: corrupt-chunk
corrupt-chunk: ## Corrupt a replica for testing (BUCKET=... KEY=... [NODE=...])
	FLEXSTORE_GATEWAY_URL=$(GATEWAY_URL) ./scripts/corrupt-chunk.sh \
		--bucket "$(BUCKET)" --key "$(KEY)" $(if $(NODE),--node $(NODE),)

.PHONY: replication-status
replication-status: ## Print durability and repair progress
	@curl -fsS $(GATEWAY_URL)/admin/replication | (jq . 2>/dev/null || python3 -m json.tool)

## ----------------------------------------------------------- benchmarks ----

.PHONY: benchmark
benchmark: ## Measure upload/download throughput and p95 latency (8 MiB and 128 MiB objects)
	./benchmarks/run.sh

.PHONY: benchmark-recovery
benchmark-recovery: ## Measure detection and repair after killing a node (3 trials)
	@mkdir -p bin benchmarks/results
	docker run --rm -v "$(CURDIR):/src" \
		-v flexstore-gocache:/root/.cache/go-build \
		-v flexstore-gomodcache:/go/pkg/mod \
		-e GOOS=$$(uname -s | tr '[:upper:]' '[:lower:]') \
		-e GOARCH=$$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/') \
		-e CGO_ENABLED=0 -w /src golang:1.25 \
		go build -trimpath -o bin/recoverybench ./benchmarks/recoverybench
	./bin/recoverybench -size $(RECOVERY_SIZE) -trials $(RECOVERY_TRIALS) \
		-out benchmarks/results/recovery.json

RECOVERY_SIZE ?= 512MiB
RECOVERY_TRIALS ?= 3

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf $(BIN_DIR) coverage.out
