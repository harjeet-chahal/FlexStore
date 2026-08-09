module github.com/harjeetschahal/flexstore

go 1.25.0

// Go 1.25 rather than 1.24, and the reason is security rather than features.
// govulncheck reported four advisories reachable from this code -- including
// GO-2026-5004, a pgx SQL-injection via placeholder confusion with
// dollar-quoted string literals, which migration 0002 uses for its PL/pgSQL
// trigger function. Every fixed version (pgx v5.9.2+, grpc v1.82.1+,
// x/text v0.39+) requires a 1.25 toolchain, so staying on 1.24 meant staying
// vulnerable. Taking the floor bump was the cheaper side of that trade.
require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.9.2
	github.com/prometheus/client_golang v1.20.5
	github.com/redis/go-redis/v9 v9.7.3
	golang.org/x/sys v0.43.0
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	golang.org/x/net v0.53.0 // indirect
	golang.org/x/sync v0.21.0 // indirect
	golang.org/x/text v0.39.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
)
