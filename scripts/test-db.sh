#!/usr/bin/env bash
# Starts (or stops) a throwaway PostgreSQL for the metadata package tests.
#
# Those tests exercise transactions, partial unique indexes, CHECK constraints
# and window functions -- none of which a mock would validate -- so they need a
# real server. It is a separate container from the Compose cluster so running
# the tests never disturbs a development cluster's data.
set -euo pipefail

NAME=${NAME:-flexstore-test-postgres}
PORT=${PORT:-55432}
IMAGE=${IMAGE:-postgres:16-alpine}

case "${1:-up}" in
  up)
    if [ "$(docker inspect -f '{{.State.Running}}' "${NAME}" 2>/dev/null || echo false)" = "true" ]; then
      echo "==> ${NAME} already running on port ${PORT}"
    else
      docker rm -f "${NAME}" >/dev/null 2>&1 || true
      echo "==> starting ${NAME} on port ${PORT}"
      docker run -d --rm \
        --name "${NAME}" \
        -e POSTGRES_USER=flexstore \
        -e POSTGRES_PASSWORD=flexstore \
        -e POSTGRES_DB=flexstore_test \
        -p "${PORT}:5432" \
        --tmpfs /var/lib/postgresql/data:rw \
        "${IMAGE}" \
        -c fsync=off -c full_page_writes=off >/dev/null
    fi

    echo -n "==> waiting for readiness"
    for _ in $(seq 1 60); do
      if docker exec "${NAME}" pg_isready -U flexstore -d flexstore_test >/dev/null 2>&1; then
        echo " ok"
        echo
        echo "export FLEXSTORE_TEST_POSTGRES_DSN='postgres://flexstore:flexstore@127.0.0.1:${PORT}/flexstore_test?sslmode=disable'"
        exit 0
      fi
      echo -n "."
      sleep 1
    done
    echo
    echo "postgres did not become ready" >&2
    docker logs "${NAME}" 2>&1 | tail -20 >&2
    exit 1
    ;;
  down)
    docker rm -f "${NAME}" >/dev/null 2>&1 || true
    echo "==> ${NAME} removed"
    ;;
  dsn)
    echo "postgres://flexstore:flexstore@127.0.0.1:${PORT}/flexstore_test?sslmode=disable"
    ;;
  *)
    echo "usage: $0 [up|down|dsn]" >&2
    exit 2
    ;;
esac
