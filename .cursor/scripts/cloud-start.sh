#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

sudo service docker start

for _ in {1..30}; do
    if docker info >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ! docker info >/dev/null 2>&1; then
    echo "Docker daemon did not become ready within 30 seconds." >&2
    exit 1
fi

docker compose up --detach --wait --wait-timeout 300
docker compose ps --all
