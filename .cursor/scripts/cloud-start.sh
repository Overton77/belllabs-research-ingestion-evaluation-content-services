#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if ! docker info >/dev/null 2>&1; then
    if ! sudo service docker start >/dev/null 2>&1; then
        sudo sh -c "nohup dockerd > /tmp/cursor-cloud-dockerd.log 2>&1 &"
    fi
fi

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

docker compose up --detach

running_services=(
    application-postgres
    redis
    temporal-postgres
    temporal
    temporal-ui
)
one_shot_services=(
    temporal-schema
    temporal-create-namespace
)

deadline=$((SECONDS + 300))
while ((SECONDS < deadline)); do
    ready=true

    for service in "${running_services[@]}"; do
        container_id="$(docker compose ps --quiet "$service")"
        if [[ -z "$container_id" ]]; then
            ready=false
            continue
        fi

        state="$(docker inspect \
            --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
            "$container_id")"
        if [[ "$state" != "healthy" && "$state" != "running" ]]; then
            ready=false
        fi
    done

    for service in "${one_shot_services[@]}"; do
        container_id="$(docker compose ps --all --quiet "$service")"
        if [[ -z "$container_id" ]]; then
            ready=false
            continue
        fi

        state="$(docker inspect --format '{{.State.Status}}' "$container_id")"
        if [[ "$state" == "exited" ]]; then
            exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$container_id")"
            if [[ "$exit_code" != "0" ]]; then
                echo "$service exited with code $exit_code." >&2
                docker compose logs "$service" >&2
                exit 1
            fi
        else
            ready=false
        fi
    done

    if [[ "$ready" == "true" ]]; then
        docker compose ps --all
        exit 0
    fi

    sleep 2
done

echo "Docker Compose services did not become ready within 300 seconds." >&2
docker compose ps --all
docker compose logs >&2
exit 1
