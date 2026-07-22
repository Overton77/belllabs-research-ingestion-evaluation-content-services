#!/usr/bin/env bash
set -euo pipefail

project_root="$(git rev-parse --show-toplevel)"
meta_root="$(dirname "$project_root")/biotech-meta"

cd "$project_root"

if [[ ! -d "$meta_root/.git" ]]; then
    git clone https://github.com/Overton77/biotech-meta.git "$meta_root"
elif [[ "$(git -C "$meta_root" branch --show-current)" == "main" ]] \
    && git -C "$meta_root" diff --quiet \
    && git -C "$meta_root" diff --cached --quiet; then
    git -C "$meta_root" pull --ff-only origin main
fi

uv sync --frozen
docker compose config --quiet

echo "Cloud dependencies installed; Docker Compose configuration is valid."
