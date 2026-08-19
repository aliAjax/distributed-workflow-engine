#!/usr/bin/env sh
set -eu

DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$DIR"

if ! docker compose ps --status running postgres >/dev/null 2>&1; then
  docker compose up -d postgres redis
fi

go run ./cmd/engine -config configs/config.yaml
