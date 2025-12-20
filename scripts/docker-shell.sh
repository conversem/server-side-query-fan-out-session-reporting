#!/bin/bash
# Start an interactive shell in the Docker container
# Usage: ./scripts/docker-shell.sh

set -e

cd "$(dirname "$0")/.."

echo "Starting Docker shell with gcloud CLI and Python..."
echo "  - gcloud config persisted in docker volume"
echo "  - credentials/ directory mounted for key files"
echo ""

docker compose run --rm app bash

