#!/bin/sh
# Starts the docker-compose cluster with a throwaway cluster key generated for
# this run. The key lives only in this process's environment and the containers'.
set -eu
cd "$(dirname "$0")/.."
RAFTKV_CLUSTER_KEY="$(openssl rand -hex 32)"
export RAFTKV_CLUSTER_KEY
exec docker compose up --build -d "$@"
