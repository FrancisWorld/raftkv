#!/bin/sh
# Runs a local 3-node cluster (ports 18081-18083) with a throwaway cluster key.
# Ctrl-C stops every node it started. Requires ./bin/raftkv (make build).
set -eu
cd "$(dirname "$0")/.."
work="$(mktemp -d)"
umask 077
openssl rand -hex 32 > "$work/cluster.key"
peers="1=http://127.0.0.1:18081,2=http://127.0.0.1:18082,3=http://127.0.0.1:18083"
pids=""
cleanup() { for p in $pids; do kill "$p" 2>/dev/null || true; done; rm -rf "$work"; }
trap cleanup INT TERM EXIT
for i in 1 2 3; do
  ./bin/raftkv --id "$i" --listen "127.0.0.1:1808$i" --peers "$peers" \
    --cluster-key-file "$work/cluster.key" --data-dir "$work/data" &
  pids="$pids $!"
done
echo "cluster up: raftctl --endpoints http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083 status"
wait
