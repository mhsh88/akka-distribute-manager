#!/usr/bin/env bash
# Demo: kill one Pekko node → shards rebalance on survivors → bring node back.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NODE="${1:-node3}"
HOLD="${2:-25}"

case "$NODE" in
  node1|node2|node3) ;;
  *) echo "Usage: $0 [node2|node3] [hold_seconds]"; exit 1 ;;
esac

if [[ "$NODE" == "node1" ]]; then
  echo "Refuse killing seed node1 (cluster forms around it). Use node2 or node3."
  exit 1
fi

echo "== Node-fail demo (DeathWatch + passivation) =="
echo "1) Open http://127.0.0.1:9551/lab     (watch leave arcs, crown, sleep/wake)"
echo "   Also:   http://127.0.0.1:9551/viewer (shard cards / migrate flash)"
echo "2) Stopping $NODE for ${HOLD}s …"
docker compose stop "$NODE"
echo "   $NODE down → LAB banner DeathWatch leave; shards rehome; crown may move."
echo "   Cluster API: curl -s http://127.0.0.1:9551/api/cluster | python3 -m json.tool"
sleep "$HOLD"
echo "3) Starting $NODE …"
docker compose start "$NODE"
echo "   $NODE back → join/reachable + wake on new hosts."
echo "Done."
