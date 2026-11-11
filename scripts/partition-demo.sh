#!/usr/bin/env bash
# Demo: partition one node off town-net (not kill) → unreachable → heal.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NODE="${1:-node3}"
HOLD="${2:-20}"

case "$NODE" in
  node1|node2|node3) ;;
  *) echo "Usage: $0 [node2|node3] [hold_seconds]"; exit 1 ;;
esac

if [[ "$NODE" == "node1" ]]; then
  echo "Refuse partitioning seed node1. Use node2 or node3."
  exit 1
fi

CID="$(docker compose ps -q "$NODE")"
if [[ -z "$CID" ]]; then
  echo "Container for $NODE not running."
  exit 1
fi

NET="$(docker inspect "$CID" --format '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"
if [[ -z "$NET" ]]; then
  echo "No docker network on $NODE"
  exit 1
fi

echo "== Partition demo =="
echo "1) Open http://127.0.0.1:9551/lab  (watch unreachable / leave / heat)"
echo "2) Disconnect $NODE from $NET for ${HOLD}s …"
docker network disconnect "$NET" "$CID"
echo "   Partitioned. LAB should show unreachable (then leave if downed)."
echo "   Cluster: curl -s http://127.0.0.1:9551/api/cluster"
sleep "$HOLD"
echo "3) Reconnect $NODE …"
docker network connect "$NET" "$CID"
echo "   Healed. Wait ~10s for reachable/join + rehome wakes."
echo "Done."
