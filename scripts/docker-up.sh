#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Prefer an explicit JDK 21. Skip the pin when that directory is absent.
PIN="${TOWN_JAVA_HOME:-$HOME/.sdkman/candidates/java/21.0.10-amzn}"
if [[ -x "$PIN/bin/java" ]]; then
  export JAVA_HOME="$PIN"
  export PATH="$JAVA_HOME/bin:$PATH"
fi

echo "Building jar..."
mvn -B -ntp -DskipTests package

echo "Starting compose (3 nodes)..."
docker compose up --build -d

echo
echo "City map:  http://127.0.0.1:9551/city"
echo "Node 2:    http://127.0.0.1:9552/city"
echo "Node 3:    http://127.0.0.1:9553/city"
echo "Logs:      docker compose logs -f"
echo "Stop:      docker compose down"
