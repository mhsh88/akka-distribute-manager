# akka-distribute-manager

Local town-power cluster. Java 21, Apache Pekko 1.1. About 300 houses on 3 nodes. Meters feed sharded house actors. A cluster singleton trips a district on overload and the blackout can cascade. In-memory only. No database, no auth.

Design notes: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

Pekko is Apache-2.0. This repo is MIT, see `LICENSE`.

## Three JVMs on this machine

```bash
mvn -B -ntp package
python3 scripts/local-cluster.py start
```

City map: http://127.0.0.1:9551/city

Shard viewer: http://127.0.0.1:9551/viewer

```bash
python3 scripts/local-cluster.py status
python3 scripts/local-cluster.py stop
```

`java` on `PATH` must be 21. Remoting ports 2551–2553 and HTTP ports 9551–9553 must be free.

## Docker

```bash
mvn -B -ntp package
docker compose up --build -d
```

| node | city map |
|---|---|
| node1 | http://127.0.0.1:9551/city |
| node2 | http://127.0.0.1:9552/city |
| node3 | http://127.0.0.1:9553/city |

`scripts/docker-up.sh` packages, then starts compose. It uses `TOWN_JAVA_HOME` when that JDK exists, otherwise `java` on `PATH`.

```bash
docker compose logs -f
docker compose down
```

With the compose stack up:

```bash
scripts/node-fail-demo.sh node3
scripts/partition-demo.sh node3
```

Lab view: http://127.0.0.1:9551/lab
