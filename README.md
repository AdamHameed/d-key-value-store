# d-key-value-store

A small, runnable distributed key-value store MVP in Go. It starts a 3-node Raft cluster locally, exposes a gRPC API, replicates writes through a leader, persists Raft state, snapshots KV state, and includes a CLI plus a small load test.

This is a learning/demo database, not a production database.

## Architecture

- `cmd/node`: starts one database node.
- `cmd/client`: CLI for `put`, `get`, `delete`, and `status`.
- `internal/server`: gRPC API implementation.
- `internal/raftstore`: HashiCorp Raft setup, FSM, log application, snapshots.
- `internal/storage`: BadgerDB-backed embedded key-value engine.
- `proto/kv.proto`: protobuf service definition.
- `docker-compose.yml`: 3-node local cluster.

RocksDB is not used because it usually needs native library setup in the container. This MVP uses BadgerDB instead, which is pure Go and keeps `docker compose up --build` straightforward.

## How Raft Is Used

HashiCorp Raft provides leader election, log replication, quorum commits, and leader failover. Client writes become Raft log commands:

- `put key value`
- `delete key`

Only the leader accepts writes. If a follower receives a write, it returns the current leader gRPC address. The CLI follows that redirect when it can map container names to host ports.

Reads are intentionally simple: `Get` is served by the leader only.

## WAL And Snapshots

Raft logs and stable metadata are persisted under each node volume using Bolt-backed HashiCorp Raft stores:

- `/data/<node>/raft/logs.bolt`
- `/data/<node>/raft/stable.bolt`

These files are the persistent Raft WAL/log storage used for replay after restart.

Snapshots are written by Raft's file snapshot store under:

- `/data/<node>/snapshots`

The snapshot contains a serialized copy of the Badger key-value state. On restart, Raft restores the latest snapshot and replays later committed log entries.

BadgerDB stores the materialized key-value state under:

- `/data/<node>/badger`

## Run The Cluster

```bash
docker compose up --build
```

Wait until the logs show an elected leader. In another terminal:

```bash
go run ./cmd/client --addr localhost:5001 status
go run ./cmd/client --addr localhost:5002 status
go run ./cmd/client --addr localhost:5003 status
```

## Put, Get, Delete

The client can start against any node. If it reaches a follower, it follows the reported leader using the default host-port map.

```bash
go run ./cmd/client --addr localhost:5001 put foo bar
go run ./cmd/client --addr localhost:5002 get foo
go run ./cmd/client --addr localhost:5003 delete foo
go run ./cmd/client --addr localhost:5001 get foo
```

You can also run the client from inside the compose network:

```bash
docker compose exec node1 /kv-client --addr node1:50051 --peers node1=node1:50051,node2=node2:50051,node3=node3:50051 put foo bar
docker compose exec node2 /kv-client --addr node2:50051 --peers node1=node1:50051,node2=node2:50051,node3=node3:50051 get foo
```

## Kill The Leader And Observe Failover

Find the leader:

```bash
go run ./cmd/client --addr localhost:5001 status
go run ./cmd/client --addr localhost:5002 status
go run ./cmd/client --addr localhost:5003 status
```

If the leader is `node1`, kill it:

```bash
docker compose stop node1
```

Wait a few seconds, then check the surviving nodes:

```bash
go run ./cmd/client --addr localhost:5002 status
go run ./cmd/client --addr localhost:5003 status
```

Write through a surviving node:

```bash
go run ./cmd/client --addr localhost:5002 put after-failover still-works
go run ./cmd/client --addr localhost:5003 get after-failover
```

If a different node is leader, stop that service instead, then use either surviving node's host port.

## Restart And Verify Recovery

Start the stopped node again:

```bash
docker compose start node1
```

Give it a few seconds to rejoin and catch up:

```bash
go run ./cmd/client --addr localhost:5001 status
go run ./cmd/client --addr localhost:5001 get after-failover
```

Because compose uses named volumes, the node keeps its Raft logs, snapshots, and Badger state across container restarts.

To reset all data:

```bash
docker compose down -v
```

## Load Test

Run many put/get pairs and print approximate throughput:

```bash
go run ./scripts/loadtest.go --addr localhost:5001 --n 1000
```

Or via Make:

```bash
make load
```

## Development

```bash
go test ./...
go build ./cmd/node ./cmd/client
```

Regenerate protobuf bindings:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
PATH="$(go env GOPATH)/bin:$PATH" make proto
```

## Known Limitations

- No TLS or authentication.
- No dynamic cluster membership.
- Reads are leader-only; there are no linearizable follower reads.
- Follower writes return leader information instead of proxying.
- Conflict handling is last-write-wins by Raft log order.
- Snapshot format is a simple JSON map, good for demos but not large datasets.
- The load test is intentionally simple and single-process.

## Resume Bullets

- Built a 3-node replicated key-value database in Go using gRPC, protobuf, HashiCorp Raft, BadgerDB, Docker, and docker-compose.
- Implemented leader-only writes, follower leader redirects, Raft-backed log replication, failover handling, and quorum-based commit through HashiCorp Raft.
- Added persistent Raft log/stable stores, Badger-backed materialized state, file snapshots, and restart recovery from snapshots plus committed logs.
- Created a CLI and load test script to demonstrate Put/Get/Delete, leader status, failover, and approximate throughput.
