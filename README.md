# Kage

Quietly but (hopefully) effectively, Kage is an attempt to build a mesage queue inspired by Kafka.

## Running

Build the broker binary:

```
go build -o kage ./cmd/kage
```

Kage is being built in phases (see `CLAUDE.md`). Phase 1 (storage + raw-TCP data plane) and Phase 2
(Raft metadata control plane, including multi-broker join and redirect-to-leader) are both done. Phase 3
(ISR data replication) is in progress: followers actually pull data from the partition leader and the
leader tracks a real in-sync-replica set, but leader failover and epoch fencing aren't implemented yet.

Start a broker, bootstrapping a new single-node raft cluster:

```
./kage -data-dir ./data -bootstrap -broker-id 1
```

Every other flag has a working default:

| Flag | Default | Meaning |
|---|---|---|
| `-data-dir` | `data` | base directory for partition logs and the raft log/snapshot store |
| `-grpc-addr` | `:9093` | kadmin gRPC control-plane listen address |
| `-raw-addr` | `:9092` | raw-TCP data-plane listen address (`Fetch`/`Produce`) |
| `-raw-port` | `9092` | this broker's advertised raw data-plane port (paired with `-host`) |
| `-raft-addr` | `:9094` | raft transport bind address |
| `-raft-port` | `9094` | raft transport advertised port (paired with `-host`) |
| `-host` | `localhost` | this broker's advertised host |
| `-port` | `9093` | this broker's advertised kadmin port |
| `-broker-id` | `0` | this broker's ID — must be unique per broker in the cluster |
| `-cluster-id` | `kage-cluster` | cluster identifier |
| `-bootstrap` | `false` | bootstrap a new single-node raft cluster (only the first broker ever needs this) |
| `-join-addr` | `""` | an existing broker's kadmin gRPC address to join the raft cluster through |

### Multi-broker cluster

`-bootstrap` and `-join-addr` are mutually exclusive: the first broker bootstraps, every other broker
joins through it (or through any broker already in the cluster — `JoinCluster` redirects to the leader
automatically if needed).

```
./kage -data-dir ./data1 -bootstrap -broker-id 1
./kage -data-dir ./data2 -broker-id 2 -port 9193 -raw-addr :9192 -raw-port 9192 -raft-addr :9194 -raft-port 9194 -join-addr localhost:9093
```

Topics created afterwards are round-robin-assigned across every registered broker, and each broker's
`api/rawdata` server pulls (as a replica) whatever partitions it owns but doesn't lead.

On a successful start you'll see the broker win its own single-node election and register itself in
cluster metadata:

```
raft: entering leader state
raft: registered broker 1 (localhost:9093, raft localhost:9094) in cluster metadata
kadmin gRPC server listening on :9093
raw data-plane server listening on :9092
```

### Talking to it

There's no `grpcurl` dependency baked in, so use `cmd/manualverify` — it exercises the whole path
(`CreateTopic` over kadmin gRPC, then `Produce`/`Fetch` over the raw TCP data plane) against the
default addresses above:

```
go run ./cmd/manualverify
```

### Verifying durability

Metadata (topics, broker registration) is now Raft-backed, not the in-memory bookkeeping Phase 1 had.
Kill the broker and restart it with the same `-bootstrap -broker-id 1` flags — topics created before
the restart are still there, replayed from `<data-dir>/raft/raft.db` instead of reset to empty. Message
data (partition logs under `<data-dir>`) was already durable in Phase 1 and is unaffected by any of
this.