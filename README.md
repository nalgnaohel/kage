# Kage

Quietly but (hopefully) effectively, Kage is an attempt to build a mesage queue inspired by Kafka.

## Running

Build the broker binary:

```
go build -o kage ./cmd/kage
```

Kage is being built in phases (see `CLAUDE.md`). As of Phase 2 Round A, running it means starting one
broker with a `hashicorp/raft`-backed control plane — there's no multi-broker join yet (`-join-addr`
doesn't exist until Round B), so a single process is not a limited demo, it's the whole cluster for now.

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
| `-raft-addr` | `:9094` | raft transport bind address |
| `-raft-port` | `9094` | raft transport advertised port (paired with `-host`) |
| `-host` | `localhost` | this broker's advertised host |
| `-port` | `9093` | this broker's advertised kadmin port |
| `-cluster-id` | `kage-cluster` | cluster identifier |
| `-bootstrap` | `false` | bootstrap a new single-node raft cluster (only the first broker ever needs this) |

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