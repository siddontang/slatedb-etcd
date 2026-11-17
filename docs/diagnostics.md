# Diagnostic and fault APIs

These interfaces are trusted test controls. They bypass etcd Auth.
Keep them on loopback and restrict peer credentials to trusted operators.
With separate listeners, only the peer listener exposes these routes.

## Routes

| Method and path | Purpose |
| --- | --- |
| `POST /kv` | Replicated diagnostic operations in a separate keyspace |
| `POST /raft` | Internal Raft messages; use the service transport, not a manual request |
| `GET /status` | Local identity, role, apply progress, disk use, and memory counters |
| `POST /checkpoint` | Build and persist a pinned state snapshot |
| `GET /debug/value?key=KEY` | Read local diagnostic data without a quorum barrier |
| `POST /debug/partition` | Drop traffic to the listed peer IDs in both directions |
| `POST /debug/snapshot-delay` | Delay background snapshot construction |
| `POST /debug/apply-delay` | Delay entry application and snapshot installation |
| `POST /debug/log-fault` | Delay or fail the next log write |
| `POST /debug/log-read-fault` | Delay or fail a log read |

The delay controls accept `{"delay_ms":2500}`. Log fault controls also accept `"fail":true`.
Apply and log delays have a five-second limit. Zero delay and `"fail":false` clear those controls.
An injected log failure can stop the node. Use these endpoints only with disposable test data.

## Run a node manually

Run this command in three terminals. Set `NODE_ID` to 1, 2, and 3, respectively.

```sh
NODE_ID=1
target/release/slatedb-etcd \
  --id "$NODE_ID" --listen "127.0.0.1:700$NODE_ID" \
  --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 --peer 3=127.0.0.1:7003 \
  --dir "tmp/raft-poc/node-$NODE_ID" --cluster-id demo --snapshot-every 1000
```

Check each `/status` endpoint to find the leader. Send requests to that node:

```sh
curl http://127.0.0.1:7001/status
curl -H 'Content-Type: application/json' http://127.0.0.1:7001/kv \
  -d '{"operations":[{"op":"put","key":"hello","value":"world"}]}'
curl -H 'Content-Type: application/json' http://127.0.0.1:7001/kv \
  -d '{"operations":[{"op":"get","key":"hello"}]}'
curl -H 'Content-Type: application/json' http://127.0.0.1:7001/checkpoint -d '{}'
```

A follower returns HTTP 409 with `leader_id`. A timeout or leadership change can leave the outcome
unknown. The client must not interpret this response as proof that a request was never committed.
Requests have no retry deduplication guarantee.

`GET /debug/value?key=hello` reads a local replica without a consensus barrier. It is only a test
endpoint and can return stale data. `POST /debug/partition` accepts `{"blocked":[2,3]}` to drop peer
traffic in both the receive and send paths. Send `{"blocked":[]}` to restore traffic.
`POST /debug/snapshot-delay` accepts `{"delay_ms":2500}` for the background snapshot test.
