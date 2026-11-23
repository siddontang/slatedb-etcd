# Object storage Raft snapshots

## Problem

A Raft snapshot was a full logical image. The leader scanned every SlateDB row into memory. It
sent the image in one HTTP request. The follower deleted every row and wrote the image again.
Memory, request size, and install time grew with the data. The limit was 64 MiB.

SlateDB already keeps its state as immutable SST objects and manifests. A snapshot can move these
objects instead of rows.

## Decision

The leader creates a SlateDB checkpoint. The Raft snapshot carries a small descriptor of that
checkpoint. The follower copies the checkpoint objects into its own namespace. It then opens the
copy as a new SlateDB database and switches to it.

The follower copies SST objects. It does not reference them in the leader namespace. Each member
keeps sole ownership of its prefix, its manifests, and its garbage collection. No member writes to
another member prefix. A SlateDB clone would reference the parent SSTs until compaction rewrote
them. That couples member lifetimes for an unbounded time and writes to the parent manifest.

## Descriptor

The Raft snapshot data starts with format 5. It holds these fields:

| Field | Purpose |
| --- | --- |
| `applied` | The Raft index of the checkpoint. It must equal the Raft snapshot index. |
| `member`, `instance` | The source member and its process instance. |
| `backend` | The source backend identity: local, or S3 bucket, endpoint, and region. |
| `peer_url` | The source peer URL for an HTTP copy. |
| `root` | The source database path in its object store. |
| `checkpoint`, `manifest` | The SlateDB checkpoint ID and its manifest ID. |
| `ssts` | The SST paths of that manifest, relative to `root`. |
| `members` | The membership store at `applied`. |
| `created_ms` | The build time. It bounds reuse of the descriptor. |

The descriptor size grows with the SST count, not with the data size.

## Leader

1. Raft asks for a snapshot. The node builds an image only when a peer needs one, as before.
2. The apply worker calls `Db::begin_checkpoint(CheckpointScope::All)` between two apply jobs.
   The checkpoint therefore contains exactly the applied prefix. Its sequence equals `applied`.
3. A background task waits for the checkpoint. It reads the manifest and lists its SST paths.
4. The descriptor goes through the existing checkpoint file and Raft Engine publication.

The checkpoint has a one-hour lifetime. The node reuses a descriptor for 20 minutes. After that,
the descriptor no longer counts as a transferable image. The next peer request builds a new
checkpoint. SlateDB removes expired checkpoints and their unreferenced SSTs. The leader needs no
other cleanup, and a follower never extends a leader checkpoint.

A received descriptor is not transferable. A new leader builds its own checkpoint first.

## Transfer

The leader sends the descriptor in the `MsgSnapshot` HTTP request, as before. The request runs
in its own task, so heartbeats continue during a long copy. The descriptor names the peer URL of
the applied membership. The follower copies the objects before it passes the message to Raft.
A copy error fails the request. The leader then reports the snapshot as failed, and Raft retries
later. Raft does not accept a snapshot whose objects are missing.

The follower selects the source in this order:

1. The same S3 bucket, endpoint, and region: a server-side copy from the leader prefix.
2. A source in the same process: a direct object read. Tests and embedded clusters use this path.
3. Otherwise: an HTTP read from the leader `GET /raft/snapshot-file` endpoint.

A failed direct read falls back to HTTP. Local storage always uses HTTP between hosts.

### Cross-prefix read access

A server-side copy needs read access to the other member prefixes of the same cluster:

```text
s3:GetObject on <prefix>/<sha256(cluster-id)>/member-*/state*/manifest/*
s3:GetObject on <prefix>/<sha256(cluster-id)>/member-*/state*/compacted/*
```

Write, list, and delete access stays on the own member prefix. Without the read grant, the copy
uses HTTP through the leader. The data then passes through both members.

## Follower staging

The follower database path changes on each install:

```text
<member prefix>/
    state.current      the name of the current database, for example "state-3"
    state.namespace    the random ID of this database family
    state-3/           the current database
    state-4/           the staging database for the next install
    state-4.ready      the checkpoint ID that state-4 holds completely
    state-4.owner      the namespace ID: this member created state-4 and can delete it
```

`state` without a number is the first database. A missing `state.current` object selects it.
The pointer and the ID belong to the first database name. A sibling prefix such as `state-5`
therefore opens as an independent family with its own pointer.

1. If an owned `state-N.ready` names the descriptor checkpoint, the staging copy is complete.
2. Otherwise the follower selects an owned staging database. It does not select a database whose
   checkpoint Raft accepted but the node has not installed. It then deletes the ready marker and
   every staging object outside `compacted/`.

   One lock covers this selection and the node admission of a snapshot. The node reserves a
   complete staging copy for Raft under the lock. A new copy claims a staging database under the
   same lock and removes it from the complete set. A claimed copy therefore cannot be reserved,
   and a reserved copy cannot be claimed. If Raft skips the snapshot, the node releases the
   reservation at once.
3. It copies each missing SST. An SST ID is unique, so an existing SST is already complete.
4. It copies the manifest. It removes the copied checkpoints from the copy with SlateDB `Admin`.
5. It writes the ready marker.

A retry with a newer checkpoint keeps the SSTs it already copied. A large transfer therefore
progresses across retries. SlateDB garbage collection later removes SSTs that the final manifest
does not reference.

## Install

The apply worker installs a snapshot after Raft persists it:

1. It prepares staging again if the ready marker is missing. This path also serves DST.
2. It opens the staging database. The apply marker must equal the snapshot index.
3. It writes `state.current` with one atomic object PUT.
4. It replaces the state machine contents. It keeps metrics, the row marker, and Watch capture.
5. The node rebinds the compaction flush and the manifest metrics to the new database.
6. A background task closes the old database after its readers finish, then deletes its prefix.

A crash before step 3 opens the old database. Its apply marker is below the Raft snapshot index,
so the node installs the snapshot again. A crash after step 3 opens the new database. Startup
deletes owned databases older than the current database. A prefix without an owner marker stays
intact, because another database can use a similar name. The delete removes the owner marker
last, so an interrupted delete continues after a restart.

Step 6 waits until no read view holds the old database. A pinned read therefore never fails
because of an install.

The descriptor membership is kept in memory. The next write batch also stores it, because a
bootstrap membership exists only in memory until then.

## Limits

- The legacy image format stays readable. A node never builds it for Raft any more.
- Native backups and `Maintenance.Snapshot` still build a logical image. The 64 MiB limit stays.
- The HTTP copy reads one object at a time per request. It uses eight parallel copies.
- A copy slower than the checkpoint lifetime fails. The next retry continues from copied SSTs.
- Old binaries cannot install format 5. Upgrade followers first, as for earlier format changes.
