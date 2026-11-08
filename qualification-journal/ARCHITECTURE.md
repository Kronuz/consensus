# Architecture

`journal` stores bounded opaque batches durably, publishes checkpoint generations, and
reclaims what they replace. It has no Raft, network, or application dependency:
`consensus` encodes what goes into a batch or a checkpoint, and `completion` supplies the
I/O backends. Two backends sit behind one `Store` facade:

- **The per-group journal** (`journal.h`): one directory per store, a manifest that names
  the durable frontier, and immutable checkpoint artifacts.
- **The shared turn log** (`turn_log.h`, `turn_driver.h`): one log per core, carrying
  every group on that core, one durability barrier per turn for all of them.

Measurements and qualification records are in [docs/MEASUREMENTS.md](docs/MEASUREMENTS.md).

## The per-group journal

### Contract

One serialized executor owns a journal. `append_batch()` returns after the append's final
durability barrier:

```text
Append complete batch → sync journal → create/write temporary manifest
→ sync manifest → replace manifest → sync directory → return frontier
```

The frontier names the storage identity, generation, byte offset, and contiguous batch
sequence. Batches carry checksums for framing and payload; native structs never reach
the disk. CRC-32C detects accidental corruption and does not authenticate data. The batch
bound defaults to 64 MiB, and admission rejects an oversized batch before storage
changes. The `IO` instance outlives the journal and every builder, prepared handle, and
reader.

`create(identity)` starts a new store in a dedicated directory the application has
already made durable; it reserves the stable owner lock and the manifest exclusively.
Interrupted initialization may need operator repair: missing or damaged metadata never
becomes an empty store silently.

`recover(callback)` hands complete batches to the callback one at a time, each view
expiring when the callback returns. The callback builds unpublished state, usable only
once recovery succeeds. Corruption or truncation anywhere in the required prefix,
including the last acknowledged batch, fails closed; extra bytes past the frontier are
discarded only after the prefix validates.

Every uncertain I/O or callback failure **fences** the journal until a fresh instance
recovers; the owner lock stays held while fenced. A failed append has an unknown outcome,
so callers need application-level idempotency. Related changes (a term and vote update,
a suffix replacement) belong in one batch; replacing an uncommitted Raft suffix appends a
new operation and never deletes acknowledged history.

### Immutable checkpoint generations

`prepare_artifact()` creates an exclusive immutable artifact and syncs its ownership
prefix before accepting payload, in chunks of at most 64 KiB; `finish()` seals its length
and checksum and syncs contents and name. One builder and at most nine prepared leases are
admitted, within a 512 MiB cumulative artifact bound.

Version 2 artifacts start with a 44-byte ownership prefix (magic, store identity,
artifact identity, CRC), then a 24-byte seal binding the prefix, payload length, and
payload checksum, then the payload. Readers also accept sealed version 1 artifacts
(56-byte headers), so a checkpoint can reuse an older dependency. Older binaries reject
version 2.

`publish_checkpoint(bundle, dependencies, covered_sequence)` takes prepared handles from
the same owner session, at most eight dependencies, and the exact current batch sequence:

```text
Use sealed capabilities → create/sync exclusive new journal generation
→ sync directory → write/sync replacement manifest → rename → sync directory
```

The version 2 manifest binds the bundle and every dependency by identity, length, and
checksum. The next append uses `covered_sequence + 1`. `verify_artifact(handle)` reads a
payload back before a consensus cutover freezes; publication itself checks capabilities
without rereading. `recover(replay, restore)` verifies the bundle and every dependency,
passes their bounded readers to `restore`, then replays the suffix. A missing or corrupt
referenced artifact fails closed; recovery never falls back to an older generation.
`pin_artifact()` keeps a referenced artifact for a later publication without rewriting
it.

### Incremental verification and published dependencies

`begin_artifact_verification(prepared)` returns a move-only `ArtifactVerifier`. Opening
checks the file size and header without scanning the payload; each `read_next()` reads
at most 64 KiB and advances a private offset and CRC-32C. `std::move(verifier).finish()`
checks the CRC and turns the same descriptor and pin into an `ArtifactReader`, without
reopening. At most nine verification leases exist per owner session. This proves byte
integrity only; application validation stays with the caller.

`select_published_dependency(index)` binds a session and a publication stamp (identity,
generation, checkpoint, dependency) without I/O, slots, or pins.
`begin_published_verification(selection)` opens it through the same verifier. An append
keeps a selection valid; a new checkpoint invalidates it. A snapshot source reads its
image this way.

### Bounded reclamation

`reclaim_step(scan_budget)` scans 1 to 4,096 directory entries through a streaming
cursor. Candidates match the reserved filename grammar, then a bounded header read must
prove the same store and basename identity; foreign, malformed, and unrelated files are
never touched, and obsolete payloads are never read. The current generation, manifest,
lock, checkpoint bundle, dependencies, active builders, and every pinned artifact are
protected. Deletions get a directory barrier before the step succeeds; uncertain I/O
fences. Statistics count scanned, protected, removed, and unidentifiable candidates.
Because namespace changes during a pass can omit or repeat entries, cleanup is eventual
and every candidate is rechecked against the current roots before removal.

### Inventory and admission

`Inventory` (`inventory.h`) is a census of a store's directory taken while the host keeps
the store quiescent: `step(budget)` counts every direct entry's logical and allocated
bytes without following links or opening targets. A partial or failed census never
authorizes admission.

`Admission` (`admission.h`) starts from a ready census, with hard limits on bytes and
directory entries and two protected pools, Control and Replacement, that normal permits
cannot consume. A move-only `StoragePermit` reserves the worst simultaneous footprint
before any mutation; `mark_started()` precedes I/O, and `settle(added, removed)` records
the final charges once the operation's barriers complete. Space returns only through
`credit_durable_reclaim()`, after a durable deletion. A started permit that is never
settled taints the ledger. The journal's `bootstrap_plan()`, `append_plan()`, and
`checkpoint_plan()` compute the footprints to reserve; `resources.h` holds the value
types and checked arithmetic.

### Store

`Store` (`store.h`) owns a journal, its census, its accounting session, and one
replacement cycle, and exposes only opaque reservations and replacement IDs. Appends
reserve as Normal or Control, then `append()`. A checkpoint reserves the whole
replacement cycle before capture, prepares Application then Bundle with
`begin_artifact()`, `write_chunk()`, and `finish_artifact()`, optionally verifies, and
`publish()`es. Ordinary appends interleave with preparation. Known cancellation charges
the staged orphans until reclamation removes them; uncertain errors fence journal and
accounting together.

## Completion-driven operations

Every mutation is a resumable state machine (`mutation.h`, `artifact.h`) exposing one
typed primitive at a time (write, sync, create, replace, directory sync, read, open,
size, scan, remove), so a completion backend can run it off the owner's thread. The
synchronous API drives the same states. The rules every driver follows:

- An accepted operation retains its backend, files, buffers, and the owner lock until its
  original primitive is reaped, even after its facade is destroyed.
- `complete()` accepts only the matching owner, operation, and step token; stale or
  duplicate completions advance nothing.
- Executed I/O never publishes metadata: the frontier settles only after the final
  barrier, on the owning executor.
- Canceling before submission does no I/O; dropping a started, unsettled operation
  fences.

Owned operations exist for appends (`begin_append`), artifact preparation
(`begin_artifact_preparation`), checkpoint publication (`begin_checkpoint`), payload reads
(`begin_read_at`, `begin_read_next`), metadata opening (`begin_artifact_open`,
`begin_published_open`), and reclamation (`begin_reclaim`), with matching `Store` jobs
that settle accounting on terminal success. The backends (BSD AIO and kqueue, Linux
io_uring, and their fallback worker) live in
[completion](https://github.com/Kronuz/completion).

## The shared turn log

`RFTW0001` is one log of turns shared by every group on a core (strata Track C, D11). A
turn is the records the core's groups staged, written in one write and made durable by
one data barrier (`File::sync_data`): one barrier per core per turn, whatever the group
count.

- **Segments** are preallocated (64 MiB by default, `TurnLogOptions`) ahead of need, so
  appends never create files or change sizes. Reclaimed segments are recycled by renaming.
- **Blocks** of 32 KiB start with a header (magic, identity, segment, block, epoch) that
  also carries the manifest of the block written before it: each group with a fragment
  there and its highest sequence, under one CRC-32C. A damaged block therefore costs only
  the groups it names.
- **Records** (a group, a per-group sequence, a kind: batch, hard state, or checkpoint,
  and a payload) are written as fragments into blocks. Fragments are bound to their
  segment and epoch, so bytes left by a recycled segment's earlier use never read as
  current.
- **Epochs.** Each open starts a new epoch. Recovery reads epoch by epoch; the newest
  epoch's last turn is the torn tail and is kept only if complete, and a torn turn of an
  earlier epoch names nothing acknowledged.
- **Recovery** (`TurnRecovery`) returns each group's records from its latest checkpoint
  record in sequence order. A group with a sequence gap, or one that may have lost
  records to damage, is lost and rejoins as a new incarnation.
- **Retention.** Past `retain_segments`, live records move forward (`forward_bytes` per
  turn) so old segments can be recycled.

`TurnDriver` (`turn_driver.h`) runs one core's turns. A group's `Store`, opened on the
shared backend, stages its batches and checkpoint records there; each staged operation
completes at its turn's barrier, or fails with the turn. One turn is in flight at a time.

**Migration (R20).** `Store::migrate` moves a per-group journal into the turn log; once
the migrated records are durable, `finish_migration` writes the `journal.shared` marker
and removes the old files. The marker is an empty file whose presence selects the shared
layout. Every step is checked at every crash point (`test/shared_store_test.cc`).

## POSIX backend and the durability model

`PosixIO` resolves single-component filenames against an opened directory descriptor and
rejects symlinks, special files, hard links, other devices, and permissions another user
could exploit. The stable `owner.lock` gives cross-process exclusivity and is never
replaced. Linux and FreeBSD use `fsync` on files and directories. macOS uses
`F_FULLFSYNC` for files, and for namespace barriers a directory `fsync` followed by
`F_FULLFSYNC` on the same-device lock file ([XNU's `fcntl(2)`](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2)).
Unsupported operations fail rather than weaken the contract.

The model assumes a local filesystem that honors atomic same-directory rename and
successful syncs, on hardware that honors its flush contract. The fault tests exercise
the protocol; they do not qualify a filesystem, mount option, drive, or power loss.
Rolling back an entire store consistently needs an external witness to detect.

## Testing

`memory_io.h` is an injected filesystem that tracks visible and durable contents and
names separately. The tests interrupt every operation before and after each side effect,
take both namespace outcomes before a barrier, restart and then cut power, corrupt every
durable byte, and truncate every acknowledged prefix; the sync and completion drivers run
the same schedules and must recover the same history. The turn log runs at every crash
point with corrupted blocks (T9), and `fuzz/` fuzzes its segments and checkpoint records.
The POSIX tests cover real create, append, checkpoint, reopen, locking, confinement, and
rejection of symlinks, hard links, and special files.
