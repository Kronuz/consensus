# Peer fetch: name-to-bytes reads on the bulk lane

A voter sometimes needs a file that every voter holds, such as Detent's history files after a
local file is lost. `Peers::fetch` asks one peer for it by name, over the connections, pins,
addresses and byte budgets the transport already has. This page describes the fetch lane
(`include/consensus/transport/fetch.h`, `peers.h`) and what it costs consensus.

## The contract

A host gives each `Peers` a reader in `PeerOptions::reader`: a function from a name to its
bytes, or `nullopt` when the voter holds none. With no reader, every name is not-found.

`fetch(peer, name, done)` asks `peer` for `name`. `done(error, bytes)` runs once, on the
owner executor, and never within `fetch` itself:

| Outcome | `error` | `bytes` |
|---|---|---|
| The peer holds the name | none | its exact bytes |
| The peer holds none | none | `nullopt` |
| No bulk lane to the peer | `not_connected` | `nullopt` |
| `fetches` already await the peer | `resource_unavailable_try_again` | `nullopt` |
| The peer's reader threw, or its reply exceeds the peer's `fetch_bytes` | `io_error` | `nullopt` |
| The reply exceeds this voter's `fetch_bytes` | `message_size` | `nullopt` |
| No reply within `fetch_deadline` | `timed_out` | `nullopt` |
| The bulk lane closed first | `connection_aborted` | `nullopt` |

An empty name, a name longer than one lane frame, or an empty `done` throws
`std::invalid_argument`. The caller picks the peer, and retries another on an error.

## Frames

Fetch frames ride the bulk lane, one per lane frame, beside snapshot transfer frames. Each
starts with the magic `DTFT0001`, which no transfer frame (`RFTXFR01`) shares, so the bulk
reader routes a frame by its first eight bytes. The 28-byte header holds the magic, a kind, three
zero bytes, the fetch's id, and a total:

- `Request` carries the name.
- `Data` carries one chunk of the reply, in order, and the reply's total size. A reply
  larger than one lane frame (64 KiB of body) spans several `Data` frames, and an empty
  file is one `Data` frame with a zero total.
- `Missing` and `Failed` end a reply with no body.

Ids are this voter's, increasing from one. A reply to an id this voter is not awaiting, a
chunk past the total, a total that changes mid-reply, or a malformed frame closes the bulk
lane. A voter running an older consensus closes the lane on the first fetch frame, since its
snapshot channel refuses the unknown magic; Detent upgrades its voters together.

## Serving

The peer queues each request and answers them in order. It calls the reader when the
request reaches the front, so one reply's bytes at most are in memory per lane, and writes
the reply one frame at a time. The peer holds at most `fetches` requests from a voter and
closes the lane past that. A voter never sends more: a failed fetch keeps its slot until
its reply ends (see Failures), so voters must agree on `fetches`.

## Budgets and consensus

A fetch frame takes its credit from the `outgoing_bytes` budget every lane shares, for one
frame at a time, the frame it is writing; received frames take `incoming_bytes` credit the
same way. A reply never holds credit for its whole size, so a large fetch cannot starve
control frames of budget. The bulk writer alternates fetch and snapshot frames while both
wait, so neither stalls the other.

Consensus frames travel on the control lane, its own TLS connection, so a fetch never sits
in front of them in a socket. They share only the executor. The transport holds it for one
frame's encryption at a time, but the serving voter runs the host's reader there too, once per
reply, for the whole file: a reader that reads from disk adds its read time to consensus on
that voter, so a host serves from memory or measures its largest file. The `fetch-alongside-consensus` scenario of
`test/transport/peers_test.cc` measures this: four rounds of four concurrent 8 MiB fetches
between three voters, through 1 MiB budgets, while the leader commits.

| Window | Append round trips | Slowest round trip | Timeouts | Slowest commit |
|---|---|---|---|---|
| Quiet, 500 ms | 64 | within 16 ms | 0 | 23 ms |
| Fetching 128 MiB, 540 ms | 64 | within 16 ms | 0 | 41 ms |

The budgets peaked at 133 KiB, and the leader kept its term. The slowest round trip is bounded
by its histogram bucket. This is one run, a release build at consensus:6c3018e, on an Intel
i9-9980HK laptop (macOS, 16 threads) shared with other jobs at low priority; a commit's time
includes the test's 1 ms polling. Under TSan the slowest round trip lands in the (16, 64] ms
bucket, so a TSan build asserts the next bucket and relies on the unchanged term.

That run used the 40 ms RPC timeout of the other scenarios. On slow CI runners the three
voters' TLS, all on the one test thread, pushes append round trips past 40 ms, and the Core
never matches a late response, so no append completes. The scenario therefore runs with a
200 ms RPC timeout and a 400 ms election minimum (D92), and keeps its 2 s commit bound.

Memory outside the budgets: a requester assembles each reply in a buffer of its claimed total,
reserved on the first chunk, so the bound is peers x `fetches` x `fetch_bytes` (1 GiB at the
defaults with four peers); a server holds one reply per lane.

## Failures

A fetch fails at `fetch_deadline` (30 s by default) if no complete reply arrived. It keeps
its slot without a completion and drains the rest of its reply. A peer then never holds more
than `fetches` of this voter's requests, even when a late reply is still coming. The lane
stays open for one more deadline: a slow peer is not a broken one. If the reply is still
unfinished then, the peer is hung or gone (a half-open TCP connection never errors, and an
idle bulk lane has no read deadline), so the voter closes the bulk lane. That frees the slots,
both ends start over on the redial, and fetches to the peer fail with `not_connected` until a
lane is back.

When a bulk lane closes, whether the peer stopped, a frame broke the protocol, or a write
failed, every fetch awaiting that lane fails with `connection_aborted`. Its requests and the
peer's queued requests are dropped. The control lane is a separate connection and keeps
carrying consensus. `stop()` closes every lane, so it fails every awaiting fetch the same
way.

The name a peer sends reaches the reader unchecked: the reader confines it to what it serves.

`PeerStats` counts `fetched` (answered with bytes or not-found), `fetch_failures` and
`served`; `PeerOccupancy` counts `fetches` awaiting replies and `serving` queued requests.
