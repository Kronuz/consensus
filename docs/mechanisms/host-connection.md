# The host connection: one per peer node and lane

The host transport (Detent `docs/design/async-drivers.md` D10, Track C D16) gives each pair of
nodes one Control and one Bulk connection, shared by every group the two nodes have in common.
This page describes how one connection starts, authenticates, stays alive and ends
(`include/consensus/transport/connection.h`, `node_pins.h`, `node_hello.h`, `links.h`), and the
deadline rule every transport wait follows (`deadline.h`). Today's `Peers` stays beside it until
worker-pump-removal deletes it.

## The late-deadline rule

A deadline that fires more than one wheel tick (1 ms) past its time fired late because the loop
did not run: the host stopped or descheduled the process. The peer had no chance to answer
during that time, so the deadline extends once, by the lateness, capped at one timeout. A
second late fire expires it. `late_deadline` is the only implementation, and every transport
wait uses it: the TLS handshake and hellos, each read wait (the dead-peer deadline), each
write wait, close_notify's included, the keepalive interval, a name's lookup and the TCP connect race. The keepalive
interval times this end's own duty, not the peer's time, so it takes no extension: a late fire
sends the keepalive at once. Extended, a loop busy longer than the interval would send one
every other turn, twice as far apart as it can.

- **The wait owns its timer.** A `late_deadline` is its own wheel entry, so destroying it
  unfiles the entry. A `wait_list` holds the deadlines waiting for one event, and a deadline
  leaves its list when destroyed, so no waker reaches a dead one.
- **A wake after a late fire is seen.** The fire decides on the loop when it runs. If it
  extends, a `wake()` later in the same batch (a lookup's answer, a completed connect, a
  close) cancels the extension and ends the wait at once. A wake is never lost to an expiry
  already queued.
- **Nothing resumes inside the call that ends it.** `wake()` and the fire post the waiter.

`deadline_test` drives it on `inline_scheduler`, whose `advance()` stands in for a stop.

## Node identity

A host has one TLS context and one `NodePins`: a certificate per node, pinned once per host
(async-drivers Q7 (a)). Construction refuses node 0, a repeated certificate, and a local pin
that is not the context's certificate. A handshake authenticates a verified leaf that is pinned
to a node other than this one, and, on a dial, to the dialed node. Another node's certificate,
this node's own, an unpinned one or none is refused before any hello or frame. Per-group checks
(cluster, group, epoch, profile) belong to the group-open exchange, not to the connection
([host-transport.md](host-transport.md), Identity).

## The connection

OpenSSL runs on the non-blocking socket; readiness comes from `await_asio` on `async_wait`.
`SSL_MODE_RELEASE_BUFFERS` frees the record buffers while idle, and the read buffer grows only by
what arrived, so an idle connection holds neither. OpenSSL reads through its socket BIO and writes
through the connection's own, which sends with `MSG_NOSIGNAL`: a write to a peer that closed fails
with `EPIPE` and never raises `SIGPIPE`, so the host need not ignore it (`peer_gone` in
`connection_test`).

A host's loops share its TLS context. OpenSSL decodes a certificate's extensions on its first
verification and publishes them through an atomic flag that ThreadSanitizer cannot see in
libcrypto, so `Links` caches every certificate the context holds (`cache_certificates`, in
`tls.h`) when it takes the context, before any loop handshakes: handshakes only read them.

A frame is a 4-byte little-endian length and its bytes. The reader hands each frame to the
connection's handler synchronously, viewing the connection's own buffer, and never waits on a
shared budget. A zero-length frame is the keepalive: a connection that sent nothing for
`keepalive` (1 s) sends one, once per connection, whatever groups use it. A connection that
received nothing for `dead_after` (5 s) ends `dead`; one stuck writing ends `failed`. Each
wait of the writer, the frames' and `close_notify`'s alike, has its own `dead_after`, and at
its expiry a socket whose send queue (bytes written and not yet acknowledged: `SIOCOUTQ`,
`SO_NWRITE` or `FIONWRITE`) drained during the wait is retried rather than ended. The queue is
sampled right before the wait, after this end's own writes, so only the peer's
acknowledgments count, and a reading that fails counts as no drain. So a peer reading steadily
but slower than the queue drains is not stuck, however coarsely the kernel wakes the writer:
Linux reports a full socket writable only once a third of its send buffer is free, and a
macOS or FreeBSD low-water mark as high as the buffer only once it is empty, which at a slow
reader's pace can take longer than `dead_after`. A peer that stops reading stops the drain once
its own buffer fills, and the next wait ends the connection. The cost is one `ioctl` or
`getsockopt` before each wait and one at its expiry, none for a write that never waits.

`close()` drains: the frames queued so far, then TLS `close_notify`. A peer that reads
`close_notify` answers the same way, and both ends finish `closed`. An end that stops without
`close_notify` (`abort()`, a crash) leaves the peer `failed`, never `closed`, so a truncated
stream is never taken for a complete one.

A sender with queues of its own (the host transport) hooks two moments: `on_read`, after each
read's frames went to the handler (also when the read then fails, as on an oversized length,
so no frame it handed over is left waiting for another connection's read), and `on_drained`, when the queue is out and the writer would
wait, so it hands over a little at a time and keeps the order of what waits. `rehome()` moves
a connection to another loop between its handshake and its hellos.

An end (`abort()`, a failure) shuts the socket down and cancels its waits; the fd closes only
when `run()` returns, or with the connection. Until then the number stays this socket's, so no
socket opened elsewhere can take it while the reader or writer still holds it, and no SSL call
follows the end. No frame reaches the handler after it either.

## The hello, DTHL0002

After the TLS handshake the dialer sends its hello and the acceptor replies with its own. Both
are 64 bytes, little-endian:

| Offset | Bytes | Field | Rule |
|---|---|---|---|
| 0 | 8 | magic | `DTHL0002` |
| 8 | 8 | node | nonzero; the authenticated node |
| 16 | 4 | lane | 1 Control, 2 Bulk; the dialer's choice, echoed |
| 20 | 4 | flags | bit 0 `refused`, from the acceptor only; other bits zero |
| 24 | 16 | cluster | nonzero; equal at both ends |
| 40 | 16 | connection | nonzero, random per connection; differs from the peer's |
| 56 | 4 | rpc_min | at least 1 |
| 60 | 4 | rpc_max | at least rpc_min, at most 9,999 |

A hello that breaks a rule ends the connection. The RPC lane's version on the connection is
`min(max_a, max_b)` when that is at least `max(min_a, min_b)`, else 0, and the lane is off on it
(rpc-lane T1). DTHL0001 is `Peers`' hello and is not read here.

## Dial, accept and the tie-break

`Links` keeps a host's connections on one loop: at most one undrained link per peer node and
lane. A dial resolves the peer (a literal at once; a cached name at once while a lookup
refreshes it; an uncached name after its lookup's answer, or its deadline), races the addresses
(RFC 8305: the next a `stagger` after the one before, or as soon as every started one failed),
and handshakes against the dialed node's pin. An accept handshakes against every pin; when another loop's `Links` owns the peer (`owner`), the
authenticated connection moves there for its hellos (`adopt`).

Two nodes may dial each other at once. The hellos settle it before any frame:

1. A dial's hello counts from the moment it is sent. Before that, a dial that finds a link
   already established gives up quietly: its connection is aborted and released.
2. An acceptor refuses a hello for a lane it holds undrained.
3. When its own dial's hello is out, the dial of the lower node wins: the higher node accepts
   the lower one's dial, and the lower node refuses the higher one's. Both ends reach the same
   answer from the same two facts.
4. The refusal is the acceptor's hello with `refused` set. Frames flow only after the dialer
   reads an accepting hello, and after the acceptor sends one.

A draining connection does not count: once `close()` starts, or the peer's `close_notify`
arrives, a new connection of the lane may be dialed or accepted while the old one finishes. The
peer sees the drain only when it reads the `close_notify`, behind the queued frames; until then it
refuses the retiring end's redial, and the caller redials.

### Before authentication

An incoming connection is unauthenticated until its TLS handshake passes the pins, so `Links`
bounds those: at most `handshakes` (4) at once, and at most `handshakes_per_source` (2) from one
address, since a node dials each lane once at a time. A newcomer replaces the oldest instead of
being refused: the source's oldest when the source is at its cap, else, when the pool is full, its
oldest from an address no route names. A newcomer from a peer's address (a literal one, or a
name's cached answer) replaces the pool's oldest when every slot is a peer's; one from an unknown
address is then refused. So a flood from addresses the cluster does not name never drops a peer's
pending handshake (`links_test churn`, mutant `no-peer-priority`), and a peer holding idle
connections from one address keeps 2 slots and cannot lock a real dialer out.

What is left: a flood from a peer's own address, or one that lands while no peer handshake is
pending, delays links. A real handshake is dropped when `handshakes` newer connections (from its
own address, `handshakes_per_source`) arrive before it completes, so the rate the flood needs is
the pool's size per handshake time: thousands a second on loopback, tens a second across a WAN,
where a handshake takes a round trip or more. The per-source cap keys on the exact address; an
IPv6 attacker holding a /64 has as many sources as it wants, so there the pool is the only bound.
A connection leaves the pool once its pins pass, before the hellos: only an authenticated node
holds hello-phase connections, each for at most the `handshake` deadline (5 s). Each unknown
connection holds a slot for at most that deadline too. Keeping an unauthenticated flood out
belongs to the network (an allowlist of the cluster's addresses).

## Checks

| What | Where |
|---|---|
| The rule, a mutant per part | `deadline_test`; mutants `no-extension`, `uncapped`, `twice`, `wake-ignores-extension`, `entry-left-armed`, `late-always-extends` |
| Node identity | `connection_test` `authenticate`, `wrong_node`, `unpinned`, `no_certificate`, `pins`; mutants `pin-any-node`, `pin-self`, `pin-unpinned` |
| DTHL0002 | `node_hello_test`; `fuzz_decoders` case 9 |
| Frames, keepalive, dead peer, close | `connection_test` `frames`, `keepalive`, `dead_peer`, `graceful_close`, `abort_truncates`, `busy_reader`; mutants `no-keepalive`, `keepalive-extends`, `close-drops-queue`, `no-close-notify` |
| The fd kept until `run()` returns, no SSL call after the end | `connection_test` `fd_reuse_reader`, `fd_reuse_writer`; mutants `fail-closes-fd`, `ssl-after-end` |
| Each write wait has its own stuck deadline | `connection_test` `slow_reader` (a failed run the host stalled 100 ms or more runs again, at most twice); mutant `stuck-spans-flush`; `slow_reader_stops.py` forces the stalls with `SIGSTOP` |
| A send queue draining during the wait keeps it, sampled after this end's writes | `connection_test` `coarse_writer` (the writer wakes only at an empty 1.5 MiB queue, 3 s or more against a 2 s `dead_after`; where the low-water mark takes, a drained expiry must occur; a failed run whose queue the host held half of `dead_after` runs again, at most twice); mutants `stuck-ignores-drain`, `stuck-samples-before-flush` |
| A peer that never reads ends the writer `failed` | `connection_test` `nonreader` (a relay stops forwarding to the peer, which keeps sending keepalives); mutants `stuck-never-fails`, `stuck-equal-drains` |
| Dial, tie-break, draining, lookup, two loops | `links_test`; mutants `refuse-when-offered`, `accept-when-offered`, `draining-counts`, `lookup-no-wake`; `threads` under TSan |
| A given-up dial released; the pre-authentication bounds | `links_test` `abandoned_dial`, `flood`; mutants `abandon-keeps-dial`, `refuse-newest`, `no-source-cap` |

Mutants are commits written by `test/transport/mutant_commit.py`; the plain test run at one must
fail.
