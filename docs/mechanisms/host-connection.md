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
stuck write, the keepalive interval, a name's lookup and the TCP connect race.

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
(cluster, group, epoch, profile) belong to the group-open exchange, not to the connection.

## The connection

OpenSSL runs on the non-blocking socket (`SSL_set_fd`); readiness comes from `await_asio` on
`async_wait`. `SSL_MODE_RELEASE_BUFFERS` frees the record buffers while idle, and the read buffer
grows only by what arrived, so an idle connection holds neither.

A frame is a 4-byte little-endian length and its bytes. The reader hands each frame to the
connection's handler synchronously, viewing the connection's own buffer, and never waits on a
shared budget. A zero-length frame is the keepalive: a connection that sent nothing for
`keepalive` (1 s) sends one, once per connection, whatever groups use it. A connection that
received nothing for `dead_after` (5 s) ends `dead`; one that cannot write for as long ends
`failed`.

`close()` drains: the frames queued so far, then TLS `close_notify`. A peer that reads
`close_notify` answers the same way, and both ends finish `closed`. An end that stops without
`close_notify` (`abort()`, a crash) leaves the peer `failed`, never `closed`, so a truncated
stream is never taken for a complete one.

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
and handshakes against the dialed node's pin. An accept handshakes against every pin.

Two nodes may dial each other at once. The hellos settle it before any frame:

1. A dial's hello counts from the moment it is sent. Before that, a dial that finds a link
   already established gives up quietly.
2. An acceptor refuses a hello for a lane it holds undrained.
3. When its own dial's hello is out, the dial of the lower node wins: the higher node accepts
   the lower one's dial, and the lower node refuses the higher one's. Both ends reach the same
   answer from the same two facts.
4. The refusal is the acceptor's hello with `refused` set. Frames flow only after the dialer
   reads an accepting hello, and after the acceptor sends one.

A draining connection does not count: once `close()` starts, or the peer's `close_notify`
arrives, a new connection of the lane may be dialed or accepted while the old one finishes.

## Checks

| What | Where |
|---|---|
| The rule, a mutant per part | `deadline_test`; mutants `no-extension`, `uncapped`, `twice`, `wake-ignores-extension`, `entry-left-armed` |
| Node identity | `connection_test` `authenticate`, `wrong_node`, `unpinned`, `no_certificate`, `pins`; mutants `pin-any-node`, `pin-self`, `pin-unpinned` |
| DTHL0002 | `node_hello_test`; `fuzz_decoders` case 9 |
| Frames, keepalive, dead peer, close | `connection_test` `frames`, `keepalive`, `dead_peer`, `graceful_close`, `abort_truncates`; mutants `no-keepalive`, `close-drops-queue`, `no-close-notify` |
| Dial, tie-break, draining, lookup, two loops | `links_test`; mutants `refuse-when-offered`, `accept-when-offered`, `draining-counts`, `lookup-no-wake`; `threads` under TSan |

Mutants are commits written by `test/transport/mutant_commit.py`; the plain test run at one must
fail.
