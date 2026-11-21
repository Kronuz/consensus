# Peers wake: telling the host when owner work waits

A host that runs a Worker over `Peers` drives it from an owner loop: `admit()`, the Worker's
`run_one`, the snapshot channel's `run_one`, `pump()`. Between turns it wants to sleep until the
Worker's next deadline. Peers holds work the loop cannot see from there: a Control reader keeps
its decoded input and waits until `admit()` takes it, and the Bulk lane feeds the snapshot
channel on its own. `PeerOptions::wake` tells the host when that happens, so the loop can sleep
until it is called or a deadline passes. This page describes the contract
(`include/consensus/transport/peers.h`).

## The contract

`wake` is a `std::function<void()>`, empty by default (no calls). Peers calls it on its
executor, synchronously, right after an event leaves the owner work and before Peers suspends
again, so no owner turn runs between the event and its call:

| Event | The owner's work |
|---|---|
| A Control frame decoded; its reader retains the input | `admit()` |
| A Bulk frame the snapshot channel took bytes from, or whose held frame it ran | the channel's and the Worker's `run_one`, `pump()` for any reply |
| A Bulk frame the channel refused, closing its open session (corrupt or unbound frame, fenced Worker) | the channel's and the Worker's `run_one` |
| A snapshot frame written and consumed from the channel | the channel's `run_one`: its next chunk or control frame |
| A Bulk session opened (registered with the channel) | the channel's `run_one`: a transfer waiting for that peer |
| An open Bulk session lost: the peer hung up, a frame deadline or a read failed, a fetch's second deadline closed the lane | the channel's and the Worker's `run_one` |

One event is one call. A frame retried while the channel is busy calls again only when the
channel takes it. Nothing else calls it: an idle Peers, dials and redials, handshakes, Control
lanes opening or closing (a closing Control lane drops its retained input), frames written on the
Control lane, fetch frames (Peers serves requests itself), and fetch completions, whose `done` is
already the host's own callback.

Peers never calls `wake` from inside a call the host made into it (`start`, `stop`, `send`,
`pump`, `fetch`, `admit`): work that call made is the caller's to see. The Bulk sessions
`stop()` closes, and the lanes that end after it, call nothing.

## What the host may do in it

Peers holds no lock, and every call site is at a consistent point, so `wake` may call back into
Peers: `admit()` (the input is taken at once and the reader goes on to its next frame), `send`,
`pump`, `fetch`, `stop`. A host whose Worker and Peers share one executor can admit from the
wake directly. A host whose owner loop runs elsewhere raises its own wake (an eventfd, a
condition variable, a posted handler) and returns.

`wake` must not throw: Peers calls it from a `noexcept` function, so a throw terminates.

## Edge, not level

`wake` marks a change, not a state. A host checks the level before it sleeps: a retained input
`admit()` refused (`occupancy().retained_inputs`, as when the Worker answered `Busy` or
`Pressure`), the Worker's own runnable work, a transfer in flight on the channel. A wake that
arrives while the loop is awake, or during the check, is not lost if the loop re-checks after
every wake it consumed.

## Tests

`test/peers_wake_test.cc`, one ctest entry per case (`consensus_transport_peers_wake_*`):

- `idle`: a lone voter redials a dead peer for a second with no call; three connected voters
  call it twice each (their two Bulk sessions) and not at all through a second of no traffic.
- `control`: three voters elect and commit with every input admitted from the wake itself. Each
  wake that follows a Control frame sees exactly one new frame, with its input retained; each
  owner turn sees no frame without its wake; at rest each voter's count is its frames plus two.
  A voter stopping costs each other voter one call (its lost Bulk session) and itself none.
- `bulk`: a scripted voter on the Bulk lane checks each row of the table in turn (session opened,
  fetch served with no call, a Begin taken with the transfer already the channel's, its
  Accepted written, a corrupt frame, a hang-up, a fetch's second deadline) and the exact count
  after each.

`test/peers_wake_mutants.py` writes a mutant per row that drops its call; the plain test fails
at each.
