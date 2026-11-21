# Multicast seeds: a seed source, never an identity

A node on a LAN finds the other nodes of its cluster with no flags: it announces its services by
UDP multicast and learns other nodes' addresses as seeds (`include/consensus/transport/seeds.h`,
the datagram in `seeds_codec.h`). Joining and membership stay where they were: a node dials a
seed over the transport's TLS, and the node pins decide who answered
([host-connection.md](host-connection.md)). An announcement is a hint. `--join`-style explicit
seeds, resolved by [peer-dns.md](peer-dns.md), remain the way on Kubernetes and clouds, where
multicast does not route. Xapiand and Detent use this one source; neither keeps its own.

## The datagram

`DTSD0001`, little-endian:

| Offset | Bytes | Field |
|---|---|---|
| 0 | 8 | magic `DTSD` and the version as four digits, `0001` |
| 8 | 16 | cluster |
| 24 | 16 | sender: random per `Seeds` object, so a process drops its own announcements |
| 40 | 1 | service count, 1 to 8 |
| 41 | | per service: name length (u8, 1 to 32), the name (`[a-z0-9._-]`), port (u16, nonzero) |

A host may announce several services: Detent's voters and relays, Xapiand's node. The address is
the datagram's source address; an announcement does not name one, so a sender cannot point
listeners at a third host without spoofing its source.

## Limits

- `seed_services_limit` = 8 services and `seed_name_limit` = 32 bytes: the largest valid datagram
  is 321 bytes. `seed_datagram_limit` = 512 is the receive bound, under any IPv4 path's 576-byte
  minimum reassembly size, so an announcement is never fragmented. The receive buffer is one byte
  larger, so a longer datagram reads as oversized instead of truncated.
- A seed is a dial target: the source address and the asked-for services (names and ports),
  never the sender, which the announcer picks freely. `SeedOptions::per_address` (16) bounds the
  entries one address holds, enough for a host's few processes; beyond it an announcement for a
  new target is counted in `SeedStats::address_full`. `SeedOptions::seeds` (256) bounds the whole
  set, counted in `SeedStats::full` beyond it.
- `SeedOptions::expire` (10 s, at least one interval and its jitter) drops a seed not heard.
- `validate_seed_options` refuses a non-multicast group, port 0, a TTL outside 0..255, an
  interval below 10 ms or jitter above it, a burst outside 1..16, a per-address bound outside
  1..`seeds`, and a backoff above its maximum.

## Drops

`decode_announcement` returns a fault for every datagram it does not read: `oversized` (over 512
bytes), `version` (a `DTSD` magic with other digits), and `malformed` (anything else: a short
datagram, another magic, a count of 0 or above 8, an overrun or empty name, a character outside
the name alphabet, port 0, a repeated name, a zero cluster, a trailing byte). The listener then
drops a well-formed announcement of another cluster (`cluster`), its own (`own`), and one with
none of the services it asks for (`unwanted`). Each has a counter in `SeedStats`; none is a seed.
The decoder is fuzzed: `fuzz/seeds_fuzz.cc` checks that a decoded announcement is valid and
re-encodes to the same bytes, and its corpus `fuzz/seeds_corpus/` replays in ctest.

## Announcing

`Seeds` runs on one executor, the host's loop. `start()` opens a receiving socket bound to the
group and port (`SO_REUSEADDR` and `SO_REUSEPORT`, so several processes on one host share it),
joins the group on `SeedOptions::interface` (the system's choice when empty), and opens a
sending socket with that outbound interface, the TTL (3 by default, 0 keeps announcements on the
host) and loopback on. It sends `burst` announcements (3) `burst_spacing` apart (100 ms), so a
starting node is found at once, then one per `interval` (2 s) plus a uniform jitter up to
`jitter` (1 s), so nodes started together do not stay in step. The defaults are the Xapian-era
group and port, 239.192.168.1:58880.

A node that only seeds announces nothing; one that only announces joins nothing.

## Unavailable multicast

When a step of `start()` fails (no route to the group, an interface the host lacks, a sandbox
that denies the socket, a network not up yet), or a send or a receive fails, `Seeds` calls
`on_unavailable` once with the step and the error, and `unavailable()` keeps the reason. It
throws nothing: the node goes on with its explicit seeds. A setup failure closes the sockets and
retries the whole setup after `backoff`, doubling per failure up to `backoff_max`
(`setup_failures`); when a retry succeeds, `unavailable()` is empty again and a later outage is
reported anew. A send failure is counted in `send_failures` and announcing continues. A receive
failure is counted in `receive_failures` and the next receive waits the same doubling backoff,
reset by a datagram received, so a persistent error costs a few wakeups, not a spinning loop.

## Dialing seeds, and backoff

`on_dial(seed)` fires for a new seed, and again for one still announced whose backoff has passed.
The caller dials the seed's service port at its address over the transport's TLS, with its node
pins, and answers `joined(id)` when a pinned node authenticated, or `failed(id)` otherwise. A
seed is not offered again until then, nor after `joined` until the caller reports the node lost
with `failed(id)`. Each failure doubles the wait before the
next dial, from `backoff` (1 s) to `backoff_max` (60 s). Announcements do not reset it: a forger
repeating an announcement every 10 ms gets one dial per backoff, not one per datagram, whatever
sender it writes, since the backoff belongs to the dial target. A seed that stops announcing while
it backs off is kept until the backoff ends, so coming back resumes the backoff instead of
starting over. An `on_dial` that throws counts as `failed(id)`, and the exception leaves the
loop's `run()` while the source keeps listening.

## What it trusts

Nothing in an announcement is authenticated. Anyone on the link can send one, with any cluster,
sender and port, from a spoofed source. What that buys them:

1. A dial to its source address and the port it names, then backoff: one dial per backoff step
   per target, and at most `per_address` targets from one address.
2. Nothing else. The cluster and sender only select and drop a process's own; the node behind a
   seed is whoever the TLS handshake authenticates against the node pins, so a forged seed never
   becomes a member, and a genuine node on another address is found through its own announcements
   whatever a forger sends without spoofing.

A forger that spoofs a genuine node's address and names other ports makes new targets, which back
off on their own; the genuine target's backoff is untouched. A forger on a genuine node's own
host can take that address's `per_address` entries.

The threat model is the trusted cluster's: honest faults and the perimeter, not compromised nodes.

## Tests

`test/transport/seeds_test.cc`, one ctest entry per case:

- `codec`: the round trip, the largest datagram (321 bytes), every truncation, and each fault.
- `drops`: oversized, malformed, wrong-version and other-cluster datagrams counted and never
  seeds; a host announcing three services seeded only with the two a listener asks for.
- `processes`: three processes on one host find each other and report the same seed set.
- `unavailable`: an interface no device has: reported once, nothing thrown, no seeds.
- `drops` also floods 64 ports from one address at a listener with `per_address` 4: that address
  holds 4 entries, the rest counted in `address_full`.
- `forged`: a forger announcing a TLS listener whose certificate is not pinned every 10 ms while
  a genuine node announces its own: the genuine node joins, the impostor is dialed at most five
  times in 2 s, each wait at least its backoff.
- `forged_rotating`: the same forger flooding 256 senders, then a new sender per datagram: one
  seed, the same backoff bound, and a genuine node started after the flood joins.
- `rate`: the burst within 80 ms, then gaps of one interval plus jitter (plus up to 100 ms of a
  loaded runner's slack); the TTL and interface reach the sending socket; the defaults and every
  refused option.
- `recovers`: a `start()` with no file descriptor left fails, retries at 50 and 150 ms, reports
  once, and seeds once descriptors are back.
- `receive_errors`: a receiving descriptor turned into a pipe fails every receive: 3 to 6 failures
  in a second with backoff 50 ms, reported once.
- `throws`: an `on_dial` that throws: the exception leaves `run()`, the seed is dialed again after
  its backoff.
- `require`: the always-skipping case exits 77, and 1 where `CI` or `CONSENSUS_SEEDS_REQUIRE` is set.

Announcements in tests stay on the host (TTL 0) on loopback, or on the default interface where
loopback carries no multicast; a host where neither works skips the cases with the reason. Where
`CI` (GitHub Actions sets it, and the FreeBSD VM receives it) or `CONSENSUS_SEEDS_REQUIRE` is set,
that is a failure, so CI never passes these cases by skipping them. The `forged` cases dial over
the transport's TLS, whose OpenSSL `write()` to a closed peer raises `SIGPIPE` where
`SO_NOSIGPIPE` is missing (Linux): the test ignores it, the host's duty
([host-connection.md](host-connection.md)), and raises one first to prove it. `test/transport/seeds_mutants.py` seeds the
faults the tests must catch.
