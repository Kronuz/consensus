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
| 24 | 16 | sender: random per `Seeds` object, so a process knows its own announcements |
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
- `SeedOptions::seeds` (256) bounds the seed set: a flood of distinct senders costs that many
  entries and is counted in `SeedStats::full` beyond it.
- `SeedOptions::expire` (10 s, at least one interval and its jitter) drops a seed not heard.
- `validate_seed_options` refuses a non-multicast group, port 0, a TTL outside 0..255, an
  interval below 10 ms or jitter above it, a burst above 16, and a backoff above its maximum.

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
that denies the socket), or a send fails, `Seeds` calls `on_unavailable` once with the step and
the error, and `unavailable()` keeps the reason. It throws nothing: the node goes on with its
explicit seeds. After a setup failure the sockets close and the source stays idle; a later send
failure is counted in `send_failures` and announcing continues.

## Dialing seeds, and backoff

`on_dial(seed)` fires for a new seed, and again for one still announced whose backoff has passed.
The caller dials the seed's service port at its address over the transport's TLS, with its node
pins, and answers `joined(id)` when a pinned node authenticated, or `failed(id)` otherwise. A
seed is not offered again until then, nor after `joined`. Each failure doubles the wait before the
next dial, from `backoff` (1 s) to `backoff_max` (60 s). Announcements do not reset it: a forger
repeating an announcement every 10 ms gets one dial per backoff, not one per datagram. A seed that
stops announcing while it backs off is kept until the backoff ends, so coming back resumes the
backoff instead of starting over.

## What it trusts

Nothing in an announcement is authenticated. Anyone on the link can send one, with any cluster,
sender and port, from a spoofed source. What that buys them:

1. A dial to the address it names, then backoff: one dial per backoff step per forged sender,
   and at most `seeds` senders at once.
2. Nothing else. The cluster and sender only select and deduplicate; the node behind a seed is
   whoever the TLS handshake authenticates against the node pins, so a forged seed never becomes a
   member, and a genuine node is found through its own announcements whatever a forger sends.

The threat model is the trusted cluster's: honest faults and the perimeter, not compromised nodes.

## Tests

`test/transport/seeds_test.cc`, one ctest entry per case:

- `codec`: the round trip, the largest datagram (321 bytes), every truncation, and each fault.
- `drops`: oversized, malformed, wrong-version and other-cluster datagrams counted and never
  seeds; a host announcing three services seeded only with the two a listener asks for.
- `processes`: three processes on one host find each other and report the same seed set.
- `unavailable`: an interface no device has: reported once, nothing thrown, no seeds.
- `forged`: a forger announcing a TLS listener whose certificate is not pinned every 10 ms while
  a genuine node announces its own: the genuine node joins, the impostor is dialed at most five
  times in 2 s, each wait at least its backoff.
- `rate`: the burst within 50 ms, then gaps of one interval plus jitter; the TTL and interface
  reach the sending socket; the defaults and every refused option.

Announcements in tests stay on the host (TTL 0) on loopback, or on the default interface where
loopback carries no multicast; a host where neither works skips the cases with the reason
(`CONSENSUS_SEEDS_REQUIRE=1` makes that a failure). `test/transport/seeds_mutants.py` seeds the
faults the tests must catch.
