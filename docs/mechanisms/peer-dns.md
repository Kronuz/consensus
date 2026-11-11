# Peer names: resolution off the executor

`PeerOptions::addresses` names each peer by a host and a port. The host is an IP literal or a
host name. A slow resolver, a failing one, or one that never answers must not block the
executor, the consensus tick, another peer's dial, a dial to a known address, or `stop()`.
This page describes how `Peers` meets that (`include/consensus/transport/peers.h`,
`resolve.h`), and what a hung `getaddrinfo` costs.

## Literals and names

The constructor parses each host with `asio::ip::make_address`. A literal becomes the
peer's only endpoint and never reaches the resolver. A name starts with no endpoints, and
each dial resolves it again, so a name that moves is followed on the next dial.

Only the lower node id dials, as before: voter 1 dials 2 and 3, voter 2 dials 3. Each dial
covers a control lane and a bulk lane, so two dials of a peer can run at once.

## The lookup and its thread

`PeerOptions::resolver` is a blocking function from a name to its addresses. It defaults to
`system_resolver`, which calls `getaddrinfo`; tests inject their own. A failure is an empty
answer, and an exception counts as one.

`Lookup::start` runs the resolver on a new detached thread. `getaddrinfo` cannot be
cancelled, so the lookup does not use asio's `async_resolve`: that runs every lookup of an
`io_context` on one internal thread, and one hung lookup would queue every later one behind
it. Because each lookup has its own thread, a hung lookup holds only that thread.

The thread hands its answer back through a delivery function under the lookup's mutex. The
delivery posts the answer to the executor with a weak reference to the `Peers`. `stop()`
and the destructor call `abandon()`, which clears the delivery under the same mutex. After
that, the thread touches nothing of the transport's, not even the executor. When the
resolver returns, the thread drops its answer and exits. The thread holds no executor work,
so a hung lookup never keeps `io_context::run()` from returning.

A peer has at most one lookup in flight. A dial that finds a lookup already running waits
for the same lookup and does not start another. Each peer name therefore holds at most one
thread, however often its lanes redial. `PeerOccupancy::lookups` counts these threads.

## The deadline

A lookup's deadline is `PeerOptions::resolve_deadline` (1 s by default), measured from the
lookup's start. A dial waits for the answer on a timer that expires at that deadline. The
delivery cancels the timer, and so does `stop()`. If the deadline has already passed when a
dial joins a lookup, the dial does not wait. Either way, a dial that goes ahead without an
answer counts in `PeerStats::lookup_timeouts` and dials the cached endpoints.

An answer that arrives after its deadline still updates the cache. A resolver that is
slower than the deadline therefore still keeps the cache current. The next dial uses its
answer and starts the next lookup.

## The cache

The cache is the set of endpoints from the last lookup that returned any address. The dial
keeps the whole set, not only the address that connected, so a peer that moves within its
set is found during an outage. An empty or failed answer leaves the cache as it was, and
counts in `PeerStats::lookup_failures`.

The cache lives in the `Peers` object. A restarted `Peers` starts empty, so its first dial
of a name waits for a lookup. With no cached endpoints and no answer, the dial fails and
backs off 100 to 199 ms, as a refused connect does. Each lane then repeats its dial.

A lookup never closes a connection. While a peer is connected its lanes do not dial, so a DNS
outage costs nothing until a connection drops. A dropped connection is redialed at the
cached endpoints.

## Several addresses

A lookup's answer is ordered as RFC 8305 section 4 describes (`interleave`). The family of
the first address comes first, then the families alternate, and each family keeps the
resolver's order. A dial starts the first endpoint. It starts the next endpoint
`PeerOptions::stagger` later (250 ms by default), or at once when every attempt it started
has failed. Started attempts keep running until one of them connects. The first to connect
wins, and the dial closes the rest. `PeerOptions::deadline` bounds the whole dial.

An address with no route therefore costs one stagger, not a connect deadline. A refused
address costs nothing beyond its round trip.

## Identity

A name only decides where to dial. The handshake decides who answers. A dial authenticates
against two pins only: its own and the dialed peer's. If a name resolves to another voter,
that voter's certificate fails the pin check right after the TLS handshake, before either
end sends a hello. The other voter admits nothing, and the dial backs off and retries. The
accepting side still pins every voter, because it learns who is calling from the
certificate.

## What a hung `getaddrinfo` costs

A hung lookup costs one parked thread per peer name. That thread has a default stack and
blocks in the resolver until the resolver returns. The system resolver eventually returns,
through `resolv.conf`'s timeout and attempts settings.

It costs nothing else:

1. The executor never waits on a lookup. Dials wait on timers.
2. Other peers' lookups run on their own threads.
3. A peer with cached endpoints is dialed when the lookup's deadline passes.
4. `stop()` cancels the waits and abandons the lookup, so `drained()` does not depend on
   the resolver.

`test/transport/peers_test.cc` runs three Workers over TLS and checks each of these
properties. `test/transport/dns_mutants.sh` seeds two faults the test must catch: a dial
that waits for its lookup with no deadline, and every lookup queued on one shared thread.
