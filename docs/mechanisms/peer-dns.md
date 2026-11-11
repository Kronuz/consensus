# Peer names: resolution off the executor

`PeerOptions::addresses` names each peer by a host and a port. The host is an IP literal or a
host name. A slow resolver, a failing one, or one that never answers must not block the
executor, the consensus tick, another peer's dial, a dial to a known address, or `stop()`.
This page describes how `Peers` meets that (`include/consensus/transport/peers.h`,
`resolve.h`), and what a hung `getaddrinfo` costs.

## Literals and names

The constructor takes an address for each other voter and nothing else. It parses each host
with `asio::ip::make_address`. A literal becomes the peer's only endpoint and never reaches
the resolver. A name starts with no endpoints and is looked up again as its lanes dial, so a
name that moves is followed.

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

A peer has at most one lookup in flight, and a new lookup of a name starts only once the
last one's deadline has passed. A dial that finds a lookup already running, or one started
within the deadline, does not start another. Each peer name therefore holds at most one
thread per `Peers`, and a peer that is down but resolvable costs one lookup per
`resolve_deadline`, not one per backoff of each lane. `PeerOccupancy::lookups` counts the
threads.

## The deadline

A dial of a name with cached endpoints never waits for DNS. It dials the cache at once, and
the lookup it started, if any, refreshes the cache behind it. A slow resolver therefore
costs a cached peer no dial latency. A name that moved is dialed at its new address on the
first dial after the refresh answers.

Only a name with no cached endpoints waits. A lookup's deadline is
`PeerOptions::resolve_deadline` (1 s by default), measured from the lookup's start. An
uncached dial waits for the answer on a timer that expires at that deadline. The delivery
cancels the timer, and so does `stop()`. If the deadline has already passed when the dial
joins a lookup, the dial does not wait. Either way, an uncached dial that goes ahead without
an answer counts in `PeerStats::lookup_timeouts`, finds no endpoint, and backs off.

An answer that arrives after its deadline still updates the cache. A resolver that is
slower than the deadline therefore still fills and refreshes the cache.

## The cache

The cache is the set of endpoints from the last lookup that returned any address. The dial
keeps the whole set, not only the address that connected, so a peer that moves within its
set is found during an outage. An empty or failed answer leaves the cache as it was, and
counts in `PeerStats::lookup_failures`. An answer equal to the cache leaves it as it was,
including where the next dial starts (see Identity).

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

A dial whose connection fails authentication, whether refused by the pin or by a service
that is not a voter at all, moves the peer's next dial to start after that endpoint. The
race picks its winner at TCP connect, so without this a name whose earlier address reaches
another voter would win the same wrong race on every dial. The order wraps around, so every
address is still tried. An answer that changes the set starts again from its first
address.

## What a hung `getaddrinfo` costs

A hung lookup costs one parked thread per peer name per `Peers` instance. A restarted
`Peers` starts its own lookup while the old one's thread is still parked, so parked threads
grow with restarts until the resolver returns. Each thread has a default stack and blocks in
the resolver until the resolver returns. The system resolver eventually returns,
through `resolv.conf`'s timeout and attempts settings.

It costs nothing else:

1. The executor never waits on a lookup. Dials wait on timers.
2. Other peers' lookups run on their own threads.
3. A peer with cached endpoints is dialed at once.
4. `stop()` cancels the waits and abandons the lookup, so `drained()` does not depend on
   the resolver.

`test/transport/peers_test.cc` runs three Workers over TLS and checks each of these
properties. `test/transport/dns_mutants.sh` seeds the faults the test must catch: a dial
that waits for its lookup with no deadline, every lookup queued on one shared thread, a
refused address that does not move the next dial past it, a cached dial that waits for its
lookup, and a lookup started on every dial.
