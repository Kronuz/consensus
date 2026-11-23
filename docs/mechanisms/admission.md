# Node admission: nodes join and leave a running host transport

A host transport starts with the nodes it knows and admits more while it runs: a node's address
and pin are added and removed after construction, so `--join` and multicast seeds can grow a
cluster. Which certificate makes a joining node trusted is a trust policy; the default is the
cluster CA, with the pin recorded at first join (D149). The code is
`include/consensus/transport/admission.h` (faults and policies), `node_pins.h` (the directory),
`links.h` (routes and removal) and `host_transport.h` (`admit_node`, `remove_node`). How one connection
authenticates is in [host-connection.md](host-connection.md).

## The directory

`NodePins` is a handle to one directory shared by every copy: the host's `Links` on each loop
holds a copy, so an admission through any of them holds on every loop at once. A mutex guards
it; it is taken once per handshake and once when a connection becomes a link, never per frame.
For each node it keeps:

| State | Meaning | A certificate naming it |
|---|---|---|
| pinned | `admit(node, pin)`, or recorded at first join | accepted when it is the pin, else `repinned` (cluster CA) or `unpinned` (pre-approved pins) |
| unknown | never admitted, or admitted with no pin | the trust policy decides; the pin is recorded on acceptance |
| removed | `remove(node)` | `removed`, whatever the policy says, until `admit` again |

A fingerprint belongs to one node: a second node cannot take it, and a removed node keeps it, so
its old certificate is refused as `removed` rather than looked up again. Admitting a removed node
with a new pin frees the old one; admitting it with none lets the policy pin it again at its next
first join. The local node is never removed and never re-pinned.

## Admission

`HostTransport::admit_node(node, address, pin)` runs on `loop_of_node(node)`, the loop that owns the
node's connections. It admits the node in the directory, then routes it: `Links::route` sets the
address, abandons a lookup of the old name, and raises the route's serial, so an answer for the
old name that is already posted is dropped. Dials and calls to the node start from there; before
it, `call` ends `not_connected` at once, since the node has no route. Admitting a routed node
again moves its address and keeps its pin and its links.

An accept needs no route: any loop's accept authenticates against the shared directory and moves
the connection to the node's loop, as before.

## Trust policies

`TrustPolicy` sees the verified leaf and the chain the peer sent, for a certificate no pin names,
and answers the node it joins as or the fault that refuses it. It runs on the handshake's loop,
on several loops at once, outside the directory's lock; the directory then records the pin under
the lock, unless another loop recorded it meanwhile, the node has another pin (`repinned`), or it
was removed (`removed`). A connection refused anyway, the local node's certificate or a dial that
reached another node, records nothing.

- **The cluster CA, the default.** `NodePins(local, pins, context)` builds `ClusterCa(context,
  local)`: the CAs `context` trusts, and the cluster the local certificate names. A certificate
  that chains to those CAs and whose URI is the prefix and a decimal node id
  (`spiffe://<cluster>/node/17`) joins as that node. The prefix is the local certificate's URI
  that ends in `/<local id>`, less the id, so a node's own certificate defines its cluster and no
  flag can disagree with it. A certificate no CA signed is `unpinned`; one that names no node
  under the prefix, two of them, or a zero-padded id is `cluster`. A local certificate that names
  no node leaves only pins admitting, as before this mechanism. `ClusterCa(ca, prefix)` takes
  both explicitly.
- **Pre-approved pins.** `pre_approved_pins()` is no policy, and admits nothing by itself: a node
  joins only once `admit(node, pin)` named its certificate; every other certificate is `unpinned`.

The policy runs only for certificates no pin names. A pinned node is checked against its pin on
every connection, whichever policy recorded it.

## Removal

`HostTransport::remove_node(node)` runs on the node's loop. `Links::remove` removes it from the
directory, which refuses its certificates from then on, on every loop; erases its route, so no
dial starts and `call` ends `not_connected`; and aborts its links and its dial's offered hello on
both lanes. The links' end fails their calls as any lost connection does (`connection_aborted`),
and `remove_node` fails the calls still waiting for a connection (`not_connected`). Frames groups
queue for the node wait as for an unreachable one; removing a member from its groups is the
groups' business.

One race remains: a connection whose TLS handshake passed before the removal, with its hellos
still in flight, possibly on another loop moving to this one. Each removal raises the directory's
epoch. `Links` records `epoch()` when each connection starts, before its handshake reads the
pins, and a connection becomes a link only while `current(node, since)`: the node is pinned and
was not removed after `since`. A remove and a re-admit with a new pin in between therefore cannot
let the old certificate's connection through. Removal through `NodePins::remove` alone refuses new
connections but leaves established links up; the transport's `remove_node` is the one that closes them.

## Joining with a token

Every node speaks TLS under the cluster CA; there is no unauthenticated mode (D150). A node with
no key and no certificate gets both by joining (`admission_join.h`): it holds only the cluster's
join token, `DTJT` and two hex fields, 132 characters: the CA certificate's SHA-256 fingerprint
and a 32-byte secret (`fresh_join_token`, `encode_join_token`, `decode_join_token`). Creating the
CA on first start and printing the token are the caller's (Xapiand's cluster A7).

`join(executor, loop, endpoint, token, requested)` connects to an `Issuer`, a node holding the CA
key, over TLS that authenticates neither end yet. The DTJN0001 exchange, length-prefixed frames
of at most 64 KiB, within one deadline:

```
issuer -> joiner   the CA certificate                      joiner: its fingerprint is the token's,
                                                           and the issuer's leaf chains to it (ca)
joiner -> issuer   the secret, the node it asks for, a CSR  issuer: the secret (token), the CSR's
                   for a P-256 key it made just now        signature (malformed), the hook (node)
issuer -> joiner   a fault, or the node and its certificate joiner: the key and the CA (issued)
```

The joiner sends its secret only after it pinned the CA by the token, so a host that is not the
cluster's never sees it. The issuer compares the secret in constant time; a wrong or empty one is
`token`. Its `assign` hook names the node a joiner becomes, or refuses it (`node`); `issued` may
record the certificate. The certificate names the node under the CA policy's prefix
(`URI:<prefix><node>`), CA:FALSE, server and client auth, signed with SHA-256 for a year by
default. `Joined::save(directory)` writes the certificate, the key and the CA, and returns the
options a Peer context loads: each file is written whole to `<name>.new`, synced and renamed into
place, the key created 0600 so no other user ever reads its bytes, and a failed create, write,
sync, close or rename throws `std::system_error` naming the file; the joined node then connects as any node does, and the
cluster CA policy records its pin at its first connection. Refusals are `JoinRefused` with a
`JoinFault`, counted at the issuer in `refused(fault)`.

The exchange runs on whatever socket the caller accepts for it, not on the transport's own
listener: the transport's handshake requires a client certificate, which a joiner does not have
yet.

## Refusals

Every refusal throws `PinRefused` with its `PinFault` before any hello or frame, and counts in
`NodePins::refused(fault)`: `unverified` (TLS did not verify it), `unpinned`, `cluster`,
`repinned`, `removed`, `own` (this node's certificate) and `another` (a dial reached another
node). The messages of the faults that existed before keep their text.

## Tests

`test/transport/admission_nodes_test.cc`, its own certificates from
`admission_certificates.cmake`; one ctest entry per case and policy (`ca`, the default, and
`pins`), the same steps and checks under both:

- `policy`: the cluster CA's verdict on each fixture certificate, the prefix derived from the
  local certificate, and the directory's rules (shared copies, `repinned`, a pin is one node's,
  the epoch and `current`).
- `join`: a node admitted at runtime is dialed, completes the hello and answers; its pin is the
  one approved (pins) or recorded at first join (ca).
- `refused`: a node never admitted (a CA-signed certificate naming no node) and node 3 presenting
  a re-keyed certificate, by dial and by accept, are refused by name (`cluster` or `unpinned`,
  `repinned` or `unpinned`); node 3 keeps its first pin.
- `remove`: eight removals and re-admissions of node 3 while calls flow both ways between nodes 1
  and 2, which share a loop with node 3 on node 1: each removal ends node 3's calls both ways at
  once, its dials are refused (`removed`), node 1 does not dial it, it answers once admitted again,
  and no call between 1 and 2 fails. It runs under TSan.
- `epoch`: the race the removal epoch closes, forced on loops the test steps. Node 1's dial to
  node 3 finishes TLS, node 3 is removed, then the hellos run; and node 1 accepts node 3 on one
  loop, moves it to its owner loop, which waits while node 3 is removed, then reads its hello.
  Neither becomes a link.
- `issue` (the CA policy alone): the token's round trip; a wrong token and a missing one refused
  (`token`), a token of another CA refused by the joiner before its secret goes out (`ca`), a node
  the hook refuses (`node`), nothing issued for them, and an impostor presenting another CA's leaf
  that replays the token's CA certificate (`ca`, it receives 0 bytes); then a node with no key or
  certificate joins, calls node 1, and node 1 pins its issued certificate at its first join.
- `save`: under umask 022 the key is 0600 and the certificates 0644; a child stopped by
  `SIGXFSZ` at the key's write left only owner-readable key bytes; a write failing with `EFBIG`
  throws naming the key and leaves none; an unwritable directory throws naming `node.pem`.

Mutants (`test/transport/admission_mutants.py`): `no-pin-check` (a dial takes any certificate as
the node it meant) fails `refused_pins`; `no-repin-check` (a first join re-pins a pinned node)
fails `refused_ca`; `remove-keeps-links` fails `remove_*`; `links-no-epoch` (a connection
authenticated before the removal becomes a link) fails `epoch_*`; `any-token` (an issuer takes any
secret) and `join-unchained-issuer` (the joiner skips the issuer leaf's chain) fail `issue`;
`save-chmod-after` (the key written through an ofstream, then chmodded) fails `save`.
