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
no certificate gets one by joining (`admission_join.h`) with the cluster's token, version 2:
`DTJT0002` and three hex fields, 168 characters: the 16-byte cluster id, the CA key's pin
(`key_pin`, the SHA-256 of its SubjectPublicKeyInfo, as kubeadm pins) and a 32-byte secret
(`fresh_join_token`, `encode_join_token`, `decode_join_token`). A version 1 token (`DTJT` and 128
hex digits) or another version's magic is refused as `version`, anything else as `malformed`.
The cluster keeps only the secret's SHA-256 (`join_secret_digest`), compared in constant time
(`join_secret_matches`). Creating the CA and the token, and checking the token's life and uses,
are the caller's (Xapiand's M4).

The joiner makes its key once and saves it before its first request (`fresh_join_key`,
`save_join_key`), so a retry after a lost answer or its own crash is the same key, and the same
node. `join(executor, loop, endpoint, token, key, detail, requested)` reaches any member on the
transport's own port. The exchange, DTJN0002, within one deadline at the joiner (10 s):

```
joiner -> member   "DTJN0002" in the clear, then TLS that authenticates neither end
member -> joiner   the offer: its cluster id and the CAs   joiner: the cluster is the token's
                   it trusts (at most 16)                  (cluster), a CA's key is the pin and
                                                           the member's leaf chains to it (ca)
joiner -> member   the request: cluster id, secret,        member: its cluster (cluster), the
                   requested node, the caller's detail,    CSR's signature (malformed); then
                   a CSR for the saved key                 the hook
member -> joiner   the answer: a fault and the hook's      joiner: the certificate is its key's,
                   reason, or the node and its certificate chains to the CA, names the node (issued)
```

Frames are a 4-byte little-endian length and at most 64 KiB (`join_frame_limit`): a request
holds a CSR (about 300 bytes) and the caller's detail, a name and, for a cluster of one carrying
its indexes (Xapiand's M3), their paths, so at most about 63 KiB of them. The joiner sends its
secret only after the cluster and the CA's key match its token, so a host of another cluster, or
one replaying the CA certificate with another CA's leaf, never sees it.

The member's side is `JoinService`, which asks the issuer hook (`JoinHook`) for the answer:
asynchronous, since in Xapiand it relays to the root's leader, which checks the secret, names
the node by one command and signs. The hook must be idempotent per key (`JoinRequest::key`, the
CSR key's pin): a retry gets the same node. Its refusals carry a fault (`token`, or `refused`
with its own reason: expired, used, name) that the joiner reads as `JoinRefused::fault` and
`reason`. `Issuer`, for the node holding the CA key, is one: its `assign` hook names the node,
and it signs once per key and node within a retry window (10 minutes, in memory), so two joins
with the same key at once, and a retry, get one certificate. The certificate names the node under
the CA policy's prefix (`URI:<prefix><node>`), CA:FALSE, server and client auth, SHA-256, a year
by default.

`Joined::save(directory)` writes the key, the CA and the certificate, in that order, each whole
to `<name>.new`, synced and renamed over the old one, the key created 0600 so no other user ever
reads its bytes; the certificate goes last, so a crash before it leaves the saved key and the old
certificate (or none), and the next start joins again with the key. A failed create, write,
sync, close or rename throws `std::system_error` naming the file. The joined node then connects
as any node does, and the cluster CA policy records its key's pin at its first connection.
Refusals are `JoinRefused` with a `JoinFault`, counted at the member in `refused(fault)`.

### The join pool

Joins share the transport's port (`HostTransportOptions::join`, the hook). An accepted
connection stays in the handshakes' pool (host-connection.md, Before authentication) only until
its first byte, peeked: a TLS record (0x16) goes on as a node's, in the same place; the join
magic's `D` leaves it for the join service's own pool, so joiners never take a node's handshake
slot, and a node's handshake keeps the peer priority it had. The join pool (`JoinOptions`):

| Limit | Default | What it costs |
|---|---|---|
| `pool` | 64 joins at once | past it a joiner is refused, never a slot evicted: 64 sockets and their TLS sessions |
| `per_source` | 8 from one address | one address cannot fill the pool; an IPv6 /64 can, as for handshakes |
| `rate` | 10 a second per address, 10 at once | past it a joiner waits for its turn in its slot (`delayed()`), so a flood costs at most 10 TLS handshakes a second per address on loop 0 |
| `slot` | 10 s | from the pool's taking it until the request arrived: a joiner that stalls is cut (`cut()`), so 64 idle sockets free their slots within 10 s |
| `answer` | 10 s | the hook and the answer's write; a hook that fails answers `refused` |

The pool's counters are `busy()` (refused at a cap), `delayed()`, `cut()` and `serving()`;
`LinkStats::joins` counts the accepts handed to it. A reload (below) gives the service the new
context, so its offer names the new CAs.

## Keys, renewal and reload

Pins are key pins: `key_pin(certificate)` hashes the certificate's SubjectPublicKeyInfo, so every
certificate for the key has it. A renewed certificate for a pinned key is the node's with no
operator step, and a new key under a pinned node's name is refused (`repinned`, or `unpinned`
under pre-approved pins). The pin costs one SHA-256 of about 91 bytes per handshake.

`NodePins` keeps the CAs it verifies against. At authentication, and again when a connection's
hellos are done, before it becomes a link, the peer's leaf and the chain it sent are verified
against them at that moment (`verifies`): an expired certificate, or one a reload no longer
trusts, is refused (`unverified`), whatever the handshake saw. That is one more chain verification
per connection, none per frame.

`HostTransport::reload(context)` takes a context with this node's renewed certificate (the same
key, or it throws) or a new CA bundle: NodePins and the cluster CA policy take its CAs, the join
service its offer, new connections the context, and each loop then aborts every link, established
or offered, whose peer's chain no longer verifies (`LinkStats::unverified`). The peer redials,
and is refused until its own certificate chains to a trusted CA again. Pins do not change. Peers
(the transport `Peers` replaces) still pins the certificate's DER (`certificate_fingerprint`),
as Detent's configuration names it.

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
- `renew`: node 3 with a renewed certificate (`n3r`, its key, a new certificate) calls and
  answers, its pin unchanged and nothing refused.
- `issue` (the CA policy alone, on node 1's transport port): token v2's round trip; a version 1
  token and another version's refused (`version`), a cut or non-hex one (`malformed`); a wrong
  and a missing secret refused by the hook (`token`); a token of another cluster (`cluster`) and
  one pinning another CA key (`ca`) refused by the joiner before its secret goes out; a node the
  hook refuses (`refused`, with its reason); an impostor presenting another CA's leaf that offers
  the token's CA certificate (`ca`, it receives 0 bytes); nothing signed for any of them. Then two
  joins with one saved key at once and a retry get node 5 and one certificate, another key node
  6; node 5 calls node 1, which pins its key at its first join.
- `save`: under umask 022 the key is 0600 and the certificates 0644; a child stopped by
  `SIGXFSZ` at the key's write left only owner-readable key bytes; a write failing with `EFBIG`
  throws naming the key and leaves none; a save over a whole one killed while writing the key
  leaves the old three files whole, and killed while writing the certificate, the last, leaves
  the new key and CA and the old certificate whole; an unwritable directory throws naming
  `node.key`.
- `expired` (CA): a certificate that expired an hour ago neither calls nor answers; no pin is
  recorded.
- `reload` (CA): a reload with another key's certificate throws; node 1 reloaded to trust only a
  second CA closes its links to node 2 (`unverified`), neither calls the other, and node 2
  renewed under the second CA links again, both pins unchanged.
- `flood` (CA; joiners from ::1, nodes from 127.0.0.1): 12 joiners stall after the magic: 8 are
  served and 4 refused at the address's cap; the 8 are cut within the 1 s slot deadline set for
  the test. Then 8 joiners join back to back for about 3 s while node 2's links are removed and
  admitted 12 times: node 2 answers each time within the handshake deadline, and joins past the
  rate wait.
- `boot` (CA): hello.md, The boot check.
- `redial` (CA): a call made within the redial spacing after a failed dial, with no other call
  waiting, dials once the spacing passes rather than waiting for its deadline, as the relink after
  a reload needs (the spacing 1500 ms, a silent peer failing each dial at its 300 ms handshake
  deadline).

Mutants (`test/transport/admission_mutants.py`): `no-pin-check` (a dial takes any certificate as
the node it meant) fails `refused_pins`; `no-repin-check` (a first join re-pins a pinned node)
fails `refused_ca`; `remove-keeps-links` fails `remove_*`; `links-no-epoch` (a connection
authenticated before the removal becomes a link) fails `epoch_*`; `any-token` (a secret matches
any digest), `join-any-cluster`, `join-unpinned-ca`, `token-no-version`, `join-unchained-issuer`
(the joiner skips the member leaf's chain) and `sign-each-time` (two joins with one key get two
certificates) fail `issue`; `save-chmod-after` (the key written through an ofstream, then
chmodded) and `save-in-place` (each file written over the old one) fail `save`; `pin-der` (pins
over the certificate's DER) fails `renew_*`; `reload-keeps-links` and `reload-keeps-trust` fail
`reload`; `join-no-cap`, `join-no-slot-deadline` and `join-no-rate` fail `flood`;
`skip-boot-check` (the acceptor skips the boot check) fails `boot`; `redial-at-deadline` (such a
call waits for its deadline) fails `redial`.
