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
- **Pre-approved pins.** `pre_approved_pins()` admits nothing by itself: a node joins only once
  `admit(node, pin)` named its certificate.

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

Mutants (`test/transport/admission_mutants.py`): `no-pin-check` (a dial takes any certificate as
the node it meant) fails `refused_pins`; `no-repin-check` (a first join re-pins a pinned node)
fails `refused_ca`; `remove-keeps-links` fails `remove_*`.
