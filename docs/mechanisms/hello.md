# The node hello: limits and versions

Each host connection starts with one hello per end, `DTHL0002`
(`include/consensus/transport/node_hello.h`), after TLS authenticates the peer's node: the dialer
sends first, the acceptor answers or closes ([host-connection.md](host-connection.md)). Besides
the node, lane, cluster and connection id, it carries what the two ends must agree on before any
frame: the RPC lane's versions, the host transport's wire versions, and the node's ten `Limits`.

## Bytes

Little-endian, 152 bytes:

| Offset | Bytes | Field |
|---|---|---|
| 0 | 8 | `DTHL0002` |
| 8 | 8 | node |
| 16 | 4 | lane: 1 Control, 2 Bulk |
| 20 | 4 | flags: bit 0, the acceptor refuses the connection as a duplicate |
| 24 | 16 | cluster |
| 40 | 16 | connection id |
| 56 | 8 | `rpc_min`, `rpc_max` (u32 each): the RPC lane's versions, `DTRPnnnn` |
| 64 | 8 | `wire_min`, `wire_max` (u32 each): the host transport's frame versions |
| 72 | 80 | the node's `Limits`, u64 each: voters, command_bytes, log_bytes, log_entries, control_entries, rpc_bytes, rpc_entries, reads, uncommitted_entries, members |

`decode_node_hello` refuses a hello of another size or magic, node 0, an unknown lane, a
reserved flag, a zero cluster or connection id, an empty or inverted version range, an RPC
version above 9999, a limit past `size_t`, and limits that fail `validate_limits` (the one rule
a group's own limits pass, so a zero or inverted limit never reaches a leader). The connection
then closes before any frame.

## Versions

- **Wire.** Each end speaks `wire_min` to `wire_max` (this build: 1, `wire_version`). Ranges that
  share no version are refused: `check_node_hello` throws `WireVersionRefused`, naming both
  ranges, and `LinkStats::version_refused` counts it. Nothing crosses that connection.
- **RPC lane.** The lane runs at `min(max_a, max_b)` when that is at least `max(min_a, min_b)`;
  otherwise it is off on that connection and its calls end `protocol_not_supported`, while group
  frames still flow (rpc-lane design T1).

## Limits and `PeerLimits`

A host states its node's limits in `LinkOptions::limits`; its hellos carry them. The host
transport refuses, at construction, limits that fail `validate_limits` and a connection
`maximum_frame` below a frame of its own `rpc_bytes` (`frame_header` plus `control_frame_bound`):
peers' leaders send it appends up to that size. A group's window below that size is not refused,
since a frame always goes when nothing is outstanding.

The transport reports a peer's limits per incarnation: before the first admitted frame from a
source incarnation on a connection, on the group's loop, it calls the loop's `on_peer_limits`
with the group and `PeerLimits{source, limits}` from that connection's hello. A new connection
reports again, and a store or forget of the group resets it. The host steps the report into the
group's `Core` (`Driver::peer_limits`), whose leader then sends that member only entries within
both nodes' command and RPC limits and holds back a larger one, which the member takes by install
once a checkpoint covers it (Core's `peer_wire_limits`, M8).

## Evidence

`test/transport/hello_test.cc` (`consensus_transport_hello_*`):

| Case | What it shows | Control |
|---|---|---|
| `codec` | the limits and wire range round-trip; zero or inverted limits and `wire_min` 0 refused; disjoint wire ranges refused as `WireVersionRefused` | |
| `leader` | host 3's hello carries a 64 KiB command limit; host 1 reports it for incarnation 3 before that source's first frame; a leader fed the report commits a 512 KiB entry on 1 and 2 and sends no append carrying it to 3 | `hello-limits-ignored` (`test/transport/rpc_mutants.py`) |
| `version` | a host speaking only wire version 2 never connects to one speaking 1: refusals counted, no link, no frame; a 1 MiB `maximum_frame` for 2 MiB `rpc_bytes` refused | |

`test/transport/node_hello_test.cc` checks the bytes field by field and each malformed field;
`fuzz/fuzz_decoders.cc` (case 9) decodes and re-encodes arbitrary hellos.
