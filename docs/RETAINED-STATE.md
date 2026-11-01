# Retained state and checkpoint capacity

How the retained log is bounded, retired and funded across checkpoints. The host contract and
the Worker are in [ARCHITECTURE.md](../ARCHITECTURE.md).

Checkpoint encoding and recovery share retained-entry validation. Lowering current operating
limits does not invalidate existing retained history; configuration entries use their
membership encoding bound. Incremental encoding validates entries within each call's record
budget. Hosts still need enough replacement capacity for the retained suffix when lowering log
limits. Committed delivery emits an oversized historical entry on its own when the current RPC
byte limit is lower.

`RetainedStatePolicy` bounds historical command bytes, total payload bytes and entry counts,
and replay batch bytes and entry counts independently of operating `Limits`. Defaults use the
library ceilings without reserving those maxima. `WorkerLimits::retained` supplies the policy
to the Worker; Core, Recovery and checkpoint codecs accept the same policy as a trailing
argument. Operating limits must fit the policy. Counts, lengths and aggregate capacity are
checked before corresponding retained-state allocation.

Retirement shares one budget across retained entries and pending output cleanup. A first
historical payload larger than a fresh nonzero byte budget may retire alone, saturating
remaining bytes to zero. Zero-byte predecessors consume units and defer oversized work to a
fresh budget. Lowering command limits therefore preserves finite shutdown without raising every
cleanup budget to the maximum payload size.

Checkpoint preparation initially reserves the configured operating capacity. Immediately before
capture, the Worker queries `Core::capture_usage(request)` and grows the same Journal
replacement reservation to cover the surviving suffix. Insufficient protected capacity returns
pressure before freeze; the candidate remains cancelable. Requests already owed a rejection
bypass funding growth and still run through Core, including durable term advancement for a
higher-term caught-up install. Provision replacement capacity for retained history, not only
the current admission limits. The policy bounds one Worker and does not replace node-wide
admission across groups.
