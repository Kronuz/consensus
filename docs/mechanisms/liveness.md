# The liveness test's windows

`consensus_liveness` (test/liveness_test.cc) runs a discrete-event cluster of Cores under four
timing profiles: the default (Detent's authority profile, `election_max` 2 s) and three derived
from per-platform barrier estimates (`election_max` 192-372 ms). Each phase builds a cluster,
applies a fault, and checks a property. The run is deterministic: a seed fixes every schedule.

## Windows and deadlines

The phases wait in two ways:

1. A deadline bounds a wait that ends on an event: `commit_one`'s 20 s and 30 s, a transfer's
   10 s, a step-down's 20 s. It costs only the time until the event, and it is the bound a
   failure hits, so it stays in milliseconds in every profile.
2. A window is idle time the phase spends on purpose: a partition's isolation (1-5 election
   timeouts), a lone follower's isolation (10) and its return (2.5), a witness group's settling
   (2.5), a removed voter's return (10). `window(profile, ms)` takes the default profile's
   milliseconds and scales them by the profile's `election_max` over 2 s, so a window is the same
   number of election timeouts in every profile. The default profile's run is unchanged.

Each property a window feeds is a count (a raised term, an unfinished hand-off) or a latency
measured after the window against a bound in milliseconds or in `election_max`. Every Core timer
except `rpc_timeout` is a fixed share of `election_max` in every profile (heartbeat a tenth of
`election_min`); `rpc_timeout`'s 100 ms floor makes it 0.25-0.52 of `election_max`, so a window
holds 1.9-4 RPC timeouts at its shortest.

## Cost

Events, not simulated time, set the cost: each node ticks every `heartbeat / 5`, 20 ms in the
default profile and 1-3 ms in the derived ones. Written in the default profile's milliseconds,
the windows cost the FreeBSD profile 14.2x the default profile's events, and the idle phases
(lone, removed, witness, heal) took 96% of an ASan run:

| ASan, 20 seeds, main (job 261124-040930-6e9c88) | default | arm64 | Linux | FreeBSD |
|---|---|---|---|---|
| events | 0.37 M | 2.79 M | 1.91 M | 5.27 M |
| seconds | 7.5 | 49.1 | 44.3 | 91.4 |

## Output

`main` line-buffers stdout, so a run killed at its timeout still shows each profile it finished.
