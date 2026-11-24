# The liveness test's cost

`consensus_liveness` (test/liveness_test.cc) runs a discrete-event cluster of Cores under four
timing profiles: the default (Detent's authority profile, `election_max` 2 s) and three derived
from per-platform barrier estimates (`election_max` 192-372 ms). The run is deterministic: a seed
fixes every schedule.

## Where the time goes

Each node ticks every `heartbeat / 5`: 20 ms in the default profile, 2-3 ms in arm64 and
Linux, 1 ms in FreeBSD. The phases' windows are written in milliseconds (a 20 s isolation, a 5 s
settling wait), so the derived profiles spend 5-14 times the default's events, almost all of
them Ticks that change nothing. At main, ASan, 20 seeds (job 261124-040930-6e9c88):

| | default | arm64 | Linux | FreeBSD |
|---|---|---|---|---|
| events | 0.37 M | 2.79 M | 1.91 M | 5.27 M |
| seconds | 7.5 | 49.1 | 44.3 | 91.4 |

The windows stay as they are. Their length in election timeouts differs by profile (a lone
follower's isolation is 10 election timeouts in the default profile and 104 in FreeBSD), and
mutants show the long ones catch what the short ones do not: a node that campaigns after 12
election timeouts without a leader, and a vote granted without the log check, which fences a
node only in a long heal window (Linux, seed 7).

## Held Ticks

`Core::next_deadline()` is the earliest Tick time at which a Tick may change anything. The
harness reads it after each core step, and a node's Tick before it is held rather than stepped:
its delay is still drawn, so the random stream and every schedule stay the same, and the held
Tick reaches the core just before the node's next event, so that event sees the clock it would
have. A leader's Tick is held only while its membership is settled and it is not a witness: a
D3 removal and a hand-off wait for a Tick, which `next_deadline()` does not report.

`LIVENESS_VERIFY=1` steps every held Tick instead and fails one that returns an action or moves
the deadline. With it, and with a hash of every action each core returns (time, node, kind, a
message's kind and term), main's harness and this one produce the same output and the same hash
in every profile.

## Output

`main` line-buffers stdout, so a run killed at its timeout still shows each profile it finished.
