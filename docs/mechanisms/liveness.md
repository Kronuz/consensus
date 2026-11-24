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
harness reads it at a node's first Tick after a core step, and a Tick before it is held rather
than stepped:
its delay is still drawn, so the random stream and every schedule stay the same, and the held
Tick reaches the core just before the node's next event, so that event sees the clock it would
have. A leader's Tick is held only while its membership is settled and it is not a witness: a
D3 removal and a hand-off wait for a Tick, which `next_deadline()` does not report.

`LIVENESS_VERIFY=1` steps every held Tick instead and fails one that returns an action or moves
the deadline. With it, and with a hash of every action each core returns (time, node, kind, a
message's kind and term), main's harness and this one produce the same output and the same hash
in every profile.

Each node's one scheduled Tick lives on the node, beside the queue, ordered with the queue's
events by time and then by the same order counter; the queue holds deliveries, persistences and
applications as small records, a message in flight in a slot vector.

## Cost

ASan, 20 seeds, main's harness and this one on the same host, one after the other (job
261124-061757-8fb9aa): 129.7 s and 64.8 s. On CI's linux-asan-ubsan, against
`consensus_shared_worker` in the same job:

| Run | consensus_liveness | consensus_shared_worker | Ratio |
|---|---|---|---|
| main 54c6599 (typical runner) | 82.09 s | 139.12 s | 0.590 |
| main 512e3ef (slow runner) | 173.49 s | 238.35 s | 0.728 |
| held Ticks, 8511ff1 | 40.79 s | 143.55 s | 0.284 |

What is left is the cores' own work on the events main also delivers: a profile of the ASan run
(job 261124-062325-f25d84) puts 54% in `Core::step`, and no single harness cost above 9%.

## Output

`main` line-buffers stdout, so a run killed at its timeout still shows each profile it finished.
