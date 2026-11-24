# The simulation's cost

`consensus_simulation` (test/simulation_test.cc, with test/driver_simulation.cc) is
deterministic: a seed fixes every schedule, so its work is the same on every host, and its
wall time scales with the host alone. Under ASan on CI that time has to fit the 300 s TIMEOUT
on the slowest runner class, not the typical one.

## Runner classes

The linux-asan-ubsan runners come in classes that differ by up to 2.2x in every test, not only
this one. Over 46 runs on 2026-11-21..23, keyed by `consensus_consensus`'s time:

| Class | consensus_consensus | consensus_liveness | consensus_shared_worker |
|---|---|---|---|
| fast | 2.3 s | 62-67 s | 121-139 s |
| typical | 2.8-3.1 s | 78-82 s | 137-145 s |
| slow | 4.4-4.7 s | 160-176 s | 233-258 s |

The simulation took 170 s on the typical class and passed 300 s on the slow one
(45234099713, 45204597820), with no hang: per seed the heal's round counts repeat exactly
from run to run.

## Where the time goes

Each phase ends in waiting rounds on a healed network, and their count, not the faults
before them, sets most of the cost:

- **The Driver simulation's healing** waits out a stalled image transfer. Nothing retransmits
  a lost frame, so the transfer ends at the leader's `Timing::snapshot_timeout` and starts over
  (docs/DRIVER.md, Install). With the production 60 s against a 200 ms `election_max`, the
  seeds holding such a transfer when the network healed (4 of 20 at the default range) each spent
  about 13,500 quiet rounds, 2.5 times the rest of the Driver phase. The simulation scales
  `snapshot_timeout` with its other timers, 1 s (5x `election_max`, as the core simulation's
  3 s is 2.5x its 1.2 s). A stalled transfer now restarts within the run as well, so more seeds
  exercise the timeout and the restart, and healing waits about 200 rounds for one.
- **The core simulation's healing** proposes a new entry every 50 quiet rounds and ends once a
  majority of the voters in force applies it, within 713 rounds on seeds 1-200 (median 130),
  instead of idling 2,000 rounds before the first proposal and 4,000 in all. It fails a seed
  whose majority has not applied within 2,000 rounds, the budget the old loop gave after its
  first proposal.

| ASan, one run, the same host and load | core seeds | Driver seeds | total |
|---|---|---|---|
| 60 s `snapshot_timeout`, 4,000 heal rounds | 114 s | 272 s | 387 s |
| 1 s `snapshot_timeout` | 114 s | 93 s | 208 s |

With both, one ASan run takes 91-124 s on that host, and 57-59 s on CI's typical class
(171 s before) and 112.5 s on its slow class, under half the TIMEOUT.

## Output

`main` line-buffers stdout, so a run that times out still shows each phase that completed
(closing, quiescence, the core seeds' summary); the Driver phase prints its summary at its end.
