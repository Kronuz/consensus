#!/usr/bin/env python3
"""The faults seeds_test.cc must catch, written as mutant commits by mutant_commit.py:

    test/transport/seeds_mutants.py REF NAME     prints the mutant commit's sha
    test/transport/seeds_mutants.py --list        names every mutant

Run the case named beside each at the mutant with `campaign job submit --fails-without`."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mutant_commit  # noqa: E402

T = "include/consensus/transport/"
mutant_commit.MUTANTS = {
    # forged: a failed dial waits no backoff, so every announcement dials again
    "no-backoff": (T + "seeds.h", "e->next = Clock::now() + std::min(wait, options_.backoff_max);", "e->next = Clock::now();"),
    # drops: another cluster's announcement becomes a seed
    "any-cluster": (T + "seeds.h", "if (a.cluster != self_.cluster)", "if (false)"),
    # drops: services the listener did not ask for are seeded
    "unwanted-seeded": (T + "seeds.h", "if (wanted_.contains(s.name))", "if (true)"),
    # codec: a trailing byte is read as a valid announcement
    "trailing-accepted": (T + "seeds_codec.h", "return !bytes.empty() || announcement_fault(out)", "return announcement_fault(out)"),
    # rate: no burst on start
    "no-burst": (T + "seeds.h", "burst_left_ = options_.burst;", "burst_left_ = 0;"),
    # rate: announcements every burst spacing, the interval ignored
    "no-interval": (T + "seeds.h", "wait = options_.interval + std::chrono::milliseconds(jitter);", "wait = options_.burst_spacing;"),
}

if __name__ == "__main__":
    sys.exit(mutant_commit.main())
