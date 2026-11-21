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
    "no-backoff": (T + "seeds.h", "e->next = Clock::now() + backoff_after(e->failures++);", "e->next = Clock::now();"),
    # drops: another cluster's announcement becomes a seed
    "any-cluster": (T + "seeds.h", "if (a.cluster != self_.cluster)", "if (false)"),
    # drops: services the listener did not ask for are seeded
    "unwanted-seeded": (T + "seeds.h", "if (wanted_.contains(s.name))", "if (true)"),
    # codec: a trailing byte is read as a valid announcement
    "trailing-accepted": (T + "seeds_codec.h", "return !bytes.empty() || announcement_fault(out)", "return announcement_fault(out)"),
    # rate: no burst on start
    "no-burst": (T + "seeds.h", "burst_left_ = options_.burst;", "burst_left_ = 1;"),
    # rate: announcements every burst spacing, the interval ignored
    "no-interval": (T + "seeds.h", "wait = options_.interval + std::chrono::milliseconds(jitter);", "wait = options_.burst_spacing;"),
    # forged_rotating: dial state keyed by the forger-chosen sender, not the dial target
    "sender-keyed": (T + "seeds.h", "const Key key{from_.address(), services};",
                     "const Key key{from_.address(), {{std::string(a.sender.begin(), a.sender.end()), 1}}};"),
    # drops: one source address fills the set
    "no-address-cap": (T + "seeds.h", "if (held(key.first) >= options_.per_address)", "if (false)"),
    # recovers, unavailable: a failed start() is final
    "no-setup-retry": (T + "seeds.h", "if (!e && !stopping_)\n\t\t\t\topen();", "if (false)\n\t\t\t\topen();"),
    # receive_errors: a failed receive re-arms at once
    "receive-spins": (T + "seeds.h", "\t\tif (stopping_)\n\t\t\treturn;\n\t\treceive_timer_.expires_after(backoff_after(receive_failures_++));",
                      "\t\tif (stopping_)\n\t\t\treturn;\n\t\treceive_timer_.expires_after(std::chrono::seconds(0));"),
    # throws: a throwing on_dial leaves its seed dialing forever
    "throw-keeps-entry": (T + "seeds.h", "\t\t\t\tfailed(id);\n\t\t\t\tthrow;", "\t\t\t\tthrow;"),
    # require: CI does not require multicast, so a broken platform reads as skipped
    "ci-skips": ("test/transport/seeds_test.cc", 'return std::getenv("CI") || std::getenv("CONSENSUS_SEEDS_REQUIRE");',
                 'return std::getenv("CONSENSUS_SEEDS_REQUIRE");'),
    # forged (Linux, where SO_NOSIGPIPE is missing): the host does not ignore SIGPIPE
    "sigpipe-default": ("test/transport/seeds_test.cc", "\tstd::signal(SIGPIPE, SIG_IGN);", "\t(void)0;"),
}

if __name__ == "__main__":
    sys.exit(mutant_commit.main())
