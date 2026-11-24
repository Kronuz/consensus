#!/usr/bin/env python3
"""Writes a follower-read mutant (docs/mechanisms/follower-read.md): a commit on top of REF whose
only change drops one rule. The plain test at it, `follower_read_test CASE` with the case named
below (`campaign job submit --fails-without --commit SHA`), proves the case catches it.

    test/follower_read_mutants.py REF NAME       prints the mutant commit's sha
    test/follower_read_mutants.py --list         names every mutant and its case"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "transport"))
import mutant_commit  # noqa: E402

C = "include/consensus/core.h"
CODEC = "include/consensus/transport/codec.h"
# name: (case, (file, search, replace))
CASES = {
    # A follower completes a read once answered, before it applied the index.
    "apply-skipped": ("confirmed", (C, "if (!read.index || applied_ < read.index) { return false; }", "if (!read.index) { return false; }")),
    # A leader answers a forwarded read without its quorum's confirmation.
    "ready-without-quorum": ("stale", (C, "if (read->term != hard_.term || acknowledged(*read) < quorum()) { ++read; }",
                                       "if (read->term != hard_.term || (!read->from && acknowledged(*read) < quorum())) { ++read; }")),
    # A follower keeps its read when its leader changes in the same term.
    "leader-change-kept": ("lost", (C, "if (read.term == hard_.term && read.leader == leader) { return false; }",
                                    "if (read.term == hard_.term) { return false; }")),
    # A follower's own campaign keeps its forwarded reads.
    "campaign-keeps-reads": ("term", (C, "\t\tfor (const auto& read : forwarded_) { deferred.emplace_back(Reject{read.request, RejectReason::NotLeader}); }\n\t\tforwarded_.clear();\n", "")),
    # An answer from any peer, or at any term, counts.
    "any-peer": ("identity", (C, "read->index || source != read->leader || response.term", "read->index || response.term")),
    "any-term": ("identity", (C, "|| response.term != read->term || hard_.term", "|| hard_.term")),
    # The per-start nonce is not checked: a restart's reused token matches the old answer (review V1).
    "start-ignored": ("restart", (C, "if (response.start != start_ || role_ != Role::Follower) { return; }", "if (role_ != Role::Follower) { return; }")),
    # No deadline from the send to the apply (review V4).
    "no-deadline": ("deadline", (C, "if (now_ - read.sent < timing_.election_max) { return false; }", "if (true) { return false; }")),
    # The deadline is kept but not among the follower's: nothing ticks it there.
    "deadline-unarmed": ("deadline", (C, "install_deadline(), forwarded_deadline()}", "install_deadline()}")),
    # A read admitted during another's confirmation shares that probe (review V5).
    "probe-shared": ("in-flight", (C, "\t\tread.probe = token();\n", "\t\tread.probe = reads_.empty() ? token() : reads_.back().probe;\n")),
    # One answer completes every pending read at the follower.
    "answer-completes-all": ("in-flight", (C, "case ReadIndexReply::Ready: read->index = response.index; return;",
                                           "case ReadIndexReply::Ready: for (auto& r : forwarded_) { r.index = response.index; } return;")),
    # A leader refuses a forwarded read without answering it (review V3).
    "refusal-unsent": ("refusals", (C, "if (read.from) { output.emplace_back(Send{read.from, ReadIndexResponse{hard_.term, read.start, read.token, reason, 0}}); return; }",
                                    "if (read.from) { return; }")),
    # A follower rests holding a read (review V7).
    "rests-with-read": ("rest", (C, "&& !reinstall_ && forwarded_.empty();", "&& !reinstall_;")),
    # A follower forwards with no start nonce.
    "nonce-unchecked": ("nonce", (C, "if (role_ != Role::Follower || !leader_ || !start_)", "if (role_ != Role::Follower || !leader_)")),
    # A restart reuses the previous start nonce (a repeating std::random_device): a re-injected
    # pre-restart answer completes a new read below an earlier commit (review N1's probe, mode 3).
    "nonce-reused": ("randomized", ("test/follower_read_test.cc", "n.restart(id, ++nonce);", "n.restart(id, n.host(id).nonce); (void)nonce;")),
    # The decoder takes a refusal with an index, or Ready without one.
    "codec-unchecked": ("codec", (CODEC, "if (outcome > static_cast<unsigned>(ReadIndexReply::NotLeader) || (outcome == static_cast<unsigned>(ReadIndexReply::Ready)) != (index != 0))",
                                  "if (outcome > static_cast<unsigned>(ReadIndexReply::NotLeader))")),
}
mutant_commit.MUTANTS = {name: edit for name, (case, edit) in CASES.items()}

if __name__ == "__main__":
    if sys.argv[1:] == ["--list"]:
        print("\n".join(f"{name} {case}" for name, (case, edit) in CASES.items()))
        sys.exit(0)
    sys.exit(mutant_commit.main())
