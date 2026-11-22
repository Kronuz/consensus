#!/usr/bin/env python3
"""Writes a PeerOptions::wake mutant (docs/mechanisms/peers-wake.md): a commit on top of REF whose
only change in peers.h drops one owner-work call or adds one where there is no owner work. The
plain test at it (`campaign job submit --fails-without --commit SHA`) proves
test/peers_wake_test.cc catches it.

    test/peers_wake_mutants.py REF NAME       prints the mutant commit's sha
    test/peers_wake_mutants.py --list         names every mutant"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "transport"))
import mutant_commit  # noqa: E402

P = "include/consensus/transport/peers.h"
mutant_commit.MUTANTS = {
    # A Control frame retained for admit().
    "control": (P, "\t\t\t\t\t++stats_.received;\n\t\t\t\t\twake();\n", "\t\t\t\t\t++stats_.received;\n"),
    # A Bulk frame the snapshot channel took or ran.
    "bulk-taken": (P, "if (result.consumed || result.result == kronuz::consensus::TransferReceive::Accepted ||",
                   "if (false ||"),
    # A Bulk frame the channel refused, closing its session.
    "bulk-refused": (P, "(result.result == kronuz::consensus::TransferReceive::Closed && open))",
                     "(false && open))"),
    # A snapshot frame written and consumed.
    "consumed": (P, "if (flight.consume_if_current(node_->channel()))\n\t\t\t\t\t\t\twake();",
                 "flight.consume_if_current(node_->channel());"),
    # A Bulk session opened.
    "opened": (P, "\t\t\t\tif (node_->channel().session_open(*connection->trusted))\n\t\t\t\t\twake();\n", ""),
    # An open Bulk session lost, on every path.
    "lost": (P, "lost = node_->channel().session_open(*connection->trusted);", "lost = false;"),
    # A fetch's second deadline closing the lane.
    "timer": (P, "owner->end(connection);", "owner->close(connection);"),
    # Never otherwise: every lane that opens wakes, Control included.
    "any-lane-opened": (P, "\t\t\t\tif (node_->channel().session_open(*connection->trusted))\n\t\t\t\t\twake();\n\t\t\t}\n",
                        "\t\t\t}\n\t\t\twake();\n"),
    # Never from a host call: stop() wakes for the sessions it closes.
    "stop-wakes": (P, "\t\tfor (auto &connection : connections_)\n\t\t\tclose(connection);", "\t\tfor (auto &connection : connections_)\n\t\t\tend(connection);"),
}

if __name__ == "__main__":
    sys.exit(mutant_commit.main())
