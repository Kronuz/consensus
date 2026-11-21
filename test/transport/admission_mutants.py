#!/usr/bin/env python3
"""Writes a node-admission mutant with mutant_commit.py's machinery: a commit on top of REF whose
only change is one seeded fault. The plain admission_nodes_test run at it with `campaign job submit
--fails-without --commit SHA` proves the test catches the fault.

    test/transport/admission_mutants.py REF NAME   prints the mutant commit's sha
    test/transport/admission_mutants.py --list      names every mutant"""
import os
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import mutant_commit  # noqa: E402

T = "include/consensus/transport/"
mutant_commit.MUTANTS = {
    # A1, pre-approved pins: a dial takes any verified certificate as the node it meant to reach
    "no-pin-check": (T + "node_pins.h", '\t\tif (!found)\n\t\t\trefuse(PinFault::unpinned, "unpinned peer certificate");',
                     "\t\tif (!found)\n\t\t\treturn expected;"),
    # A1, cluster CA: a first join re-pins a node pinned to another certificate
    "no-repin-check": (T + "node_pins.h", "\t\tif (entry.pin)\n\t\t\trefuse(PinFault::repinned",
                       "\t\tif (false)\n\t\t\trefuse(PinFault::repinned"),
    # A5: an issuer takes any token
    "any-token": (T + "admission_join.h", "\t\tif (secret.size() != secret_.size() || CRYPTO_memcmp(secret.data(), secret_.data(), secret_.size()) != 0)",
                  "\t\tif (false)"),
    # connection_test peer_gone: a connection's writes raise SIGPIPE on a closed peer
    "send-signals": (T + "connection.h", "data, std::size_t(n), MSG_NOSIGNAL);", "data, std::size_t(n), 0);"),
    # A2: a removal leaves the node's links up
    "remove-keeps-links": (T + "links.h", "\t\t\t\t\tif (link)\n\t\t\t\t\t\tlink->connection->abort();",
                           "\t\t\t\t\tif (link)\n\t\t\t\t\t\t(void)link;"),
}

if __name__ == "__main__":
    sys.exit(mutant_commit.main())
