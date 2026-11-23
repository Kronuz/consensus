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
    # A1: a secret matches any digest (issue: a wrong secret joins)
    "any-token": (T + "admission_join.h", "\treturn CRYPTO_memcmp(presented.data(), digest.data(), digest.size()) == 0;",
                  "\treturn (void)presented, true;"),
    # A1: the joiner takes a member of another cluster (issue: the cluster token's secret goes out)
    "join-any-cluster": (T + "admission_join.h", "\tif (offer.cluster != token.cluster)\n", "\tif (false)\n"),
    # A1: the joiner takes the offer's first CA, whatever key the token pins (issue: ca)
    "join-unpinned-ca": (T + "admission_join.h", "c && key_pin(c.get()) == token.ca) {", "c) {"),
    # A1: a version 1 token reads as malformed, not version (issue)
    "token-no-version": (T + "admission_join.h", "\t\tthrow JoinRefused(JoinFault::version, \"the token is not version 2\");",
                         "\t\tthrow JoinRefused(JoinFault::malformed, \"the token is not version 2\");"),
    # A2: the issuer signs each request anew (issue: two joins with one key get two certificates)
    "sign-each-time": (T + "admission_join.h", "if (recent.node != assignment.node || recent.certificate.empty()) {", "if (true) {"),
    # A2: Joined::save writes each file in place (save: a killed save leaves a part)
    "save-in-place": [
        (T + "admission_join.h", "const int fd = ::open(staged.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, secret ? 0600 : 0666);",
         "const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW | O_CLOEXEC, secret ? 0600 : 0666);"),
        (T + "admission_join.h", "\tif (::rename(staged.c_str(), path.c_str()) != 0)\n", "\tif (false)\n"),
    ],
    # connection_test peer_gone: a connection's writes raise SIGPIPE on a closed peer
    "send-signals": (T + "connection.h", "data, std::size_t(n), MSG_NOSIGNAL);", "data, std::size_t(n), 0);"),
    # R5: a connection authenticated before its node's removal still becomes a link (epoch_*)
    "links-no-epoch": (T + "links.h", "return it != since_.end() && pins_.current(link.peer, it->second);", "return true;"),
    # the joiner sends its secret to a member whose own leaf is another CA's (issue)
    "join-unchained-issuer": (T + "admission_join.h",
                              "!chains_to(member.get(), SSL_get_peer_cert_chain(stream.native_handle()), ca.get()))", "false)"),
    # Joined::save writes the key through an ofstream, then chmods it (save); the checked writer
    # stays, unused
    "save-chmod-after": [
        (T + "admission_join.h", "#include <filesystem>\n", "#include <filesystem>\n#include <fstream>\n"),
        (T + "admission_join.h", "inline std::string replace_file(const std::filesystem::path &directory, const char *name, const std::string &bytes, bool secret) {\n",
         "inline std::string replace_file(const std::filesystem::path &directory, const char *name, const std::string &bytes, bool secret) {\n"
         "\tconst auto path = directory / name;\n"
         "\tstd::filesystem::remove(path);\n"
         "\tstd::ofstream(path, std::ios::binary) << bytes;\n"
         "\tif (secret)\n"
         "\t\tstd::filesystem::permissions(path, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);\n"
         "\treturn path.string();\n"
         "}\n"
         "[[maybe_unused]] inline std::string checked_file(const std::filesystem::path &directory, const char *name, const std::string &bytes, bool secret) {\n"),
    ],
    # A4: pins over the certificate's DER, not its key (renew: a renewed certificate is refused)
    "pin-der": (T + "node_pins.h", "\t\tconst auto digest = key_pin(leaf.get());", "\t\tconst auto digest = certificate_fingerprint(leaf.get());"),
    # A4: a reload keeps the links whose chain no longer verifies (reload)
    "reload-keeps-links": (T + "links.h", "\t\t\t\t\t++stats_.unverified;\n\t\t\t\t\tlink->connection->abort();", "\t\t\t\t\t++stats_.unverified;"),
    # A4: a reload keeps verifying against the old CAs (reload)
    "reload-keeps-trust": (T + "node_pins.h", "\t\td.trust = std::move(trust);", "\t\t(void)trust;"),
    # A2: a removal leaves the node's links up
    "remove-keeps-links": (T + "links.h", "\t\t\t\t\tif (link)\n\t\t\t\t\t\tlink->connection->abort();",
                           "\t\t\t\t\tif (link)\n\t\t\t\t\t\t(void)link;"),
}

if __name__ == "__main__":
    sys.exit(mutant_commit.main())
