#!/usr/bin/env python3
# Seeds one named fault into include/consensus/ of the checkout in the current directory:
#   test/faultfs_mutants.py NAME      (NAME "list" prints the names)
# Each mutant is (header, search, replace); the search must occur exactly `count` times.
import pathlib
import sys

MUTANTS = {
    # A Worker no longer fences itself when its Store was fenced under it (wave_open cut).
    "store-fence-unnamed": ("worker.h", 2,
        '\t\tif (store_.fenced() && !failed_) { fail("storage fenced outside worker"); }\n',
        "",
    ),
    # A throw that ends the Driver's loop leaves its callers suspended (close_crash).
    "loop-throw-suspends": ("driver.h", 1,
        "} catch (...) { shutdown(); throw; }",
        "} catch (...) { throw; }",
    ),
    # Messages and deliveries deferred behind a persistence go out before it is durable
    # (tools/mutants.sh's "sends before durability").
    "sends-before-durability": ("core.h", 1,
        "if (batch.hard || batch.log) { persist(std::move(batch), std::move(deferred), output); }",
        "if (batch.hard || batch.log) { for (auto& a : deferred) { output.push_back(a); } persist(std::move(batch), {}, output); }",
    ),
    # A local checkpoint captures none of the retained log above its boundary (driver_mutants.py).
    "checkpoint-drops-suffix": ("core.h", 1,
        "\t\tif (capture_mode_) { captured_ = FrozenCheckpoint(*this, id, state, entries_.size() - first); }\n",
        "\t\tif (capture_mode_) { captured_ = FrozenCheckpoint(*this, id, state, 0); }\n",
    ),
    # A repair below base_index passes the Core's check (tools/mutants.sh).
    "repair-below-base": ("core.h", 1,
        "if (b.index < base_index_ || b.index > durable_.commit_index ||",
        "if (b.index > durable_.commit_index ||",
    ),
}


def main() -> None:
    name = sys.argv[1]
    if name == "list":
        print("\n".join(MUTANTS))
        return
    header, count, search, replace = MUTANTS[name]
    path = pathlib.Path("include/consensus") / header
    text = path.read_text()
    if text.count(search) != count:
        sys.exit(f"mutation point of {name} found {text.count(search)} times, not {count}")
    path.write_text(text.replace(search, replace))
    print(f"mutant {name} applied to {path}")


main()
