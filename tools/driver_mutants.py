#!/usr/bin/env python3
# Copies include/ to OUT/include with one of the Driver's seeded faults, which waiter_test must
# catch (under ASan for the resume-before-unlink pair):
#   driver_mutants.py NAME OUT      (NAME "list" prints the names)
import pathlib, shutil, sys

MUTANTS = {
    # Completion resumes the caller before it unlinks the waiter from the group.
    "complete-resumes-before-unlink": (
        "\t\tunlink(g, w);\n\t\tif (!w.claim(Waiter::completed)) { return; }\n\t\tw.error = std::move(error);\n\t\tw.home.post(w.handle);\n",
        "\t\tif (!w.claim(Waiter::completed)) { unlink(g, w); return; }\n\t\tw.error = std::move(error);\n\t\tw.home.post(w.handle);\n\t\tunlink(g, w);\n",
    ),
    # A cancel's hop resumes the caller before it unlinks the waiter.
    "hop-resumes-before-unlink": (
        "\t\t\t\tif (w->owner) { w->hop_to(w->owner, *w); }\n\t\t\t\tw->home.post(w->handle);\n",
        "\t\t\t\tauto* owner = w->owner;\n\t\t\t\tw->home.post(w->handle);\n\t\t\t\tif (owner) { w->hop_to(owner, *w); }\n",
    ),
    # A waiter destroyed unresumed (teardown) stays linked.
    "destructor-leaves-linked": (
        "\t\t\tcallback.reset();  // waits for a callback running on another thread\n\t\t\tif (waiter.owner) { driver.unlink(*static_cast<Group*>(waiter.owner), waiter); }\n",
        "\t\t\tcallback.reset();  // waits for a callback running on another thread\n",
    ),
    # Close frees the group's state while waiters are still linked.
    "close-frees-with-waiters": (
        "\t\tif (others) { return; }\n",
        "",
    ),
}

def main():
    name = sys.argv[1]
    if name == "list":
        print("\n".join(MUTANTS))
        return
    out = pathlib.Path(sys.argv[2])
    root = pathlib.Path(__file__).resolve().parent.parent
    shutil.rmtree(out / "include", ignore_errors=True)
    shutil.copytree(root / "include", out / "include")
    path = out / "include" / "consensus" / "driver.h"
    search, replace = MUTANTS[name]
    text = path.read_text()
    if text.count(search) != 1:
        sys.exit("mutation point not found once: " + name)
    path.write_text(text.replace(search, replace, 1))

main()
