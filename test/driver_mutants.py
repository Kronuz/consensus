#!/usr/bin/env python3
# Copies include/ to OUT/include with one seeded fault, which waiter_test, driver_test or
# membership_test must catch (under ASan for the resume-before-unlink pair). Each names its
# header, the text it replaces and the replacement:
#   driver_mutants.py NAME OUT      (NAME "list" prints the names)
import pathlib, shutil, sys

MUTANTS = {
    # Completion resumes the caller before it unlinks the waiter from the group.
    "complete-resumes-before-unlink": ("driver.h",
        "\t\tunlink(g, w);\n\t\tif (!w.claim(Waiter::completed)) { return; }\n\t\tw.error = std::move(error);\n\t\tw.home.post(w.handle);\n",
        "\t\tif (!w.claim(Waiter::completed)) { unlink(g, w); return; }\n\t\tw.error = std::move(error);\n\t\tw.home.post(w.handle);\n\t\tunlink(g, w);\n",
    ),
    # A cancel's hop resumes the caller before it unlinks the waiter.
    "hop-resumes-before-unlink": ("driver.h",
        "\t\t\t\tif (w->owner) { w->hop_to(w->owner, *w); }\n\t\t\t\tw->home.post(w->handle);\n",
        "\t\t\t\tauto* owner = w->owner;\n\t\t\t\tw->home.post(w->handle);\n\t\t\t\tif (owner) { w->hop_to(owner, *w); }\n",
    ),
    # A waiter destroyed unresumed (teardown) stays linked.
    "destructor-leaves-linked": ("driver.h",
        "\t\t\tcallback.reset();  // waits for a callback running on another thread\n\t\t\tif (waiter.owner) { driver.unlink(*static_cast<Group*>(waiter.owner), waiter); }\n",
        "\t\t\tcallback.reset();  // waits for a callback running on another thread\n",
    ),
    # Close frees the group's state while waiters are still linked.
    "close-frees-with-waiters": ("driver.h",
        "\t\tif (others) { return; }\n",
        "",
    ),
    # Close after shutdown links its waiter on a loop nothing steps (ff0cdbc's close).
    "close-after-stop-links": ("driver.h",
        "\t\tif (stopping_) { call.fail(std::make_exception_ptr(task::cancelled{})); co_await call; co_return; }\n",
        "",
    ),
    # A Sink's throw mid-batch leaves the Worker unfenced (the rest of the batch lost).
    "sink-throw-unfenced": ("driver.h",
        "\t\t\tw.fence(g.fence_reason);\n",
        "",
    ),
    # A witness incarnation may become a learner or voter (R16).
    "witness-becomes-data": ("core.h",
        "\t\t\tif (old && old->role == MemberRole::Witness && x.role != MemberRole::Witness) { reject(RejectReason::InvalidMembership); return; }\n",
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
    header, search, replace = MUTANTS[name]
    path = out / "include" / "consensus" / header
    text = path.read_text()
    if text.count(search) != 1:
        sys.exit("mutation point not found once: " + name)
    path.write_text(text.replace(search, replace, 1))

main()
