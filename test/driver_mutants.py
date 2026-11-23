#!/usr/bin/env python3
# Copies include/ to OUT/include with one seeded fault, which waiter_test, driver_test,
# membership_test or the Driver simulation must catch (under ASan for the resume-before-unlink
# pair). Each names its header, the text it replaces and the replacement, or a list of those:
#   driver_mutants.py NAME OUT      (NAME "list" prints the names; "check" applies every mutant
#                                    into OUT and fails if any pattern does not match exactly once)
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
    # A local checkpoint captures none of the retained log above its boundary (msg-1).
    "checkpoint-drops-suffix": ("core.h",
        "\t\tif (capture_mode_) { captured_ = std::make_unique<FrozenCheckpoint>(FrozenCheckpoint(*this, id, state, entries_.size() - first)); }\n",
        "\t\tif (capture_mode_) { captured_ = std::make_unique<FrozenCheckpoint>(FrozenCheckpoint(*this, id, state, 0)); }\n",
    ),
    # A shared append acknowledges its record (Persisted) once staged, before its loop's turn
    # barrier makes it durable; the Driver simulation's power cuts catch it.
    "ack-before-turn-barrier": ("worker.h",
        "\t\t\t\tif (!io_job_->done()) {\n\t\t\t\t\tif (io_persist_token_ && core_->busy()) { ingest(core_->step(current_time)); }\n",
        "\t\t\t\tif (!io_job_->done() && io_persist_token_ && store_.shared()) {\n\t\t\t\t\tauto token = *io_persist_token_; io_persist_token_.reset();\n\t\t\t\t\tingest(core_->step(current_time)); ingest(core_->step(Persisted{token})); ++foreground_; return TurnResult::Completed;\n\t\t\t\t}\n\t\t\t\tif (!io_job_->done()) {\n\t\t\t\t\tif (io_persist_token_ && core_->busy()) { ingest(core_->step(current_time)); }\n",
    ),
    # A repair below base_index passes the Core's check (tools/mutants.sh's), which the Driver
    # simulation's repairs below a checkpoint reach.
    "repair-below-base": ("core.h",
        "if (b.index < base_index_ || b.index > durable_.commit_index ||",
        "if (b.index > durable_.commit_index ||",
    ),
    # D3's removal is proposed from any drive, not only an admitted Tick: the Persisted after
    # the promotion's commit then persists with no reservation (driver_replace_test).
    "removal-from-any-drive": ("core.h",
        "\t\tif (tick) {\n\t\t\tif (auto next = removal())",
        "\t\tif (true) {\n\t\t\tif (auto next = removal())",
    ),
    # A leader's Tick is admitted with no reservation though its removal is due (driver_replace_test).
    "removal-tick-unplanned": ("worker.h",
        "plan = removal ? plan_event(ChangeMembership{0, std::move(*removal)}, false, limits_, scheduling_.retained) : EventAdmissionPlan{};",
        "plan = EventAdmissionPlan{}; (void)removal;",
    ),
    # Both: the code before D3's funded removal, where Xapiand's placement repair fenced
    # (driver_replace_test's seed 26 has its shape).
    "removal-unreserved": [
        ("core.h", "\t\tif (tick) {\n\t\t\tif (auto next = removal())", "\t\tif (true) {\n\t\t\tif (auto next = removal())"),
        ("worker.h", "plan = removal ? plan_event(ChangeMembership{0, std::move(*removal)}, false, limits_, scheduling_.retained) : EventAdmissionPlan{};",
            "plan = EventAdmissionPlan{}; (void)removal;"),
    ],
    # A witness incarnation may become a learner or voter (R16).
    "witness-becomes-data": ("core.h",
        "\t\t\tif (old && old->role == MemberRole::Witness && x.role != MemberRole::Witness) { reject(RejectReason::InvalidMembership); return; }\n",
        "",
    ),
}

ROOT = pathlib.Path(__file__).resolve().parent.parent

def apply(name, out):
    """Copies include/ to out/include with the mutant applied; returns an error text or None.
    A pattern that does not match exactly once is an error: the mutant would prove nothing."""
    shutil.rmtree(out / "include", ignore_errors=True)
    shutil.copytree(ROOT / "include", out / "include")
    edits = MUTANTS[name]
    for header, search, replace in edits if isinstance(edits, list) else [edits]:
        path = out / "include" / "consensus" / header
        text = path.read_text()
        if text.count(search) != 1:
            return "mutation point not found once: " + name
        path.write_text(text.replace(search, replace, 1))
    return None

def main():
    name = sys.argv[1]
    if name == "list":
        print("\n".join(MUTANTS))
        return
    if name == "check":  # applies every mutant into scratch directory OUT; fails if any does not apply
        out = pathlib.Path(sys.argv[2])
        errors = [e for n in MUTANTS if (e := apply(n, out))]
        shutil.rmtree(out / "include", ignore_errors=True)
        if errors:
            sys.exit("\n".join(errors))
        print("all %d mutants apply" % len(MUTANTS))
        return
    error = apply(name, pathlib.Path(sys.argv[2]))
    if error:
        sys.exit(error)

main()
