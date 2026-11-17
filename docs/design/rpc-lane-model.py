#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
"""Exhaustive model of the RPC lane's calls and credit (docs/design/rpc-lane.md, D3-D6).

One connection, ordered in each direction; a caller with `calls` calls and a server. Every
interleaving of sends, receives, consumer reads, credit grants, cancels, deadlines on either
side, server errors, overloaded refusals and one connection close is enumerated. In units of
one body chunk. Checks, in every reachable state:
  I1 a receiver never holds more body than it granted, and grants never exceed its budget
  I2 an honest run never sees a protocol violation
  I3 no call completes twice, on either side
  I4 every final state: each call completed once at the caller, each started handler once,
     both budgets back to zero, no live call, nothing in flight
  I5 a successful call delivered its whole body, in order, both ways
A state where only failures (cancel, deadlines, errors, close) can happen is final: a call
that needs one of them to end is a deadlock, and I4 reports it.
Usage: rpc-lane-model.py [mutant]; exit 1 on a violation, so each mutant runs --fails-without.
"""

import sys
from collections import deque

MUTANT = sys.argv[1] if len(sys.argv) > 1 else ""
MUTANTS = {
    "",
    "forget-is-violation",  # frames for a call no longer live close the lane (fetch's rule today)
    "grant-without-reserve",  # credit granted beyond the budget
    "no-release-on-close",  # a close completes calls but keeps their budget
    "no-regrant",  # credit only in the Call frame: a body past one window never finishes
    "grant-on-receive",  # credit topped up when bytes arrive, not when the consumer takes them
}
assert MUTANT in MUTANTS, MUTANT

REQ, RESP = 2, 3  # request and response body chunks
WINDOW = 2  # per-call window
BUDGET_CALLER, BUDGET_SERVER = 2, 1  # per-host lane budgets; the server's is under one window
CALLS = 2

# Caller call: phase (0 unsent, 1 live, 2 done), outcome, completions, request chunks sent,
# request limit (server's credit), response limit (our grant), received, consumed.
# Server call: phase (0 unseen, 1 live, 2 done, 3 refused), completions, request limit (our grant),
# received, consumed, response chunks sent, response limit (caller's credit).
VIOLATION = "violation"


def initial():
    caller = tuple((0, None, 0, 0, 0, 0, 0, 0) for _ in range(CALLS))
    server = tuple((0, 0, 0, 0, 0, 0, 0) for _ in range(CALLS))
    return (caller, server, (), (), False)  # c2s, s2c channels, closed


def reserved_caller(caller):
    return sum(c[5] - c[7] for c in caller if c[0] in (1, 4))  # 4: the no-release-on-close leak


def reserved_server(server):
    return sum(s[2] - s[4] for s in server if s[0] == 1)


def setc(caller, i, **kw):
    names = ("phase", "outcome", "done", "sent", "rlimit", "glimit", "recv", "cons")
    c = dict(zip(names, caller[i]))
    c.update(kw)
    return caller[:i] + (tuple(c[n] for n in names),) + caller[i + 1 :]


def sets(server, i, **kw):
    names = ("phase", "done", "glimit", "recv", "cons", "sent", "rlimit")
    s = dict(zip(names, server[i]))
    s.update(kw)
    return server[:i] + (tuple(s[n] for n in names),) + server[i + 1 :]


def grant_target(budget, reserved_others, window):
    if MUTANT == "grant-without-reserve":
        return window
    return min(window, budget - reserved_others)


def successors(state):
    caller, server, c2s, s2c, closed = state
    out = []
    if not closed:
        for i, c in enumerate(caller):
            phase, _outcome, done, sent, rlimit, glimit, recv, cons = c
            if phase == 0:
                # Call: the caller grants the response window from its budget before sending.
                grant = grant_target(BUDGET_CALLER, reserved_caller(caller), WINDOW)
                if grant > 0:
                    out.append(
                        (
                            "call",
                            (
                                setc(caller, i, phase=1, glimit=grant),
                                server,
                                c2s + (("call", i, grant),),
                                s2c,
                                closed,
                            ),
                        )
                    )
            elif phase == 1:
                if sent < REQ and sent < rlimit:
                    out.append(
                        (
                            "req-body",
                            (
                                setc(caller, i, sent=sent + 1),
                                server,
                                c2s + (("body", i, sent),),
                                s2c,
                                closed,
                            ),
                        )
                    )
                if cons < recv:  # the consumer reads one chunk; the caller tops its grant up
                    cons2 = cons + 1
                    caller2 = setc(caller, i, cons=cons2)
                    others = reserved_caller(caller2) - (glimit - cons2)
                    target = grant_target(BUDGET_CALLER, others, WINDOW)
                    new_limit = min(cons2 + target, RESP)
                    msgs = c2s
                    if new_limit > glimit and MUTANT != "no-regrant":
                        caller2 = setc(caller2, i, glimit=new_limit)
                        msgs = c2s + (("credit", i, new_limit),)
                    if cons2 == RESP and sent == REQ:
                        caller2 = setc(caller2, i, phase=2, outcome="ok", done=done + 1)
                    out.append(("read", (caller2, server, msgs, s2c, closed)))
                for why in ("cancel", "deadline"):
                    out.append(
                        (
                            why,
                            (
                                setc(caller, i, phase=2, outcome=why, done=done + 1),
                                server,
                                c2s + (("cancel", i),),
                                s2c,
                                closed,
                            ),
                        )
                    )
        for i, s in enumerate(server):
            phase, done, glimit, recv, cons, sent, rlimit = s
            if phase != 1:
                continue
            if cons < recv:
                cons2 = cons + 1
                server2 = sets(server, i, cons=cons2)
                others = reserved_server(server2) - (glimit - cons2)
                target = grant_target(BUDGET_SERVER, others, WINDOW)
                new_limit = min(cons2 + target, REQ)
                msgs = s2c
                if new_limit > glimit:
                    server2 = sets(server2, i, glimit=new_limit)
                    msgs = s2c + (("credit", i, new_limit),)
                out.append(("consume", (caller, server2, c2s, msgs, closed)))
            if cons == REQ and sent < RESP and sent < rlimit:
                last = sent + 1 == RESP
                server2 = sets(server, i, sent=sent + 1)
                if last:
                    server2 = sets(server2, i, phase=2, done=done + 1)
                out.append(
                    ("resp-body", (caller, server2, c2s, s2c + (("body", i, sent),), closed))
                )
            for why in ("error", "server-deadline"):
                out.append(
                    (
                        why,
                        (
                            caller,
                            sets(server, i, phase=2, done=done + 1),
                            c2s,
                            s2c + (("error", i),),
                            closed,
                        ),
                    )
                )
        if c2s:
            out.append(("deliver-c2s", server_receive(state)))
        if s2c:
            out.append(("deliver-s2c", caller_receive(state)))
        # The connection closes: every live call ends on both sides, in-flight frames are lost.
        caller2 = caller
        for i, c in enumerate(caller):
            if c[0] == 1:
                caller2 = setc(caller2, i, phase=2, outcome="abort", done=c[2] + 1)
                if MUTANT == "no-release-on-close":
                    caller2 = setc(caller2, i, phase=4)
            elif c[0] == 0:
                caller2 = setc(caller2, i, phase=2, outcome="abort", done=c[2] + 1)
        server2 = server
        for i, s in enumerate(server):
            if s[0] == 1:
                server2 = sets(server2, i, phase=2, done=s[1] + 1)
        out.append(("close", (caller2, server2, (), (), True)))
    return out


def server_receive(state):
    caller, server, c2s, s2c, closed = state
    frame, c2s = c2s[0], c2s[1:]
    kind, i = frame[0], frame[1]
    s = server[i]
    live = s[0] == 1
    if kind == "call":
        if s[0] != 0:
            return VIOLATION
        target = grant_target(BUDGET_SERVER, reserved_server(server), WINDOW)
        if target <= 0:  # no budget for its request: refused, never queued (D5)
            return (caller, sets(server, i, phase=3), c2s, s2c + (("error", i),), closed)
        limit = min(target, REQ)
        server = sets(server, i, phase=1, glimit=limit, rlimit=frame[2])
        return (caller, server, c2s, s2c + (("credit", i, limit),) if limit else s2c, closed)
    if not live:
        if MUTANT == "forget-is-violation":
            return VIOLATION
        return (caller, server, c2s, s2c, closed)  # retired: discarded and counted
    if kind == "body":
        if frame[2] != s[3] or s[3] + 1 > s[2]:
            return VIOLATION
        return (caller, sets(server, i, recv=s[3] + 1), c2s, s2c, closed)
    if kind == "credit":
        if frame[2] <= s[6]:
            return VIOLATION
        return (caller, sets(server, i, rlimit=frame[2]), c2s, s2c, closed)
    if kind == "cancel":
        return (caller, sets(server, i, phase=2, done=s[1] + 1), c2s, s2c, closed)
    return VIOLATION


def caller_receive(state):
    caller, server, c2s, s2c, closed = state
    frame, s2c = s2c[0], s2c[1:]
    kind, i = frame[0], frame[1]
    c = caller[i]
    if c[0] != 1:
        if MUTANT == "forget-is-violation":
            return VIOLATION
        return (caller, server, c2s, s2c, closed)
    if kind == "body":
        if frame[2] != c[6] or c[6] + 1 > c[5]:
            return VIOLATION
        caller = setc(caller, i, recv=c[6] + 1)
        if MUTANT == "grant-on-receive":
            new_limit = min(c[6] + 1 + WINDOW, RESP)
            if new_limit > c[5]:
                caller = setc(caller, i, glimit=new_limit)
                c2s = c2s + (("credit", i, new_limit),)
        return (caller, server, c2s, s2c, closed)
    if kind == "credit":
        if frame[2] <= c[4]:
            return VIOLATION
        return (setc(caller, i, rlimit=frame[2]), server, c2s, s2c, closed)
    if kind == "error":
        return (setc(caller, i, phase=2, outcome="error", done=c[2] + 1), server, c2s, s2c, closed)
    return VIOLATION


def check(state):
    caller, server = state[0], state[1]
    for c in caller:
        if c[2] > 1:
            return "I3 caller completed twice"
        if c[0] == 1 and c[6] - c[7] > c[5] - c[7]:
            return "I1 caller holds more than it granted"
        if c[1] == "ok" and (c[7] != RESP or c[3] != REQ):
            return "I5 success without the whole body"
    for s in server:
        if s[1] > 1:
            return "I3 handler completed twice"
        if s[0] == 1 and s[3] > s[2]:
            return "I1 server holds more than it granted"
    if reserved_caller(caller) > BUDGET_CALLER or reserved_server(server) > BUDGET_SERVER:
        return "I1 grants exceed the budget"
    return None


def final_check(state):
    caller, server, c2s, s2c = state[:4]
    if reserved_caller(caller) or reserved_server(server):
        return "I4 budget not released"
    if c2s or s2c:
        return "I4 frames left in flight"
    for c in caller:
        if c[0] != 2 or c[2] != 1:
            return "I4 a call did not complete once at the caller"
    for s in server:
        if s[0] == 1 or (s[0] == 2 and s[1] != 1):
            return "I4 a handler did not complete once"
    return None


FAILURES = {"cancel", "deadline", "error", "server-deadline", "close"}


def main():
    start = initial()
    seen = {start}
    queue = deque([start])
    finals = 0
    outcomes = set()
    while queue:
        state = queue.popleft()
        problem = check(state)
        nexts = successors(state)
        if not problem and all(label in FAILURES for label, _ in nexts):
            problem = final_check(state)
            finals += 1
            outcomes.add(tuple(c[1] for c in state[0]))
        for _, nxt in nexts:
            if nxt == VIOLATION:
                problem = "I2 protocol violation in an honest run"
                break
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
        if problem:
            print(f"model={MUTANT or 'design'} states={len(seen)} FAILS: {problem}")
            return 1
    print(
        f"model={MUTANT or 'design'} calls={CALLS} req={REQ} resp={RESP} window={WINDOW} "
        f"budgets={BUDGET_CALLER}/{BUDGET_SERVER} states={len(seen)} finals={finals} "
        f"outcomes={len(outcomes)} ok"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
