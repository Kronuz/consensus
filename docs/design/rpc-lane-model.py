#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
"""Exhaustive model of the RPC lane's calls, credit and drain (docs/design/rpc-lane.md, D4-D6).

One connection, a caller with two calls and a server, in units of one body chunk. Every
interleaving is enumerated: sends, receives, consumer reads, grants, cancels (explicit or a
deadline: the same frames), server errors (a handler's or its deadline), `overloaded` refusals,
and one abrupt close. Rules (RULES, a comma list; `drain,reorder` by default, since all three
together outgrow a Python run): `drain` adds one `Drain` from the server and its graceful close
(D4); `reorder` lets a small frame overtake queued chunks and calls' chunks interleave (D1; a
call's own chunks and the small frames stay in order, and the graceful close goes last);
`early-end` lets the server end its response before it reads the request (D4).
Checks, in every reachable state:
  I1 a receiver never holds more body than it granted, and grants never exceed its budget
  I2 an honest run never sees a protocol violation
  I3 no call completes twice, on either side
  I4 every final state (only cancels, errors, drains and closes remain possible): each call
     completed once at the caller, each started handler once, budgets back to zero, nothing live
  I5 a successful call delivered its whole response, in order
  I6 after a graceful drain, no call ends aborted: each was finished or known not processed
Usage: rpc-lane-model.py [mutant]; exit 1 on a violation, so each mutant runs --fails-without.
MODEL_BUDGETS=c/s overrides the budgets (chunks); the caller's default is two windows.
"""

import os
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
    "split-release",  # the consumed chunk returns to the pool; the regrant is a later step (F2)
    "drain-ignored",  # the caller ignores Drain and keeps calling (F4)
    "id-at-call",  # ids allocated when a call is made, not when its Call is queued (F7)
}
assert MUTANT in MUTANTS, MUTANT
RULES = set(filter(None, os.environ.get("RULES", "drain,reorder").split(",")))
assert RULES <= {"drain", "reorder", "early-end"}, RULES

REQ, RESP = 2, 3  # request and response body chunks
WINDOW = 2  # per-call window
BUDGET_CALLER, BUDGET_SERVER = map(int, os.environ.get("MODEL_BUDGETS", "4/1").split("/"))
CALLS = 2

# Caller call: phase (0 unsent, 1 live, 2 done, 4 done but holding budget), outcome, completions,
# id, request chunks sent, request limit (server's credit), response limit (our grant), received,
# consumed.
C_NAMES = ("phase", "outcome", "done", "id", "sent", "rlimit", "glimit", "recv", "cons")
# Server call: phase (0 unseen, 1 live, 2 done, 3 refused or discarded), completions, request
# limit (our grant), received, consumed, response chunks sent, response limit, a pending regrant.
S_NAMES = ("phase", "done", "glimit", "recv", "cons", "sent", "rlimit", "pend")
# Connection: next id, the server's highest Call id, its Drain's id (-1 none), the server sent
# its graceful close, the caller saw the Drain, an abrupt close happened.
G_NAMES = ("next", "high", "drain", "fin", "drained", "closed")
FAILURES = {"cancel", "error", "drain", "close"}


def initial():
    caller = tuple((0, None, 0, 0, 0, 0, 0, 0, 0) for _ in range(CALLS))
    server = tuple((0, 0, 0, 0, 0, 0, 0, 0) for _ in range(CALLS))
    return (caller, server, (), (), (1, 0, -1, False, False, False))


def put(row, names, **kw):
    values = dict(zip(names, row))
    values.update(kw)
    return tuple(values[n] for n in names)


def setc(caller, i, **kw):
    return caller[:i] + (put(caller[i], C_NAMES, **kw),) + caller[i + 1 :]


def sets(server, i, **kw):
    return server[:i] + (put(server[i], S_NAMES, **kw),) + server[i + 1 :]


def setg(g, **kw):
    return put(g, G_NAMES, **kw)


def reserved_caller(caller):
    return sum(c[6] - c[8] for c in caller if c[0] in (1, 4))


def reserved_server(server):
    return sum(s[2] - s[4] for s in server if s[0] == 1)


def grant_target(budget, reserved_others):
    if MUTANT == "grant-without-reserve":
        return WINDOW
    return min(WINDOW, budget - reserved_others)


def regrant(reserved, glimit, cons, total, budget):
    # D5: one chunk consumed (cons was its count before); the consumed bytes move into the new
    # grant in one step, and only the excess leaves the pool.
    others = reserved - (glimit - cons)
    return min(cons + 1 + grant_target(budget, others), total)


def successors(state):
    caller, server, c2s, s2c, g = state
    nxt, high, drain, fin, drained, closed = g
    out = []
    if closed:
        return out
    for i, c in enumerate(caller):
        phase, _outcome, done, _cid, sent, rlimit, glimit, recv, cons = c
        if phase == 0 and (not drained or MUTANT == "drain-ignored"):
            grant = grant_target(BUDGET_CALLER, reserved_caller(caller))
            if grant > 0:
                ident = i + 1 if MUTANT == "id-at-call" else nxt
                caller2 = setc(caller, i, phase=1, id=ident, glimit=grant)
                frame = ("call", i, ident, grant)
                out.append(("call", (caller2, server, c2s + (frame,), s2c, setg(g, next=nxt + 1))))
        elif phase == 1:
            if sent < REQ and sent < rlimit:
                caller2 = setc(caller, i, sent=sent + 1)
                out.append(("req-body", (caller2, server, c2s + (("body", i, sent),), s2c, g)))
            if cons < recv:
                cons2 = cons + 1
                caller2 = setc(caller, i, cons=cons2)
                new_limit = regrant(reserved_caller(caller), glimit, cons, RESP, BUDGET_CALLER)
                msgs = c2s
                if new_limit > glimit and MUTANT != "no-regrant":
                    caller2 = setc(caller2, i, glimit=new_limit)
                    msgs = c2s + (("credit", i, new_limit),)
                if cons2 == RESP:
                    caller2 = setc(caller2, i, phase=2, outcome="ok", done=done + 1)
                out.append(("read", (caller2, server, msgs, s2c, g)))
            caller2 = setc(caller, i, phase=2, outcome="cancel", done=done + 1)
            out.append(("cancel", (caller2, server, c2s + (("cancel", i),), s2c, g)))
    if not fin:
        for i, s in enumerate(server):
            phase, done, glimit, recv, cons, sent, rlimit, pend = s
            if phase != 1:
                continue
            if cons < recv:
                if MUTANT == "split-release":
                    server2 = sets(server, i, cons=cons + 1, pend=1)
                    out.append(("consume", (caller, server2, c2s, s2c, g)))
                else:
                    server2 = sets(server, i, cons=cons + 1)
                    reserved = reserved_server(server)
                    new_limit = regrant(reserved, glimit, cons, REQ, BUDGET_SERVER)
                    msgs = s2c
                    if new_limit > glimit:
                        server2 = sets(server2, i, glimit=new_limit)
                        msgs = s2c + (("credit", i, new_limit),)
                    out.append(("consume", (caller, server2, c2s, msgs, g)))
            if pend:
                server2 = sets(server, i, pend=0)
                others = reserved_server(server2) - (glimit - cons)
                new_limit = min(cons + grant_target(BUDGET_SERVER, others), REQ)
                msgs = s2c
                if new_limit > glimit:
                    server2 = sets(server2, i, glimit=new_limit)
                    msgs = s2c + (("credit", i, new_limit),)
                out.append(("regrant", (caller, server2, c2s, msgs, g)))
            ready = cons == REQ or "early-end" in RULES
            if ready and sent < RESP and sent < rlimit:
                server2 = sets(server, i, sent=sent + 1)
                if sent + 1 == RESP:
                    server2 = sets(server2, i, phase=2, done=done + 1)
                out.append(("resp-body", (caller, server2, c2s, s2c + (("body", i, sent),), g)))
            server2 = sets(server, i, phase=2, done=done + 1)
            out.append(("error", (caller, server2, c2s, s2c + (("error", i),), g)))
        if (
            drain < 0 and "drain" in RULES
        ):  # D4: Drain names the highest Call this end accepted, then nothing above
            g2 = setg(g, drain=high)
            out.append(("drain", (caller, server, c2s, s2c + (("drain", high),), g2)))
        elif drain >= 0 and not any(s[0] == 1 for s in server):  # graceful close after it
            out.append(("fin", (caller, server, c2s, s2c + (("fin",),), setg(g, fin=True))))
    for k in deliverable(c2s):
        out.append(("deliver-c2s", server_receive(state, k)))
    for k in deliverable(s2c):
        out.append(("deliver-s2c", caller_receive(state, k)))
    # The connection fails: every live call ends on both sides, in-flight frames are lost.
    caller2 = caller
    for i, c in enumerate(caller):
        if c[0] in (0, 1):
            leak = MUTANT == "no-release-on-close" and c[0] == 1
            caller2 = setc(caller2, i, phase=4 if leak else 2, outcome="abort", done=c[2] + 1)
    server2 = server
    for i, s in enumerate(server):
        if s[0] == 1:
            server2 = sets(server2, i, phase=2, done=s[1] + 1)
    out.append(("close", (caller2, server2, (), (), setg(g, closed=True))))
    return out


def deliverable(chan):
    if not chan:
        return []
    if "reorder" not in RULES:
        return [0]
    ks = []
    for k, f in enumerate(chan):
        if f[0] == "fin" and k:
            continue
        if all(h[0] == "body" and not (f[0] == "body" and h[1] == f[1]) for h in chan[:k]):
            ks.append(k)
    return ks


def server_receive(state, k):
    caller, server, c2s, s2c, g = state
    frame, c2s = c2s[k], c2s[:k] + c2s[k + 1 :]
    if g[3]:  # after its graceful close the server reads nothing
        return (caller, server, c2s, s2c, g)
    kind, i = frame[0], frame[1]
    s = server[i]
    if kind == "call":
        ident = frame[2]
        if s[0] != 0 or ident <= g[1]:
            return "I2 a Call id did not increase"
        g = setg(g, high=ident)
        if g[2] >= 0:  # draining: discarded; the caller knows from the Drain's id
            return (caller, sets(server, i, phase=3), c2s, s2c, g)
        target = grant_target(BUDGET_SERVER, reserved_server(server))
        if target <= 0:  # no budget for one chunk of its request: refused (D5)
            return (caller, sets(server, i, phase=3), c2s, s2c + (("error", i),), g)
        limit = min(target, REQ)
        server = sets(server, i, phase=1, glimit=limit, rlimit=frame[3])
        return (caller, server, c2s, s2c + (("credit", i, limit),), g)
    if s[0] != 1:
        if MUTANT == "forget-is-violation":
            return "I2 a late frame closed the lane"
        return (caller, server, c2s, s2c, g)  # retired: discarded and counted
    if kind == "body":
        if frame[2] != s[3] or s[3] + 1 > s[2]:
            return "I2 a request chunk out of order or past credit"
        return (caller, sets(server, i, recv=s[3] + 1), c2s, s2c, g)
    if kind == "credit":
        if frame[2] <= s[6]:
            return "I2 a response credit did not increase"
        return (caller, sets(server, i, rlimit=frame[2]), c2s, s2c, g)
    if kind == "cancel":
        return (caller, sets(server, i, phase=2, done=s[1] + 1), c2s, s2c, g)
    return "I2 unknown frame"


def caller_receive(state, k):
    caller, server, c2s, s2c, g = state
    frame, s2c = s2c[k], s2c[:k] + s2c[k + 1 :]
    kind = frame[0]
    if kind == "drain":
        if g[4]:
            return "I2 a second Drain"
        g = setg(g, drained=True)
        if MUTANT != "drain-ignored":
            for i, c in enumerate(caller):
                if c[0] == 0 or (c[0] == 1 and c[3] > frame[1]):
                    caller = setc(caller, i, phase=2, outcome="not-processed", done=c[2] + 1)
        return (caller, server, c2s, s2c, g)
    if kind == "fin":
        for c in caller:
            if c[0] == 1 and c[7] < RESP:
                return "I6 a call aborted by a graceful close"
        return (caller, server, c2s, s2c, g)
    i = frame[1]
    c = caller[i]
    if c[0] != 1:
        if MUTANT == "forget-is-violation":
            return "I2 a late frame closed the lane"
        return (caller, server, c2s, s2c, g)
    if kind == "body":
        if frame[2] != c[7] or c[7] + 1 > c[6]:
            return "I2 a response chunk out of order or past credit"
        caller = setc(caller, i, recv=c[7] + 1)
        if MUTANT == "grant-on-receive":
            new_limit = min(c[7] + 1 + WINDOW, RESP)
            if new_limit > c[6]:
                caller = setc(caller, i, glimit=new_limit)
                c2s = c2s + (("credit", i, new_limit),)
        return (caller, server, c2s, s2c, g)
    if kind == "credit":
        if frame[2] <= c[5]:
            return "I2 a request credit did not increase"
        return (setc(caller, i, rlimit=frame[2]), server, c2s, s2c, g)
    if kind == "error":
        return (setc(caller, i, phase=2, outcome="error", done=c[2] + 1), server, c2s, s2c, g)
    return "I2 unknown frame"


def check(state):
    caller, server = state[0], state[1]
    for c in caller:
        if c[2] > 1:
            return "I3 caller completed twice"
        if c[0] == 1 and c[7] > c[6]:
            return "I1 caller holds more than it granted"
        if c[1] == "ok" and c[8] != RESP:
            return "I5 success without the whole response"
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
            if isinstance(nxt, str):
                problem = problem or nxt
                break
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)
        if problem:
            print(f"model={MUTANT or 'design'} states={len(seen)} FAILS: {problem}")
            return 1
    print(
        f"model={MUTANT or 'design'} rules={','.join(sorted(RULES)) or '-'} calls={CALLS} "
        f"req={REQ} resp={RESP} window={WINDOW} budgets={BUDGET_CALLER}/{BUDGET_SERVER} "
        f"states={len(seen)} finals={finals} outcomes={len(outcomes)} ok"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
