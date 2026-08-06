# Verified core of the Raft consensus logic

The safety-critical consensus decisions now live in
[`raft/core.py`](../raft/core.py) — the module the state machines
(`raft/states/voter.py`, `candidate.py`, `follower.py`, `leader.py`) actually
call.  That module is written in the **static-python subset** and is *both*
executed (its `CHECKER.*` blocks are dead constants at runtime) *and*
transpiled to Lean with `py2many --lean`, where `lake build` proves every
pre/post condition, class invariant and lemma.  Because the implementation
calls the very same functions that are verified, the two cannot silently
diverge.

## Layout

* `raft/core.py` — the verified, total decision predicates
  (`may_grant_vote`, `promote_to_leader`, `clamp_commit`, `log_matching_ok`,
  `can_commit`, the `ConsensusState` invariant).  Each mirrors the live
  implementation *exactly* (not an idealized paper version).
* `raft/live.py` — the fallible runtime face: `Result[T, E]` (`Ok`/`Error`)
  instead of exceptions for out-of-bounds log access, per the static-python
  rule.  Not Lean-transpiled yet (Lean emission of `Ok`/`Error` is a pending
  py2many change).
* `py2many/` — a tiny *vendored* shim of `py2many.spec` / `py2many.theorem` /
  `py2many.result` so the raft package imports standalone when the real py2many
  is not installed.  It is excluded from the wheel (see `pyproject.toml`).

## Why the predicates match the implementation, not the paper

The original static-python spec was a paper-faithful idealization, but the
live code differs in a few places.  To make the verified core the single
source of truth *without changing consensus behavior*, the predicates were
corrected to mirror the implementation:

| Predicate | Live behavior it encodes |
|---|---|
| `may_grant_vote` | `cand_term > last_vote_term AND cand_index >= lastLogIndex` (stricter than the paper: an equal term never gets the vote) |
| `promote_to_leader` | `num_votes > 1 AND num_votes > total/2` |
| `clamp_commit` | `min(leader_commit, max(0, len(log)-1))` |
| `log_matching_ok` | reject when `prev_log_index >= len(log)` or term mismatch; apply only when `prev < len AND term matches` |
| `can_commit` | `entry_term == current_term AND new_commit_index > commitIndex` |

## Verify

From the `../py2many` checkout:

```bash
uv run python -m py2many --lean --outdir /tmp/core_lean raft/core.py
../py2many/scripts/lean-runner.sh build /tmp/core_lean/core.lean   # exit 0 == proved
```

Run as Python (needs the raft deps; the vendored `py2many/` shim resolves the
markers):

```bash
PYTHONPATH=. python -m raft.core
pytest tests/ -q
```

### Notes / gotchas

* `int` transpiles to `Nat`, so all quantified values are non-negative (the
  only regime Raft terms/indexes live in).
* `CHECKER.post` bodies that reference `result` become Lean subtypes
  `{ r : T // post }` discharged by `by rfl`, so postconditions are written as
  **definitional equalities**.  Boolean expressions are factored into
  `may_grant_vote` / `promote_rule` / `log_matching_ok` / `commit_advance_rule`
  so a post can reference a plain-Bool function call (a bare `x == y and ...`
  would emit a Prop instead of a Bool and break the subtype).
* The Lean backend cannot transpile a module-level docstring, so `raft/core.py`
  intentionally has none (the explanation lives in this README).
