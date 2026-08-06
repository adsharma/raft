"""Runtime face of the verified core: ``Result[T, E]`` instead of exceptions.

The pure decision predicates live in ``raft/core.py`` (transpiled to Lean and
verified).  This module wraps the *fallible* operations -- most notably
out-of-bounds log access, which the live code otherwise indexes directly and
would raise ``IndexError`` on -- in ``Result[T, E]`` (``Ok`` / ``Error``) per
the static-python rule.

Lean emission for ``Ok`` / ``Error`` is a pending py2many change, so this file
is intentionally evaluated only at runtime, never transpiled.  It is a thin
wrapper over types from ``py2many.result`` (vendored shim or the real py2many).
"""

from typing import List

from py2many.result import Error, Ok, StdResult


class Err:
    """Error codes for fallible raft-core operations (kept small & enum-like)."""

    OUT_OF_BOUNDS = 0


def log_get(log: List, i: int) -> StdResult[int, int]:
    """Return ``log[i].term`` wrapped in ``Result``.

    Falls over to ``Error(Err.OUT_OF_BOUNDS)`` when ``i`` is outside the log
    instead of raising ``IndexError``, mirroring the defensive bounds check the
    consensus code performs before indexing.
    """
    if i < 0 or i >= len(log):
        return Error(Err.OUT_OF_BOUNDS)
    return Ok(log[i].term)


def ok_value(res: StdResult[int, int]):
    """Unwrap an ``Ok`` to its value, ``None`` when it is an ``Error``."""
    if isinstance(res, Ok):
        return res.value
    return None
