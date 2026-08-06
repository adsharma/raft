"""Runtime marker shim for py2many's design-by-contract API.

The static-python source files (``raft/core.py``) import the verification
markers from ``py2many.spec`` / ``py2many.theorem`` / ``py2many.result``.
py2many recognises these imports (and the ``CHECKER.pre`` / ``CHECKER.post`` /
``CHECKER.invariant`` access) purely by name and drops the imports when it
transpiles to Lean, so the *verified* Lean output never contains them.

At Python runtime the markers must still resolve, because the ``if
CHECKER.pre:`` blocks are dead-but-present code.  When the full py2many
distribution is installed it provides these modules; this tiny vendored shim
makes the raft package importable standalone with identical semantics (the
markers evaluate to ``False`` / no-op decorators).  It shadows nothing that
matters: it is a faithful subset of the real API.
"""
