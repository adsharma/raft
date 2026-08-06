"""Runtime-only micro-shim of py2many.spec (see the package docstring).

Pure Python mirror of the markers used by raft/core.py.  Kept in sync with
py2many.spec: the sentinel attributes are always False, so the ``if
CHECKER.*:`` blocks are dead at runtime.
"""


class _Checker:
    pre = False
    post = False
    invariant = False


CHECKER = _Checker()


class _Result:
    def __getattr__(self, name: str):
        return None


result = _Result()

# Legacy flat exports (backward compatibility with py2many.smt).
pre = CHECKER.pre
post = CHECKER.post
invariant = CHECKER.invariant


def check(claim: bool):
    assert claim


def prove(fn):
    import inspect
    from itertools import product

    n = len(inspect.signature(fn).parameters)
    assert all(fn(*combo) for combo in product((True, False), repeat=n))
