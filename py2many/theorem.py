"""Runtime-only micro-shim of py2many.theorem (see the package docstring).

No-op decorator markers that py2many turns into ``theorem`` in Lean.
"""


def theorem(fn):
    return fn


def lemma(fn):
    return fn


def by(tactic: str):
    def decorator(fn):
        return fn

    return decorator
