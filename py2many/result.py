"""Runtime-only micro-shim of py2many.result (see the package docstring).

Provides the ``Result[T, E]`` (Ok/Error) types used by ``raft/live.py`` for
fallible operations instead of exceptions.
"""

from dataclasses import dataclass
from enum import IntEnum
from typing import Generic, TypeVar, Union

T = TypeVar("T")
E = TypeVar("E", Exception, IntEnum)


@dataclass
class Ok(Generic[T]):
    value: T


@dataclass
class Error(Generic[E]):
    error: E


# std::result version
StdResult = Union[Ok[T], Error[E]]
# anyhow version
Result = Ok[T]
