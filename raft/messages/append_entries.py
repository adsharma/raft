import hashlib
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Union

from serde import InternalTagging, serde
from serde.msgpack import to_msgpack

from .base import BaseMessage, HashType, Term


class Command(IntEnum):
    PUT = 0
    GET = 1
    QUORUM_PUT = 2
    QUORUM_GET = 3


@serde(tagging=InternalTagging("_type"))
@dataclass
class LogEntry:
    term: Term = Term(0)
    index: int = 0
    id: str = ""
    command: Command = Command.PUT
    key: Union[int, str, None] = None
    value: Union[int, str, None] = None

    def hash(self) -> "HashType":
        return hashlib.sha256(to_msgpack(self))


@serde(tagging=InternalTagging("_type"))
@dataclass
class AppendEntriesMessage(BaseMessage):
    leader_id: Union[int, str, None] = None
    prev_log_index: int = 0
    prev_log_term: Term = Term(0)
    entries: list[LogEntry] = field(default_factory=list)
    leader_commit: int = 0

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
