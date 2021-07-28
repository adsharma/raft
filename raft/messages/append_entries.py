import hashlib

from dataclasses import dataclass, field
from enum import IntEnum
from typing import List, Optional, Union

from serde import deserialize, serialize
from serde.msgpack import to_msgpack

from .base import BaseMessage, Term


class Command(IntEnum):
    PUT = 0
    GET = 1
    QUORUM_PUT = 2
    QUORUM_GET = 3


@deserialize
@serialize
@dataclass
class LogEntry:
    _type = BaseMessage.MessageType.LogEntry

    term: Term = Term(0)
    index: int = 0
    id: str = ""
    command: Command = Command.PUT
    key: Union[int, str, None] = None
    value: Union[int, str, None] = None

    def hash(self) -> "hashlib._Hash":
        return hashlib.sha256(to_msgpack(self))


@deserialize
@serialize
@dataclass
class AppendEntriesMessage(BaseMessage):
    _type = BaseMessage.MessageType.AppendEntries

    leader_id: Optional[str] = None
    prev_log_index: int = 0
    prev_log_term: Term = Term(0)
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        BaseMessage.EXT_DICT[LogEntry._type] = LogEntry  # type: ignore
