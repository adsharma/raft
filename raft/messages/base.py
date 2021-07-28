import hashlib
import uuid
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Optional, NewType, Union

from serde import deserialize, serialize
from serde.msgpack import to_msgpack

Term = NewType("Term", int)
Peer = Union[int, str, uuid.UUID]  # int used only on tests


@deserialize
@serialize
@dataclass
class BaseMessage:
    class MessageType(IntEnum):
        AppendEntries = 0
        RequestVote = 1
        RequestVoteResponse = 2
        Response = 3
        LogEntry = 4

    EXT_DICT = {}
    EXT_DICT_REVERSED = {}

    sender: Peer
    receiver: Optional[Peer]
    term: int  # TODO: Change to Term
    id: str = ""
    data: Union[int, str, Dict, None] = None
    timestamp: int = 0
    group: Optional[str] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.EXT_DICT[cls._type] = cls  # type: ignore
        cls.EXT_DICT_REVERSED[cls] = cls._type  # type: ignore

    @property
    def type(self):
        return self._type  # type: ignore

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    @staticmethod
    def default() -> "BaseMessage":
        return BaseMessage(0, 0, Term(0), "", 0, 0)

    def __post_init__(self):
        if self.id == "":
            self.id = uuid.uuid4().hex

    def hash(self) -> "hashlib._Hash":
        return hashlib.sha256(to_msgpack(self))
