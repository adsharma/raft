import time
import uuid
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Dict, Optional, Union

from serde import deserialize, serialize


@deserialize
@serialize
@dataclass
class BaseMessage:
    class MessageType(IntEnum):
        AppendEntries = 0
        RequestVote = 1
        RequestVoteResponse = 2
        Response = 3

    EXT_DICT = {}

    sender: Union[int, str, uuid.UUID]  # int used only on tests
    receiver: Union[int, str, uuid.UUID, None]
    term: int
    data: Union[int, str, Dict, None] = None
    timestamp: int = int(time.time())

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.EXT_DICT[cls._type] = cls  # type: ignore

    @property
    def type(self):
        return self._type  # type: ignore

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    @staticmethod
    def default() -> "BaseMessage":
        return BaseMessage(0, 0, 0, 0, 0)
