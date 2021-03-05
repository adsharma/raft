import time
import uuid
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Optional, Union

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
        LogEntry = 4

    EXT_DICT = {}
    EXT_DICT_REVERSED = {}

    sender: Union[int, str, uuid.UUID]  # int used only on tests
    receiver: Union[int, str, uuid.UUID, None]
    term: int
    id: str = ""
    data: Union[int, str, Dict, None] = None
    timestamp: int = int(time.time())
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
        return BaseMessage(0, 0, 0, "", 0, 0)
