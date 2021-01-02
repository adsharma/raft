import time
import uuid
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Optional, Union


@dataclass
class BaseMessage:
    class MessageType(IntEnum):
        AppendEntries = 0
        RequestVote = 1
        RequestVoteResponse = 2
        Response = 3

    sender: Union[int, uuid.UUID]  # int used only on tests
    receiver: Union[int, uuid.UUID, None]
    term: int
    data: Any
    timestamp: int = int(time.time())

    @property
    def type(self):
        return self._type  # type: ignore

    def __repr__(self):
        return "[ %d, %s ]" % (self.term, self.data)

    @staticmethod
    def default() -> "BaseMessage":
        return BaseMessage(0, 0, 0, 0, 0)
