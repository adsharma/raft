import time

from typing import Optional
from dataclasses import dataclass
from enum import IntEnum


@dataclass
class BaseMessage:
    class MessageType(IntEnum):
        AppendEntries = 0
        RequestVote = 1
        RequestVoteResponse = 2
        Response = 3

    sender: str
    receiver: Optional[str]
    term: int
    data: str
    timestamp: int = int(time.time())

    @property
    def type(self):
        return self._type

    def __repr__(self):
        return "[ %d, %s ]" % (self.term, self.data)
