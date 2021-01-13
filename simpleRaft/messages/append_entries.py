from dataclasses import dataclass, field
from enum import IntEnum
from typing import List, Optional, Union

from serde import deserialize, serialize

from .base import BaseMessage


@deserialize
@serialize
@dataclass
class AppendEntriesMessage(BaseMessage):
    _type = BaseMessage.MessageType.AppendEntries

    leader_id: Optional[str] = None
    prev_log_index: int = 0
    prev_log_term: int = 0
    entries: List = field(default_factory=list)
    leader_commit: int = 0


class Command(IntEnum):
    PUT = 0
    GET = 1


@deserialize
@serialize
@dataclass
class LogEntry:
    term: int
    index: int = 0
    command: Command = Command.PUT
    key: Union[int, str, None] = None
    value: Union[int, str, None] = None
