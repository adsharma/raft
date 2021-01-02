from dataclasses import dataclass

from .base import BaseMessage


@dataclass
class AppendEntriesMessage(BaseMessage):
    _type = BaseMessage.MessageType.AppendEntries
