from .base import BaseMessage
from dataclasses import dataclass


@dataclass
class AppendEntriesMessage(BaseMessage):
    _type = BaseMessage.MessageType.AppendEntries
