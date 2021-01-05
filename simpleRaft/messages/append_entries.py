from dataclasses import dataclass

from serde import deserialize, serialize

from .base import BaseMessage


@deserialize
@serialize
@dataclass
class AppendEntriesMessage(BaseMessage):
    _type = BaseMessage.MessageType.AppendEntries
