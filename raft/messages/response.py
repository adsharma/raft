from dataclasses import dataclass
from enum import IntEnum

from serde import deserialize, serialize

from .base import BaseMessage


@deserialize
@serialize
@dataclass
class ResponseMessage(BaseMessage):
    class Role(IntEnum):
        FOLLOWER = 0
        LEARNER = 1

    _type = BaseMessage.MessageType.Response

    response: bool = True
    current_term: int = 0
    role: Role = Role.FOLLOWER
