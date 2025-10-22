from dataclasses import dataclass
from enum import IntEnum

from serde import InternalTagging, serde

from .base import BaseMessage


@serde(tagging=InternalTagging("_type"))
@dataclass
class ResponseMessage(BaseMessage):
    class Role(IntEnum):
        FOLLOWER = 0
        LEARNER = 1

    response: bool = True
    current_term: int = 0
    role: Role = Role.FOLLOWER
