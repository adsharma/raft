from dataclasses import dataclass

from serde import deserialize, serialize

from .base import BaseMessage


@deserialize
@serialize
@dataclass
class ResponseMessage(BaseMessage):

    _type = BaseMessage.MessageType.Response

    response: bool = True
    current_term: int = 0
