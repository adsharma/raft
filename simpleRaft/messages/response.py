from .base import BaseMessage

from dataclasses import dataclass

@dataclass
class ResponseMessage(BaseMessage):

    _type = BaseMessage.MessageType.Response