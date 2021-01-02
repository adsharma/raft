from dataclasses import dataclass

from .base import BaseMessage


@dataclass
class ResponseMessage(BaseMessage):

    _type = BaseMessage.MessageType.Response
