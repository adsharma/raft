from .base import BaseMessage
from dataclasses import dataclass


@dataclass
class RequestVoteMessage(BaseMessage):

    _type = BaseMessage.MessageType.RequestVote


class RequestVoteResponseMessage(BaseMessage):

    _type = BaseMessage.MessageType.RequestVoteResponse
