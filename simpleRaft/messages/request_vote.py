from dataclasses import dataclass

from .base import BaseMessage


@dataclass
class RequestVoteMessage(BaseMessage):

    _type = BaseMessage.MessageType.RequestVote


class RequestVoteResponseMessage(BaseMessage):

    _type = BaseMessage.MessageType.RequestVoteResponse
