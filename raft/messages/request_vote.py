from dataclasses import dataclass

from serde import deserialize, serialize

from .base import BaseMessage, Term


@deserialize
@serialize
@dataclass
class RequestVoteMessage(BaseMessage):

    _type = BaseMessage.MessageType.RequestVote
    last_log_index: int = 0
    last_log_term: Term = Term(0)


@deserialize
@serialize
@dataclass
class RequestVoteResponseMessage(BaseMessage):

    _type = BaseMessage.MessageType.RequestVoteResponse

    response: bool = True
    current_term: Term = Term(0)
