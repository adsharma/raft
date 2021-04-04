from dataclasses import dataclass

from serde import deserialize, serialize

from .base import BaseMessage, Term


@deserialize
@serialize
@dataclass
class QueryState(BaseMessage):

    _type = BaseMessage.MessageType.QueryState

    new_term: Term = Term(0)


@deserialize
@serialize
@dataclass
class QueryStateResponseMessage(BaseMessage):

    _type = BaseMessage.MessageType.QueryStateResponse

    last_term: Term = Term(0)
    commit_index: int = 0


@deserialize
@serialize
@dataclass
class TrimLog(BaseMessage):

    _type = BaseMessage.MessageType.TrimLog

    last_term: Term = Term(0)
    new_index: int = 0


@deserialize
@serialize
@dataclass
class TrimLogResponseMessage(BaseMessage):

    _type = BaseMessage.MessageType.TrimLogResponse

    success: bool = False
