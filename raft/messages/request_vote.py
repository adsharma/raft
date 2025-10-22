from dataclasses import dataclass

from serde import InternalTagging, serde

from .base import BaseMessage, Term


@serde(tagging=InternalTagging("_type"))
@dataclass
class RequestVoteMessage(BaseMessage):
    last_log_index: int = 0
    last_log_term: Term = Term(0)


@serde(tagging=InternalTagging("_type"))
@dataclass
class RequestVoteResponseMessage(BaseMessage):
    response: bool = True
    current_term: Term = Term(0)
