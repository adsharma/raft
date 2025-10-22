from .base import BaseMessage, Term, Peer
from .append_entries import AppendEntriesMessage, LogEntry
from .request_vote import RequestVoteMessage, RequestVoteResponseMessage
from .response import ResponseMessage

from typing import Union

Message = Union[AppendEntriesMessage, RequestVoteMessage, RequestVoteResponseMessage, ResponseMessage]

__all__ = [
    "BaseMessage",
    "AppendEntriesMessage",
    "RequestVoteMessage",
    "RequestVoteResponseMessage",
    "ResponseMessage",
    "Message",
]
