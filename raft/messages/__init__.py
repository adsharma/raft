from typing import Union

from .append_entries import AppendEntriesMessage, LogEntry  # noqa: F401
from .base import BaseMessage, Peer, Term  # noqa: F401
from .request_vote import RequestVoteMessage, RequestVoteResponseMessage
from .response import ResponseMessage

Message = Union[
    AppendEntriesMessage,
    RequestVoteMessage,
    RequestVoteResponseMessage,
    ResponseMessage,
]

__all__ = [
    "BaseMessage",
    "AppendEntriesMessage",
    "RequestVoteMessage",
    "RequestVoteResponseMessage",
    "ResponseMessage",
    "Message",
]
