import logging
import random
import time
from typing import TYPE_CHECKING

from ..messages.base import BaseMessage
from ..messages.response import ResponseMessage

if TYPE_CHECKING:
    from ..servers.server import Server


logger = logging.getLogger("raft")


class State:
    def __init__(self, timeout):
        self._timeout = timeout

    def set_server(self, server: "Server"):
        self._server = server

    async def on_message(self, message):
        """This method is called when a message is received,
        and calls one of the other corrosponding methods
        that this state reacts to.

        """
        _type = message.type
        logger.debug(message)

        if message.term > self._server._currentTerm:
            self._server._currentTerm = message.term
        # Is the messages.term < ours? If so we need to tell
        #   them this so they don't get left behind.
        elif message.term < self._server._currentTerm:
            await self._send_response_message(message, yes=False)
            return self, None

        if _type == BaseMessage.MessageType.AppendEntries:
            return await self.on_append_entries(message)
        elif _type == BaseMessage.MessageType.RequestVote:
            return await self.on_vote_request(message)
        elif _type == BaseMessage.MessageType.RequestVoteResponse:
            return await self.on_vote_received(message)
        elif _type == BaseMessage.MessageType.Response:
            return await self.on_response_received(message)

    async def on_leader_timeout(self, message):
        """This is called when the leader timeout is reached."""

    async def on_vote_request(self, message):
        """This is called when there is a vote request."""

    async def on_vote_received(self, message):
        """This is called when this node recieves a vote."""

    async def on_append_entries(self, message):
        """This is called when there is a request to
        append an entry to the log.

        """

    async def on_response_received(self, message):
        """This is called when a response is sent back to the Leader"""

    async def on_client_command(self, message):
        """This is called when there is a client request."""

    def _nextTimeout(self):
        return random.randrange(self._timeout, 2 * self._timeout)

    async def _send_response_message(self, msg, yes=True):
        response = ResponseMessage(
            self._server._name,
            msg.sender,
            msg.term,
            {"response": yes, "currentTerm": self._server._currentTerm},
        )
        await self._server.receive_message(response)
