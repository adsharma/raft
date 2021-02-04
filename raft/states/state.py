import logging
import random
from asyncio.events import TimerHandle
from typing import TYPE_CHECKING, Optional

from ..messages.base import BaseMessage
from ..messages.response import ResponseMessage

if TYPE_CHECKING:
    from ..servers.server import Server


logger = logging.getLogger("raft")


class State:
    def __init__(self, timeout):
        self._timeout = timeout
        self.leader = None
        self.learner = False

    def set_server(self, server: "Server"):
        self._server = server

    async def on_message(self, message):
        """This method is called when a message is received,
        and calls one of the other corrosponding methods
        that this state reacts to.

        """
        _type = message.type

        if message.term > self._server._currentTerm:
            self._server._currentTerm = message.term
        # Is the messages.term < ours? If so we need to tell
        #   them this so they don't get left behind.
        elif message.term < self._server._currentTerm:
            if _type != BaseMessage.MessageType.Response:
                # Do not send a response to a response
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
        else:
            raise Exception(f"Unknown message type: {type(message)}")

    async def on_leader_timeout(self, message):
        """This is called when the leader timeout is reached."""
        return self, None

    async def on_vote_request(self, message):
        """This is called when there is a vote request."""
        return self, None

    async def on_vote_received(self, message):
        """This is called when this node recieves a vote."""
        return self, None

    async def on_append_entries(self, message):
        """This is called when there is a request to
        append an entry to the log.

        """
        return self, None

    async def on_response_received(self, message):
        """This is called when a response is sent back to the Leader"""
        return self, None

    async def on_client_command(self, message):
        """This is called when there is a client request."""
        return self, None

    def _nextTimeout(self):
        return random.randrange(self._timeout, 2 * self._timeout)

    async def _send_response_message(self, msg, yes=True):
        response = ResponseMessage(
            self._server._name,
            msg.sender,
            msg.term,
            id=msg.id,
            response=yes,
            current_term=self._server._currentTerm,
        )
        if self.learner:
            response.role = ResponseMessage.Role.LEARNER
        await self._server.send_message(response)

    async def _accept_leader(self, message, timer: Optional[TimerHandle]):
        if self.leader != message.leader_id:
            self.leader = message.leader_id
            logger.info(f"Accepted new leader: {self.leader}")

            from raft.states.follower import Follower  # TODO: Fix circular import

            if not isinstance(self, Follower):
                if timer is not None:
                    timer.cancel()
                follower = Follower()
                follower.leader = self.leader
                follower.set_server(self._server)
                return follower, None
