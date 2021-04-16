import asyncio
import logging

from typing import Tuple

from ..messages.base import Peer
from ..messages.append_entries import AppendEntriesMessage
from ..messages.request_vote import RequestVoteMessage, RequestVoteResponseMessage
from .state import State

logger = logging.getLogger("raft")


class Voter(State):
    def __init__(self, timeout: float):
        super().__init__(timeout)
        self._last_vote = (0, "")
        self._timeout = timeout
        self.timer = self.restart_timer()

    @property
    def last_vote(self):
        return self._last_vote

    @last_vote.setter
    def last_vote(self, value: Tuple[int, Peer]):
        if not self._server:
            raise Exception(f"setting last vote without server")
        self._server._stable_storage["last_vote"] = str(value)
        self._last_vote = value

    def restart_timer(self):
        loop = asyncio.get_event_loop()
        self._timeoutTime = self._nextTimeout()
        return loop.call_later(self._timeoutTime, self.on_leader_timeout)

    async def on_vote_request(self, message: RequestVoteMessage):
        loop = asyncio.get_event_loop()
        last_heart_beat = self.timer.when() - self._timeoutTime
        time_since_heart_beat = loop.time() - last_heart_beat
        if time_since_heart_beat < self._timeout and self.leader is not None:
            # defence against disruptive removed servers. Raft section 6
            await self._send_vote_response_message(message, yes=False)

        last_vote_term, voted_for = self.last_vote
        eligible_to_vote = message.term > last_vote_term
        if eligible_to_vote and message.last_log_index >= self._server._lastLogIndex:
            self.last_vote = (message.term, message.sender)
            await self._send_vote_response_message(message)
        else:
            await self._send_vote_response_message(message, yes=False)

        return self, None

    async def _send_vote_response_message(self, msg: RequestVoteMessage, yes=True):
        voteResponse = RequestVoteResponseMessage(
            self._server._name, msg.sender, msg.term, response=yes
        )
        await self._server.send_message(voteResponse)

    def on_leader_timeout(self):
        """This is called when the leader timeout is reached."""
        from .candidate import Candidate  # TODO: Fix circular import

        logger.info(
            f"{self._server.group}: {self._server._name}: Lost Leader: {self.leader}"
        )
        if (
            self._server._parent
            and self._server._parent._state.leader_name != self._server._human_name
        ):
            logger.debug(
                f"{self._server._name}: {self._server.group}:"
                + f"not a leader in parent: {self._server._parent._state.leader_name} {self._server._human_name}"
            )
            return

        self.timer.cancel()
        self.leader = None
        candidate = Candidate()
        candidate.set_server(self._server)
        self._server._state = candidate

        return candidate, None

    async def on_append_entries(self, message: AppendEntriesMessage):
        self._timeoutTime = self._nextTimeout()
        self.timer.cancel()
        self.timer = self.restart_timer()

        if message.term < self._server._currentTerm:
            await self._send_response_message(message, yes=False)
            return self, None

        return await self._accept_leader(message, self.timer)
