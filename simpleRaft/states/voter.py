import asyncio
import logging

from ..messages.request_vote import RequestVoteResponseMessage
from .state import State

logger = logging.getLogger("raft")


class Voter(State):
    def __init__(self, timeout):
        super().__init__(timeout)
        self._last_vote = None
        self._timeout = timeout
        self.timer = self.restart_timer()

    def restart_timer(self):
        loop = asyncio.get_event_loop()
        self._timeoutTime = self._nextTimeout()
        return loop.call_later(self._timeoutTime, self.on_leader_timeout)

    async def on_vote_request(self, message):
        if (
            self._last_vote is None
            and message.last_log_index >= self._server._lastLogIndex
        ):
            self._last_vote = message.sender
            await self._send_vote_response_message(message)
        else:
            await self._send_vote_response_message(message, yes=False)

        return self, None

    async def _send_vote_response_message(self, msg, yes=True):
        voteResponse = RequestVoteResponseMessage(
            self._server._name, msg.sender, msg.term, response=yes
        )
        await self._server.send_message(voteResponse)

    def on_leader_timeout(self):
        """This is called when the leader timeout is reached."""
        from .candidate import Candidate  # TODO: Fix circular import

        logger.info(f"Lost Leader: {self.leader}")

        self.timer.cancel()
        self.leader = None
        candidate = Candidate()
        candidate.set_server(self._server)
        self._server._state = candidate

        return candidate, None

    async def on_append_entries(self, message):
        self._timeoutTime = self._nextTimeout()
        self.timer.cancel()
        self.timer = self.restart_timer()

        if message.term < self._server._currentTerm:
            await self._send_response_message(message, yes=False)
            return self, None

        if self.leader != message.leader_id:
            self.leader = message.leader_id
            logger.info(f"Accepted new leader: {self.leader}")

            from simpleRaft.states.follower import \
                Follower  # TODO: Fix circular import

            if not isinstance(self, Follower):
                self.timer.cancel()
                follower = Follower()
                follower.leader = self.leader
                follower.set_server(self._server)
                return follower, None
