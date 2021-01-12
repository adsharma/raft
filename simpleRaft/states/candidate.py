import asyncio

from ..messages.request_vote import RequestVoteMessage
from .config import CANDIDATE_TIMEOUT
from .leader import Leader
from .voter import Voter


class Candidate(Voter):
    def __init__(self, timeout=CANDIDATE_TIMEOUT):
        super().__init__(timeout)
        self.leader = None

    def set_server(self, server):
        self._server = server
        self._votes = {}
        loop = asyncio.get_event_loop()
        return loop.create_task(self._start_election())

    async def on_vote_request(self, message):
        return self, None

    async def on_vote_received(self, message):
        if message.sender not in self._votes:
            self._votes[message.sender] = message

            if len(list(self._votes.keys())) > (self._server._total_nodes - 1) / 2:
                self.timer.cancel()
                leader = Leader()
                leader.set_server(self._server)

                return leader, None
        return self, None

    async def _start_election(self):
        self._server._currentTerm += 1
        election = RequestVoteMessage(
            self._server._name,
            None,
            self._server._currentTerm,
            last_log_index=self._server._lastLogIndex,
            last_log_term=self._server._lastLogTerm,
        )

        await self._server.send_message(election)
        self._last_vote = self._server._name
