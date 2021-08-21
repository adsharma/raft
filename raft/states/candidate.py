import asyncio
import logging

from ..messages.base import Term
from ..messages.request_vote import RequestVoteMessage, RequestVoteResponseMessage
from ..servers.server import Server
from .config import CANDIDATE_TIMEOUT
from .leader import Leader
from .voter import Voter

logger = logging.getLogger("raft")


class Candidate(Voter):
    def __init__(self, timeout=CANDIDATE_TIMEOUT):
        super().__init__(timeout)
        self.leader = None

    def set_server(self, server: Server):
        self._server = server
        self._votes = {}
        loop = asyncio.get_event_loop()
        return loop.create_task(self._start_election())

    async def on_vote_request(self, message: RequestVoteMessage):
        return self, None

    async def on_vote_received(self, message: RequestVoteResponseMessage):
        if message.sender not in self._votes and message.response:
            self._votes[message.sender] = message
            if (
                message.sender not in self._server._live_quorum
                and message.sender != self._server._name
            ):
                # TODO: more checks here if the network is not trusted
                self._server._live_quorum.add(message.sender)

            num_votes = len(self._votes.keys())
            total_nodes = self._server._total_nodes
            logger.debug(f"{num_votes} {total_nodes}\n{message}")
            # Guard for the case we're network partitioned from other nodes.
            # We shouldn't promote ourselves to a leader if the network comes
            # back
            if num_votes > 1 and num_votes > (total_nodes / 2):
                self.timer.cancel()
                leader = Leader()
                leader.set_server(self._server)

                return leader, None
        return self, None

    async def _start_election(self):
        self._server._currentTerm = Term(1 + self._server._currentTerm)
        election = RequestVoteMessage(
            self._server._name,
            None,
            self._server._currentTerm,
            last_log_index=self._server._lastLogIndex,
            last_log_term=self._server._lastLogTerm,
        )

        # Vote for self
        await self._send_vote_response_message(election)
        await self._server.send_message(election)
        self.last_vote = (self._server._currentTerm, self._server._name)
