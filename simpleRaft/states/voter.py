import asyncio

from ..messages.request_vote import RequestVoteResponseMessage
from .state import State


class Voter(State):
    def __init__(self, timeout):
        super().__init__()
        self._last_vote = None
        self._timeout = timeout
        self._timeoutTime = self._nextTimeout()
        self.timer = self.restart_timer()

    def restart_timer(self):
        loop = asyncio.get_event_loop()
        return loop.call_later(self._timeoutTime, self.on_leader_timeout, self)

    def on_vote_request(self, message):
        if (
            self._last_vote is None
            and message.data["lastLogIndex"] >= self._server._lastLogIndex
        ):
            self._last_vote = message.sender
            self._send_vote_response_message(message)
        else:
            self._send_vote_response_message(message, yes=False)

        return self, None

    def _send_vote_response_message(self, msg, yes=True):
        voteResponse = RequestVoteResponseMessage(
            self._server._name, msg.sender, msg.term, {"response": yes}
        )
        self._server.receive_message(voteResponse)

    def on_leader_timeout(self):
        """This is called when the leader timeout is reached."""
        from .candidate import Candidate  # TODO: Fix circular import
        candidate = Candidate()
        candidate.set_server(self._server)

        return candidate, None