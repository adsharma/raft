import logging

from ..messages.request_vote import RequestVoteMessage
from .config import FOLLOWER_TIMEOUT
from .follower import Follower

logger = logging.getLogger("raft")


class Learner(Follower):
    def __init__(self, timeout: float = FOLLOWER_TIMEOUT):
        super().__init__(timeout)
        # Do not participate in voting
        self.timer.cancel()
        self.learner = True

    async def on_vote_request(self, message: RequestVoteMessage):
        return self, None
