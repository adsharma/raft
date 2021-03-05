import logging

from .config import FOLLOWER_TIMEOUT
from .follower import Follower

logger = logging.getLogger("raft")


class Learner(Follower):
    def __init__(self, timeout=FOLLOWER_TIMEOUT):
        super().__init__(timeout)
        # Do not participate in voting
        self.timer.cancel()

    async def on_vote_request(self, message):
        return self, None