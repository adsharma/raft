import logging

from ..messages.response import ResponseMessage
from .config import FOLLOWER_TIMEOUT
from .learner import Learner

logger = logging.getLogger("raft")


# A recorder is like learner, but implements a few extra
# RPCs for recovery
class Recorder(Learner):
    def __init__(self, timeout: float = FOLLOWER_TIMEOUT):
        super().__init__(timeout)

    def role(self):
        return ResponseMessage.Role.RECORDER
