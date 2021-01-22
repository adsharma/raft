import logging

from .config import FOLLOWER_TIMEOUT
from .voter import Voter

logger = logging.getLogger("raft")


class Follower(Voter):
    def __init__(self, timeout=FOLLOWER_TIMEOUT):
        super().__init__(timeout)
        self.leader = None

    async def on_append_entries(self, message):
        await super().on_append_entries(message)
        log = self._server._log

        # Check if the leader is too far ahead in the log.
        if message.leader_commit != self._server._commitIndex:
            # If the leader is too far ahead then we
            #   use the length of the log - 1
            self._server._commitIndex = min(message.leader_commit, max(0, len(log) - 1))

        # Can't possibly be up-to-date with the log
        # If the log is smaller than the preLogIndex
        if len(log) <= message.prev_log_index:
            await self._send_response_message(message, yes=False)
            return self, None

        #   Is this a heartbeat?
        if len(message.entries) == 0:
            await self._send_response_message(message)
            return self, None

        # We need to hold the induction proof of the algorithm here.
        #   So, we make sure that the prevLogIndex term is always
        #   equal to the server.
        if len(log) > 0 and log[message.prev_log_index].term != message.prev_log_term:
            # There is a conflict we need to resync so delete everything
            #   from this prevLogIndex and forward and send a failure
            #   to the server.
            self._server._log = log = log[: message.prev_log_index]
            self._server._lastLogIndex = message.prev_log_index
            self._server._lastLogTerm = log[-1].term if len(log) > 0 else 0

            await self._send_response_message(message, yes=False)
            return self, None
        # The induction proof held so lets check if the commitIndex
        #   value is the same as the one on the leader
        # Make sure that leaderCommit is > 0 and that the
        #   data is different here
        if (
            len(log) > 0
            and message.leader_commit > 0
            and message.leader_commit < len(log)
            and log[message.leader_commit].term != message.term
        ):
            # Data was found to be different so we fix that
            #   by taking the current log and slicing it to the
            #   leaderCommit + 1 range then setting the last
            #   value to the commitValue
            self._server._log = log = log[: self._server._commitIndex]

        # Apply the log entries
        for e in message.entries:
            log.append(e)
            self._server._commitIndex += 1

        self._server._lastLogIndex = len(log) - 1
        self._server._lastLogTerm = log[-1].term

        await self._send_response_message(message)
        return self, None
