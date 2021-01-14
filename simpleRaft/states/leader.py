import asyncio
import logging
from collections import defaultdict

from ..messages.append_entries import AppendEntriesMessage
from .config import HEART_BEAT_INTERVAL
from .state import State

logger = logging.getLogger("raft")


class Leader(State):
    def __init__(self):
        self._nextIndexes = defaultdict(int)
        self._matchIndex = defaultdict(int)
        self.timer = None  # Used by followers/candidates for leader timeout

    def set_server(self, server):
        self._server = server
        self.leader = self._server._name
        logger.info(f"{self._server._name}: New Leader")
        loop = asyncio.get_event_loop()
        heart_beat_task = loop.create_task(self._send_heart_beat())

        for n in self._server._neighbors:
            # With ZeroMQServer we use n.name, but for ZREServer, neighbor is an id
            if hasattr(n, "_name"):
                n = n._name
            self._nextIndexes[n] = self._server._lastLogIndex + 1
            self._matchIndex[n] = 0

        return heart_beat_task

    async def on_response_received(self, message):
        # Was the last AppendEntries good?
        if not message.response:
            # No, so lets back up the log for this node
            self._nextIndexes[message.sender] -= 1

            # Get the next log entry to send to the client.
            previousIndex = max(0, self._nextIndexes[message.sender] - 1)
            previous = self._server._log[previousIndex]
            current = self._server._log[self._nextIndexes[message.sender]]

            # Send the new log to the client and wait for it to respond.
            appendEntry = AppendEntriesMessage(
                self._server._name,
                message.sender,
                self._server._currentTerm,
                leader_id=self._server._name,
                prev_log_index=previousIndex,
                prev_log_term=previous.term,
                entries=[current],
                leader_commit=self._server._commitIndex,
            )

            await self._send_response_message(appendEntry)
        else:
            # The last append was good so increase their index.
            self._nextIndexes[message.sender] += 1

            # Are they caught up?
            if self._nextIndexes[message.sender] > self._server._lastLogIndex:
                self._nextIndexes[message.sender] = self._server._lastLogIndex

        return self, None

    async def _send_heart_beat(self):
        message = AppendEntriesMessage(
            self._server._name,
            None,
            self._server._currentTerm,
            leader_id=self._server._name,
            prev_log_index=self._server._lastLogIndex,
            prev_log_term=self._server._lastLogTerm,
            entries=[],
            leader_commit=self._server._commitIndex,
        )
        await self._server.send_message(message)

        async def schedule_another_beat():
            await asyncio.sleep(HEART_BEAT_INTERVAL)
            await self._send_heart_beat()

        asyncio.create_task(schedule_another_beat())
