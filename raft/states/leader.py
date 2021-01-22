import asyncio
import logging
import statistics
from collections import defaultdict

from ..messages.append_entries import AppendEntriesMessage
from .config import HEART_BEAT_INTERVAL, SEND_ENTRIES_INTERVAL
from .state import State

logger = logging.getLogger("raft")


class Leader(State):
    def __init__(self):
        def default_next_index() -> int:
            return self._server._lastLogIndex + 1

        self._nextIndex = defaultdict(default_next_index)
        self._matchIndex = defaultdict(int)
        self.timer = None  # Used by followers/candidates for leader timeout
        asyncio.create_task(self.append_entries_loop())

    def __repr__(self):
        return (
            f"Leader:\n\tnextIndex: {self._nextIndex}\n\tmatchIndex: {self._matchIndex}"
        )

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
            self._nextIndex[n] = self._server._lastLogIndex + 1
            self._matchIndex[n] = 0

        return heart_beat_task

    async def on_append_entries(self, message: AppendEntriesMessage):
        if message.term > self._server._currentTerm:
            return await self._accept_leader(message, None)

        for entry in message.entries:
            self._server._log.append(entry)
            self._server._lastLogIndex = len(self._server._log) - 1
            self._server._lastLogTerm = entry.term = self._server._currentTerm
            entry.index = self._server._lastLogIndex
        return self, None

    async def on_response_received(self, message):
        original_message = None
        if hasattr(self._server, "_outstanding_index"):
            if message.id in self._server._outstanding_index:
                original_message = self._server._outstanding_index[message.id]
                num_entries = len(original_message.entries)
            else:
                logger.warn(f"Can't find message id: {message.id}")
                return self, None
        else:
            num_entries = 0

        # Was the last AppendEntries good?
        if not message.response:
            # No, so lets back up the log for this node
            if num_entries == 0:
                # Need to backup by at least 1
                num_entries = 1
                # Get the next log entry to send to the client.
                previousIndex = max(0, self._nextIndex[message.sender] - 1)
            else:
                # Get the next log entry to send to the client.
                previousIndex = original_message.prev_log_index

            logger.debug(
                f"Backing up {message.sender} by {num_entries} to {previousIndex}"
            )
            self._nextIndex[message.sender] = previousIndex
            return self, None
        else:
            if num_entries > 0 and original_message:
                # The last append was good so increase their index.
                self._matchIndex[message.sender] = (
                    original_message.prev_log_index + num_entries
                )
                self._nextIndex[message.sender] = max(
                    self._nextIndex[message.sender],
                    self._matchIndex[message.sender] + 1,
                )
                logger.debug(f"Advanced {message.sender} by {num_entries}")
                self._server._commitIndex = statistics.median_low(
                    self._matchIndex.values()
                )

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

    async def _send_entries(self, peer, num: int):
        prev_log_index = max(0, self._nextIndex[peer] - 1)
        message = AppendEntriesMessage(
            self._server._name,
            peer,
            self._server._currentTerm,
            leader_id=self._server._name,
            prev_log_index=prev_log_index,
            prev_log_term=self._server._log[prev_log_index].term,
            entries=self._server._log[prev_log_index + 1 : prev_log_index + 1 + num],
            leader_commit=self._server._commitIndex,
        )
        logger.debug(f"sending {num} entries in {message.id}")
        self._nextIndex[peer] += num
        await self._server.send_message(message)

    async def append_entries_loop(self):
        while True:
            for n in self._server._neighbors:
                # With ZeroMQServer we use n.name, but for ZREServer, neighbor is an id
                if hasattr(n, "_name"):
                    n = n._name
                # With ZeroMQServer we use n.name, but for ZREServer, neighbor is an id
                last_log_index = self._server._lastLogIndex
                if self._nextIndex[n] <= last_log_index and last_log_index > 0:
                    num = last_log_index - self._nextIndex[n] + 1
                    await self._send_entries(n, num)

            await asyncio.sleep(SEND_ENTRIES_INTERVAL)
