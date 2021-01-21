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
        self._nextIndexes = defaultdict(int)
        self._matchIndex = defaultdict(int)
        self.timer = None  # Used by followers/candidates for leader timeout
        asyncio.create_task(self.append_entries_loop())

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

    async def on_append_entries(self, message: AppendEntriesMessage):
        for entry in message.entries:
            self._server._log.append(entry)
            self._server._lastLogIndex = len(self._server._log) - 1
            entry.term = self._server._currentTerm
            entry.index = self._server._lastLogIndex
        return self, None

    async def on_response_received(self, message):
        if hasattr(self._server, "_outstanding_index"):
            if message.id in self._server._outstanding_index:
                original_message: AppendEntriesMessage = self._server._outstanding_index[
                    message.id
                ]
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
            logger.debug(f"Backing up {message.sender} by {num_entries}")
            # Get the next log entry to send to the client.
            previousIndex = max(0, self._nextIndexes[message.sender] - num_entries)
            self._nextIndexes[message.sender] = previousIndex
            self._matchIndex[message.sender] = max(0, previousIndex - 1)

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

            await self._server.send_message(appendEntry)
        else:
            if num_entries > 0:
                # The last append was good so increase their index.
                self._matchIndex[message.sender] += num_entries
                self._nextIndexes[message.sender] = max(self._nextIndexes[message.sender], self._matchIndex[message.sender] + 1)
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
        prev_log_index = self._server._lastLogIndex - num
        message = AppendEntriesMessage(
            self._server._name,
            peer,
            self._server._currentTerm,
            leader_id=self._server._name,
            prev_log_index=prev_log_index,
            prev_log_term=self._server._log[prev_log_index].term,
            entries=self._server._log[-num:],
            leader_commit=self._server._commitIndex,
        )
        logger.debug(f"sending {num} entries in {message.id}")
        self._nextIndexes[peer] += num
        await self._server.send_message(message)

    async def append_entries_loop(self):
        while True:
            for n in self._server._neighbors:
                # With ZeroMQServer we use n.name, but for ZREServer, neighbor is an id
                if hasattr(n, "_name"):
                    n = n._name
                # With ZeroMQServer we use n.name, but for ZREServer, neighbor is an id
                last_log_index = self._server._lastLogIndex
                if self._nextIndexes[n] <= last_log_index and last_log_index > 0:
                    num = last_log_index - self._nextIndexes[n] + 1
                    await self._send_entries(n, num)

            await asyncio.sleep(SEND_ENTRIES_INTERVAL)
