import asyncio
import logging
from typing import Optional
import statistics
from collections import defaultdict

from ..messages.append_entries import AppendEntriesMessage, Command
from ..messages.base import Peer, Term
from ..messages.response import ResponseMessage
from ..servers.server import Server
from .config import HEART_BEAT_INTERVAL, SEND_ENTRIES_INTERVAL
from .state import State

logger = logging.getLogger("raft")


class Leader(State):
    TEST_ONLY_DISABLE_SEND_LOOP = False

    def __init__(self):
        def default_next_index() -> int:
            return self._server._lastLogIndex + 1

        super().__init__(timeout=0)
        self._nextIndex = defaultdict(default_next_index)
        self._matchIndex = defaultdict(int)
        self._learnerIndex = defaultdict(int)
        self.timer = None  # Used by followers/candidates for leader timeout
        if not self.TEST_ONLY_DISABLE_SEND_LOOP:
            asyncio.create_task(self.append_entries_loop())

    def __repr__(self):
        return f"Leader:\n\tnextIndex: {self._nextIndex}\n\tmatchIndex: {self._matchIndex}\n\tlearnerIndex: {self._learnerIndex}"

    def set_server(self, server: Server):
        self._server = server
        self.leader = self._server._name
        self.leader_name = self._server._human_name
        logger.info(f"{self._server.group}: {self._server._name}: New Leader")
        loop = asyncio.get_event_loop()
        if not self.TEST_ONLY_DISABLE_SEND_LOOP:
            heart_beat_task = loop.create_task(self._send_heart_beat())
        else:
            heart_beat_task = loop.create_task(self._send_one_heart_beat())

        for n in self._server._neighbors:
            self._nextIndex[n] = self._server._lastLogIndex + 1
            if n in self._server._quorum:
                self._matchIndex[n] = 0
            else:
                self._learnerIndex[n] = 0

        return heart_beat_task

    async def on_append_entries(self, message: AppendEntriesMessage):
        if message.term > self._server._currentTerm:
            return await self._accept_leader(message, None)

        if (
            len(message.entries) == 1
            and message.entries[0].command == Command.QUORUM_PUT
        ):
            self._server._currentTerm = Term(1 + self._server._currentTerm)

        for entry in message.entries:
            entry.id = message.id
            self._server._lastLogIndex = len(self._server._log)
            self._server._lastLogTerm = entry.term = self._server._currentTerm
            entry.index = self._server._lastLogIndex
            self._server._log.append(entry)
        return self, None

    async def on_response_received(
        self,
        message: ResponseMessage,
        test_original_message: Optional[AppendEntriesMessage] = None,
    ):
        original_message = None
        if self._server._outstanding_index is not None:
            if message.id in self._server._outstanding_index:
                original_message = self._server._outstanding_index[message.id]
                num_entries = len(original_message.entries)
            else:
                logger.warn(f"Can't find message id: {message.id}")
                return self, None
        else:
            num_entries = 0
            if test_original_message:
                original_message = test_original_message
                num_entries = len(test_original_message.entries)

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
            if num_entries == 0 and message.sender not in self._server._live_quorum:
                self._server._live_quorum.add(message.sender)
            if num_entries > 0 and original_message:
                if message.role == ResponseMessage.Role.FOLLOWER:
                    # The last append was good so increase their index.
                    self._matchIndex[message.sender] = (
                        original_message.prev_log_index + num_entries
                    )
                    self._nextIndex[message.sender] = max(
                        self._nextIndex[message.sender],
                        self._matchIndex[message.sender] + 1,
                    )
                    logger.debug(f"Advanced {message.sender} by {num_entries}")
                elif message.role == ResponseMessage.Role.LEARNER:
                    self._learnerIndex[message.sender] = (
                        original_message.prev_log_index + num_entries
                    )
                    self._nextIndex[message.sender] = max(
                        self._nextIndex[message.sender],
                        self._learnerIndex[message.sender] + 1,
                    )
                    logger.debug(f"Learner: Advanced {message.sender} by {num_entries}")
                new_commit_index = statistics.median_low(self._matchIndex.values())
                if (
                    self._server._log[new_commit_index].term
                    == self._server._currentTerm
                    and new_commit_index > self._server._commitIndex
                ):
                    self._server._commitIndex = new_commit_index
                    async with self._server._condition:
                        self._server._condition.notify_all()

        return self, None

    async def _send_one_heart_beat(self):
        parent_raft = self._server._parent
        if parent_raft is not None:
            if parent_raft._state.leader_name != self._server._human_name:
                loop = asyncio.get_event_loop()
                loop.call_soon(self.on_leader_timeout)
                return

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

    async def _send_heart_beat(self):
        write_initial_quorum = len(self._server._log) == 1
        if write_initial_quorum:
            await self._server.quorum_set(self.leader, "add")
            for n in self._server._neighbors:
                await self._server.quorum_set(str(n), "add")
            entries = [
                e
                for e in self._server._log[1 : self._server._lastLogIndex + 1]
                if e.command == Command.QUORUM_PUT
            ]
            self._server.quorum_update(entries)

        await self._send_one_heart_beat()

        async def schedule_another_beat():
            await asyncio.sleep(HEART_BEAT_INTERVAL)
            await self._send_heart_beat()

        asyncio.create_task(schedule_another_beat())

    async def _send_entries(self, peer: Peer, num: int):
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
                last_log_index = self._server._lastLogIndex
                if self._nextIndex[n] <= last_log_index and last_log_index > 0:
                    num = last_log_index - self._nextIndex[n] + 1
                    await self._send_entries(n, num)

            await asyncio.sleep(SEND_ENTRIES_INTERVAL)
