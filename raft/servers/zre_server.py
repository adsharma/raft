import asyncio
import logging
import threading
import uuid

from cachetools import TTLCache
from pyre import Pyre
from serde.msgpack import from_msgpack, to_msgpack
from typing import List, Union

from ..boards.memory_board import MemoryBoard
from ..messages.append_entries import AppendEntriesMessage, LogEntry, Command
from ..messages.base import BaseMessage, Peer
from ..states.state import State
from .server import HashedLog, Server

logger = logging.getLogger("raft")


class ZREServer(Server):
    "This implementation is suitable for multi-process testing"

    DIGEST_SIZE = 32

    def __init__(
        self,
        group,
        name,
        state: State,
        node: Pyre,
        # DBM file that stores stable storage state for raft
        stable_storage,
        log=None,
        messageBoard=None,
        parent=None,
    ):
        if log is None:
            log = HashedLog()
            log.append(LogEntry(term=0))  # According to the raft spec
        if messageBoard is None:
            messageBoard = MemoryBoard()

        super().__init__(
            node.uuid().hex,
            state,
            log,
            messageBoard,
            [],
            set(),
            set(),
            _stable_storage=stable_storage,
        )
        self.group = group
        self._node = node
        self._human_name = name
        self._outstanding_index = TTLCache(maxsize=128, ttl=10)

        # Sometimes several instances of consensus are arranged in a
        # hierarchy. In order to become a candidate in the child consensus,
        # you have to be a leader in the parent. Note that in the presence
        # of failures, the parent and the child consensus could have
        # different leaders at the same time.
        self._parent = parent

    def add_neighbor(self, neighbor: Peer):
        loop = asyncio.get_event_loop()
        task = loop.create_task(self.quorum_set(str(neighbor), "add"))
        self._neighbors.append(neighbor)
        return task

    def remove_neighbor(self, neighbor: Peer):
        loop = asyncio.get_event_loop()
        task = loop.create_task(self.quorum_set(str(neighbor), "remove"))
        self._neighbors.remove(neighbor)
        if neighbor in self._quorum:
            self._quorum.remove(neighbor)
        if neighbor in self._live_quorum:
            self._live_quorum.remove(neighbor)
        return task

    def quorum_update(self, entries: List[LogEntry]) -> None:
        for entry in entries:
            assert entry.command == Command.QUORUM_PUT
            if entry.value == "add":
                self._quorum.add(entry.key)
            elif entry.value == "remove":
                if entry.key in self._quorum:
                    self._quorum.remove(entry.key)
                if entry.key in self._live_quorum:
                    self._live_quorum.remove(entry.key)
                # TODO: if the leader is removed, needs to step down
        self._total_nodes = len(self._quorum)

    async def send_message(self, message: Union[BaseMessage, bytes]):
        logger.debug(f"sending: {self._state}: {message}")
        if isinstance(message, AppendEntriesMessage):
            self._outstanding_index[message.id] = message
        if isinstance(message, bytes):
            self._node.shout(self.group, b"/raft " + message)
        else:
            if message.receiver == self._name:
                await self._receive_message(message)
                return
            elif message.receiver is not None:
                # Disambiguate in cases where a peer is in multiple groups
                message.group = self.group

            message_bytes = to_msgpack(message, ext_dict=BaseMessage.EXT_DICT_REVERSED)
            digest = message.hash().digest()
            assert len(digest) == self.DIGEST_SIZE
            message_bytes = digest + message_bytes
            if message.receiver is None:
                self._node.shout(self.group, b"/raft " + message_bytes)
            else:
                if type(message.receiver) != str:
                    raise Exception(
                        f"Expected node.uuid().hex here, got: {message.receiver}"
                    )
                self._node.whisper(
                    uuid.UUID(message.receiver),  # type: ignore
                    b"/raft " + message_bytes,
                )

    async def receive_message(self, message_bytes: bytes):
        try:
            message_hash, message_bytes = (
                message_bytes[0 : self.DIGEST_SIZE],
                message_bytes[self.DIGEST_SIZE :],
            )
            message = from_msgpack(
                BaseMessage, message_bytes, ext_dict=BaseMessage.EXT_DICT
            )
            if message_hash != message.hash().digest():
                raise Exception(f"message hash {message_hash} doesn't match {message}")

        except Exception as e:
            logger.info(f"Got exception: {e}")
            return
        if message.group is not None and message.group != self.group:
            return
        await self._receive_message(message)

    async def _receive_message(self, message: BaseMessage):
        await self.on_message(message)
        await self.post_message(message)

    async def post_message(self, message):
        await self._messageBoard.post_message(message)

    async def on_message(self, message):
        logger.debug(f"---------- on_message start -----------")
        logger.debug(f"{self._state}: {message}")
        state, response = await self._state.on_message(message)
        logger.debug(f"{state}: {response}")
        logger.debug(f"---------- on_message end -----------")

        self._state = state

    async def wait_for(self, expected_index, expected_id) -> None:
        def check_condition():
            return (
                self._commitIndex >= expected_index
                and self._log[expected_index].id == expected_id
            )

        async with self._condition:
            await self._condition.wait_for(check_condition)
            entries = [
                e
                for e in self._server._log[expected_index : self._commitIndex + 1]
                if e.command == Command.QUORUM_PUT
            ]
            self.quorum_update(entries)
            self._condition_event.set()

    async def set(self, key: str, value: str):
        leader = self._state.leader
        if leader is not None:
            append_entries = AppendEntriesMessage(
                self._name,
                leader,
                self._currentTerm,
                entries=[
                    LogEntry(
                        term=self._currentTerm,
                        index=self._commitIndex,
                        key=key,
                        value=value,
                    )
                ],
            )
            expected_index = self._commitIndex + 1
            await self.send_message(append_entries)
            self._condition_event = threading.Event()
            return (self.wait_for, expected_index, append_entries.id)
        else:
            raise Exception("Leader not found")

    async def get(self, key: str):
        return await self._messageBoard.get(key)

    async def quorum_set(self, neighbor: str, op: str):
        leader = self._state.leader
        if leader is not None:
            if leader != self._name:
                # Let the leader handle this
                async def nop():
                    pass

                return nop
            append_entries = AppendEntriesMessage(
                self._name,
                leader,
                self._currentTerm,
                id="set",  # Just so all nodes compute the same hash
                entries=[
                    LogEntry(
                        command=Command.QUORUM_PUT,
                        term=self._currentTerm,
                        index=self._commitIndex,
                        key=neighbor,
                        value=op,
                    )
                ],
            )
            expected_index = self._commitIndex + 1
            await self.send_message(append_entries)
            self._condition_event = threading.Event()
            return (self.wait_for, expected_index, append_entries.entries[0].id)
        else:
            if self._currentTerm > 0:
                raise Exception("Leader not found")
