import logging
import uuid
from typing import Union

from cachetools import TTLCache
from pyre import Pyre
from serde.msgpack import from_msgpack, to_msgpack

from ..boards.memory_board import MemoryBoard
from ..messages.append_entries import AppendEntriesMessage, LogEntry
from ..messages.base import BaseMessage
from ..states.state import State
from .server import Server

logger = logging.getLogger("raft")


class ZREServer(Server):
    "This implementation is suitable for multi-process testing"

    ZRE_GROUP = "raft"

    def __init__(self, name, state: State, node: Pyre, log=None, messageBoard=None):
        if log is None:
            log = [LogEntry(term=0)]  # According to the raft spec
        if messageBoard is None:
            messageBoard = MemoryBoard()

        super().__init__(node.uuid().hex, state, log, messageBoard, [])
        self._node = node
        self._human_name = name
        self._outstanding_index = TTLCache(maxsize=128, ttl=10)

    def add_neighbor(self, neighbor):
        self._neighbors.append(neighbor)
        self._total_nodes = len(self._neighbors) + 1

    def remove_neighbor(self, neighbor):
        self._neighbors.remove(neighbor)
        self._total_nodes = len(self._neighbors) + 1

    async def send_message(self, message: Union[BaseMessage, bytes]):
        logger.debug(f"sending: {self._state}: {message}")
        if isinstance(message, AppendEntriesMessage):
            self._outstanding_index[message.id] = message
        if isinstance(message, bytes):
            self._node.shout(self.ZRE_GROUP, b"/raft " + message)
        else:
            if message.receiver == self._name:
                await self._receive_message(message)
                return

            message_bytes = to_msgpack(message, ext_dict=BaseMessage.EXT_DICT)
            if message.receiver is None:
                self._node.shout(self.ZRE_GROUP, b"/raft " + message_bytes)
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
            message = from_msgpack(
                BaseMessage, message_bytes, ext_dict=BaseMessage.EXT_DICT
            )

        except Exception as e:
            logger.info(f"Got exception: {e}")
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

    async def set(self, key: str, value: str) -> None:
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
            await self.send_message(append_entries)
            # TODO: wait for the leader to respond
        else:
            raise Exception("Leader not found")

    async def get(self, key: str):
        return await self._messageBoard.get(key)
