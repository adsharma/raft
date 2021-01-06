import logging
import uuid
from typing import Union

from pyre import Pyre
from serde.msgpack import from_msgpack, to_msgpack

from ..boards.memory_board import MemoryBoard
from ..messages.base import BaseMessage
from ..states.state import State
from .server import Server

logger = logging.getLogger("raft")


class ZREServer(Server):
    "This implementation is suitable for multi-process testing"

    ZRE_GROUP = "raft"

    def __init__(self, name, state: State, node: Pyre, log=None, messageBoard=None):
        if log == None:
            log = []
        if messageBoard == None:
            messageBoard = MemoryBoard()

        super().__init__(node.uuid().hex, state, log, messageBoard, [])
        self._node = node
        self._human_name = name

    def add_neighbor(self, neighbor):
        self._neighbors.append(neighbor)

    def remove_neighbor(self, neighbor):
        self._neighbors.remove(neighbor)

    async def send_message(self, message: Union[BaseMessage, bytes]):
        logger.debug(f"sending: {self._state}: {message}")
        if isinstance(message, bytes):
            self._node.shout(self.ZRE_GROUP, b"/raft " + message)
        else:
            message_bytes = to_msgpack(message, ext_dict=BaseMessage.EXT_DICT)
            if message.receiver is None:
                self._node.shout(self.ZRE_GROUP, b"/raft " + message_bytes)
            else:
                self._node.whisper(
                    uuid.UUID(message.receiver), b"/raft " + message_bytes
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
