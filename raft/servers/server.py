import asyncio
import logging
from dataclasses import dataclass
from typing import Any, List, Optional

import dbm
import os
import random
import threading
import zmq
import zmq.asyncio

from ..boards.memory_board import Board, MemoryBoard
from ..messages.append_entries import LogEntry
from ..states.state import State


@dataclass
class Server:
    _name: str
    _state: State
    _log: List
    _messageBoard: Board
    _neighbors: List

    # Internal state
    _stable_storage: Any
    _commitIndex: int = 0
    _currentTerm: int = 0
    _lastApplied: int = 0
    _lastLogIndex: int = 0
    _lastLogTerm: Optional[int] = None
    _parent: Optional["Server"] = None

    def __post_init__(self):
        self.group = "raft"
        self._human_name = self._name
        self._clear()
        if not self._stable_storage:
            # Use for unit test only
            self._dbm_filename = f"/tmp/{self._name}-{random.randrange(1 << 32)}.db"
            self._stable_storage = dbm.open(self._dbm_filename, "cs")
        self._state.set_server(self)
        self._messageBoard.set_owner(self)
        self._condition = asyncio.Condition()
        self._condition_event: Optional[threading.Event] = None

    def __del__(self):
        if self._dbm_filename is not None:
            self._stable_storage.close()
            os.unlink(self._dbm_filename)

    def _clear(self):
        self._total_nodes = len(self._neighbors) + 1
        self._log = [LogEntry(term=0)]  # Dummy node per raft spec
        self._commitIndex = 0
        self._currentTerm = 0
        self._lastApplied = 0
        self._lastLogIndex = 0
        self._lastLogTerm = None
        self._dbm_filename = None

    async def send_message(self, message):
        ...

    async def receive_message(self, message):
        "Use this for the general case"
        ...

    async def _receive_message(self, message):
        "Use this for local message delivery"
        ...

    async def post_message(self, message):
        ...

    async def on_message(self, message):
        ...

    def add_neighbor(self, neighbor):
        self._neighbors.append(neighbor)
        self._total_nodes = len(self._neighbors) + 1

    def remove_neighbor(self, neighbor):
        self._neighbors.remove(neighbor)
        self._total_nodes = len(self._neighbors) + 1

    async def quorum_set(self, neighbor: str, op: str) -> None:
        pass


class ZeroMQServer(Server):
    "This implementation is suitable for single process testing"

    def __init__(
        self, name, state: State, log=None, messageBoard=None, neighbors=None, port=0
    ):
        if log is None:
            log = []
        if neighbors is None:
            neighbors = []
        if messageBoard is None:
            messageBoard = MemoryBoard()

        super().__init__(name, state, log, messageBoard, neighbors, _stable_storage=None)
        self._port = port
        self._stop = False

    async def subscriber(self):
        logger = logging.getLogger("raft")
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.SUB)
        for n in self._neighbors:
            socket.connect("tcp://%s:%d" % (n._name, n._port))

        while not self._stop:
            try:
                message = await socket.recv()
            except zmq.error.ContextTerminated:
                break
            if not message:
                continue
            logger.debug(f"Got message: {message}")
            await self.on_message(message)
        socket.close()

    async def publisher(self):
        logger = logging.getLogger("raft")
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.PUB)
        if self._port == 0:
            self._port = socket.bind_to_random_port("tcp://*")
        else:
            socket.bind("tcp://*:%d" % self._port)
        logger.info(f"publish port: {self._port}")

        while not self._stop:
            message = await self._messageBoard.get_message()
            if not message:
                continue  # sleep wait?
            try:
                await socket.send_string(str(message))
            except Exception as e:
                print(e)
                break
        socket.close()

    async def run(self):
        asyncio.create_task(self.publisher())
        asyncio.create_task(self.subscriber())

    def stop(self):
        self._stop = True

    async def send_message(self, message):
        if message.receiver is None:
            for n in self._neighbors:
                message._receiver = n._name
                await n.post_message(message)
        else:
            for n in self._neighbors:
                if n._name == message.receiver:
                    await n.post_message(message)
                    break

    async def receive_message(self, message):
        n = [n for n in self._neighbors if n._name == message.receiver]
        if len(n) > 0:
            await n[0].post_message(message)

    async def _receive_message(self, message):
        await self.receive_message(message)

    async def post_message(self, message):
        await self._messageBoard.post_message(message)

    async def on_message(self, message):
        state, response = await self._state.on_message(message)

        self._state = state
