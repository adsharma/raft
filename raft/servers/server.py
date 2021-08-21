import asyncio
import dbm
import hashlib
import logging
import os
import random
import threading
import zmq
import zmq.asyncio

from cachetools.ttl import TTLCache
from dataclasses import dataclass, field
from typing import Any, List, Optional, Set

from ..boards.memory_board import Board, MemoryBoard
from ..messages.base import Term, Peer
from ..messages.append_entries import LogEntry
from ..states.state import State


class HashedLog(List[LogEntry]):
    def __init__(self):
        super().__init__()
        self._hash = hashlib.sha256(b"")

    def append(self, entry: LogEntry):
        super().append(entry)
        self._hash = hashlib.sha256(self._hash.digest() + entry.hash().digest())

    def __getitem__(self, key):
        """So we return a HashedLog instead of lists on slicing"""
        if isinstance(key, slice):
            indices = range(*key.indices(len(self)))
            ret = HashedLog()
            for i in indices:
                ret.append(self.__getitem__(i))
            return ret
        return super().__getitem__(key)

    def digest(self) -> bytes:
        return self._hash.digest()

    def __repr__(self) -> bytes:
        return f"{self._hash.hexdigest()}: " + super().__repr__()


@dataclass
class Server:
    _name: str
    _state: State
    _log: List
    _messageBoard: Board = field(repr=False)
    _neighbors: List[Peer]
    _quorum: Set[str]
    # These are members of the quorum that have voted in an election
    # or responded to heart beat
    _live_quorum: Set[str]

    # Internal state
    _stable_storage: Any = field(repr=False)
    _commitIndex: int = 0
    _currentTerm: Term = Term(0)
    _lastApplied: int = 0
    _lastLogIndex: int = 0
    _lastLogTerm: Term = Term(0)
    _outstanding_index: Optional[TTLCache] = field(default=None, repr=False)
    _parent: Optional["Server"] = field(default=None, repr=False)

    def __post_init__(self):
        self.group = "raft"
        self._human_name = self._name
        self._clear()
        if not self._stable_storage:
            # Use for unit test only
            self._dbm_filename = f"/tmp/{self._name}-{random.randrange(1 << 32)}.db"
            self._stable_storage = dbm.open(
                self._dbm_filename, "cs"  # type: ignore (typeshed#5175)
            )
        self._state.set_server(self)
        self._messageBoard.set_owner(self)
        self._condition = asyncio.Condition()
        self._condition_event: Optional[threading.Event] = None

    def __del__(self):
        if self._dbm_filename is not None:
            self._stable_storage.close()
            os.unlink(self._dbm_filename)

    def _clear(self):
        self._quorum = set()
        self._live_quorum = {self._name}
        self._total_nodes = len(self._neighbors) + 1
        self._log = HashedLog()
        self._log.append(LogEntry(term=0))  # Dummy node per raft spec
        self._commitIndex = 0
        self._currentTerm = Term(0)
        self._lastApplied = 0
        self._lastLogIndex = 0
        self._lastLogTerm = Term(0)
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
        ...

    def remove_neighbor(self, neighbor):
        ...

    def quorum_update(self, entry: List[LogEntry]):
        ...

    async def quorum_set(self, neighbor: str, op: str) -> None:
        pass


class ZeroMQServer(Server):
    "This implementation is suitable for single process testing"

    def __init__(
        self,
        name,
        state: State,
        log=None,
        messageBoard=None,
        neighbors: List["ZeroMQServer"] = None,
        port=0,
    ):
        if log is None:
            log = HashedLog()
        if neighbors is None:
            neighbors = []
        if messageBoard is None:
            messageBoard = MemoryBoard()
        self._all_neighbors = {}
        for n in neighbors:
            self._all_neighbors[n._name] = n

        super().__init__(
            name,
            state,
            log,
            messageBoard,
            list(self._all_neighbors.keys()),
            set(),
            set(),
            _stable_storage=None,
        )
        self._port = port
        self._stop = False

    def add_neighbor(self, neighbor: "ZeroMQServer"):
        self._all_neighbors[neighbor._name] = neighbor
        self._neighbors.append(neighbor._name)
        self._quorum.add(neighbor._name)
        self._total_nodes = len(self._neighbors) + 1

    def remove_neighbor(self, neighbor: "ZeroMQServer"):
        self._neighbors.remove(neighbor._name)
        self._quorum.remove(neighbor._name)
        del self._all_neighbors[neighbor._name]
        self._total_nodes = len(self._neighbors) + 1

    def get_neighbor(self, name) -> Optional["ZeroMQServer"]:
        return self._all_neighbors.get(name, None)

    async def subscriber(self):
        logger = logging.getLogger("raft")
        context = zmq.asyncio.Context()
        socket = context.socket(zmq.SUB)
        for n_id in self._neighbors:
            n = self._all_neighbors[n_id]
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
            for n_id in self._neighbors:
                n = self._all_neighbors[n_id]
                message._receiver = n._name
                await n.post_message(message)
        else:
            for n_id in self._neighbors:
                n = self._all_neighbors[n_id]
                if n._name == message.receiver:
                    await n.post_message(message)
                    break

    async def receive_message(self, message):
        n = [n for n in self._all_neighbors.values() if n._name == message.receiver]
        if len(n) > 0:
            await n[0].post_message(message)

    async def _receive_message(self, message):
        await self.receive_message(message)

    async def post_message(self, message):
        await self._messageBoard.post_message(message)

    async def on_message(self, message):
        state, response = await self._state.on_message(message)

        self._state = state
