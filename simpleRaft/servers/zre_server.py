from pyre import Pyre

from ..boards.memory_board import MemoryBoard
from ..states.state import State
from .server import Server


class ZREServer(Server):
    def __init__(self, name, state: State, node: Pyre, log=None, messageBoard=None):
        if log == None:
            log = []
        if messageBoard == None:
            messageBoard = MemoryBoard()

        super().__init__(name, state, log, messageBoard, [])
        self._node = node

    def add_neighbor(self, neighbor):
        self._neighbors.append(neighbor)

    def remove_neighbor(self, neighbor):
        self._neighbors.remove(neighbor)

    def send_message(self, message):
        n.shout(message)

    def receive_message(self, message):
        self.post_message(message)

    def post_message(self, message):
        self._messageBoard.post_message(message)

    def on_message(self, message):
        state, response = self._state.on_message(message)

        self._state = state
