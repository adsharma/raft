from asyncio import Queue
from dataclasses import dataclass
from typing import Optional


@dataclass
class Board:
    _owner: Optional[str] = None
    _board: Optional[Queue] = None

    def set_owner(self, owner):
        self._owner = owner

    async def post_message(self, message):
        """This will post a message to the board."""

    async def get_message(self):
        """This will get the next message from the board.

        Boards act like queues, and allow multiple clients
        to write to them.
        """

    async def get(self, key: str) -> Optional[str]:
        """Get value from the key value store. Not all boards
        implement this.
        """
