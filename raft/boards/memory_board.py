import asyncio
from dataclasses import dataclass
from typing import Optional

from .board import Board


@dataclass
class MemoryBoard(Board):
    def __post_init__(self):
        self._board = asyncio.PriorityQueue()

    def clear(self):
        self._board = asyncio.PriorityQueue()

    async def post_message(self, message):
        self._board.put_nowait((message.timestamp, message))

    async def get_message(self):
        try:
            return (await self._board.get())[1]
        except IndexError:
            return None

    async def get(self, key: str) -> Optional[str]:
        raise Exception("Not implemented")
