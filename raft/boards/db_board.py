import asyncio
import glob
import os
import shelve
from dataclasses import dataclass
from typing import Optional

from ..messages.append_entries import AppendEntriesMessage, Command, LogEntry
from .board import Board


@dataclass
class DBBoard(Board):
    prefix: str = ""

    def __post_init__(self):
        self._board = asyncio.PriorityQueue()
        self._db = shelve.open(f"{self.prefix}_log.db", writeback=True)
        self._messages = shelve.open(f"{self.prefix}_msg.db")
        self._kv = shelve.open(f"{self.prefix}_kv.db")
        self.i = 0  # increments on writing to log.db
        self.lsn = 0  # increments on writing to msg.db

    def clear(self):
        self._board = asyncio.PriorityQueue()
        for i in glob.glob(f"{self.prefix}*.db"):
            os.unlink(i)

    async def post_message(self, message: AppendEntriesMessage):
        self._board.put_nowait((message.timestamp, message))
        self._messages[f"{self.lsn:09}"] = message
        self.lsn += 1
        self._messages.sync()
        if not isinstance(message, AppendEntriesMessage):
            return
        for entry in message.entries:
            self._db[f"{self.i:09}"] = entry
            self.i = self.i + 1
            if entry.command == Command.PUT and entry.key is not None:
                self._kv[str(entry.key)] = entry.value
        self._db.sync()
        self._kv.sync()

    async def get_message(self) -> Optional[LogEntry]:
        try:
            return (await self._board.get())[1]
        except Exception as e:
            print(e)
            return None

    async def get(self, key: str) -> Optional[str]:
        try:
            return self._kv[key]
        except Exception as e:
            print(e)
            return None
