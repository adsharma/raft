import glob
import os
import shelve
from dataclasses import dataclass
from typing import Optional

from raft.messages.base import BaseMessage

from ..messages.append_entries import AppendEntriesMessage, Command
from .board import Board


@dataclass
class DBBoard(Board):
    prefix: str = ""

    def __post_init__(self):
        self._db = shelve.open(f"{self.prefix}_log.db", writeback=True)
        self._messages = shelve.open(f"{self.prefix}_msg.db")
        self._kv = shelve.open(f"{self.prefix}_kv.db")
        self.i = 0  # increments on writing to log.db
        self.lsn = 0  # increments on writing to msg.db

    def clear(self):
        for i in glob.glob(f"{self.prefix}*.db"):
            os.unlink(i)

    async def post_message(self, message: BaseMessage):
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

    async def get_message(self) -> Optional[BaseMessage]:
        try:
            last_lsn = self.lsn - 1
            return self._messages[f"{last_lsn:09}"]
        except Exception as e:
            print(e)
            return None

    def __iter__(self):
        for i in range(self.lsn):
            yield self._messages[f"{i:09}"]

    async def get(self, key: str) -> Optional[str]:
        try:
            return self._kv[key]
        except Exception as e:
            print(e)
            return None
