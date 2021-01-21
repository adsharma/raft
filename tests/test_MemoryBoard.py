#!/usr/bin/env python3

import unittest

from raft.boards.memory_board import MemoryBoard
from raft.messages.base import BaseMessage


class TestMemoryBoard(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.board = MemoryBoard()

    async def test_memoryboard_post_message(self):
        msg = BaseMessage.default()
        await self.board.post_message(msg)
        self.assertEqual(msg, await self.board.get_message())

    async def test_memoryboard_post_message_make_sure_they_are_ordered(self):
        msg = BaseMessage.default()
        msg2 = BaseMessage.default()
        msg2.timestamp -= 100

        await self.board.post_message(msg)
        await self.board.post_message(msg2)

        self.assertEqual(msg2, await self.board.get_message())


if __name__ == "__main__":
    unittest.main()
