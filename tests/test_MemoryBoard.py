#!/usr/bin/env python3

import unittest
import sys

from simpleRaft.boards.memory_board import MemoryBoard
from simpleRaft.messages.base import BaseMessage


class TestMemoryBoard(unittest.TestCase):
    def setUp(self):
        self.board = MemoryBoard()

    def test_memoryboard_post_message(self):

        msg = BaseMessage(0, 0, 0, 0)
        self.board.post_message(msg)
        self.assertEqual(msg, self.board.get_message())

    def test_memoryboard_post_message_make_sure_they_are_ordered(self):

        msg = BaseMessage(0, 0, 0, 0)
        msg2 = BaseMessage(0, 0, 0, 0)
        msg2.timestamp -= 100

        self.board.post_message(msg)
        self.board.post_message(msg2)

        self.assertEqual(msg2, self.board.get_message())


if __name__ == "__main__":
    unittest.main()
