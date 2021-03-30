#!/usr/bin/env python3

import asyncio
import unittest

from raft.boards.memory_board import MemoryBoard
from raft.messages.append_entries import AppendEntriesMessage, LogEntry
from raft.messages.request_vote import RequestVoteMessage
from raft.servers.server import ZeroMQServer as Server
from raft.states.follower import Follower


class TestFollowerServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        board = MemoryBoard()
        state = Follower()
        self.oserver = Server(0, state, [], board, [])

        board = MemoryBoard()
        state = Follower()
        self.server = Server(1, state, [], board, [self.oserver])
        asyncio.create_task(self.oserver.run())
        asyncio.create_task(self.server.run())

    def tearDown(self):
        self.oserver.stop()
        self.server.stop()

    async def test_follower_server_on_message(self):
        msg = AppendEntriesMessage(0, 1, 2, {})
        await self.server.on_message(msg)

    async def test_follower_server_on_receive_message_with_lesser_term(self):

        msg = AppendEntriesMessage(0, 1, -1, {})

        await self.server.on_message(msg)

        msg = await self.oserver._messageBoard.get_message()
        self.assertEqual(False, msg.response)

    async def test_follower_server_on_receive_message_with_greater_term(self):

        msg = AppendEntriesMessage(0, 1, 2, {})

        await self.server.on_message(msg)

        self.assertEqual(2, self.server._currentTerm)

    async def test_follower_server_on_receive_message_where_log_does_not_have_prevLogTerm(
        self
    ):
        self.server._log.append(LogEntry(term=100, value=2000))
        msg = AppendEntriesMessage(
            0,
            1,
            2,
            prev_log_index=1,
            prev_log_term=1,
            leader_commit=1,
            entries=[LogEntry(term=2, value=100)],
        )

        await self.server.on_message(msg)

        msg = await self.oserver._messageBoard.get_message()
        self.assertEqual(False, msg.response)
        self.assertEqual([LogEntry(term=0)], self.server._log)
        # Try again with a matching term/index
        msg = AppendEntriesMessage(
            0,
            1,
            2,
            prev_log_index=0,
            prev_log_term=0,
            leader_commit=1,
            entries=[LogEntry(term=2, value=100)],
        )
        await self.server.on_message(msg)
        self.assertEqual(
            [LogEntry(term=0), LogEntry(term=2, value=100)], self.server._log
        )

    async def test_follower_server_on_receive_message_where_log_contains_conflicting_entry_at_new_index(
        self
    ):
        self.server._log.append(LogEntry(term=1, index=1, value=0))
        self.server._log.append(LogEntry(term=1, index=2, value=200))
        self.server._log.append(LogEntry(term=1, index=3, value=300))
        self.server._log.append(LogEntry(term=1, index=4, value=400))

        msg = AppendEntriesMessage(
            0,
            1,
            2,
            prev_log_index=0,
            prev_log_term=0,
            leader_commit=1,
            entries=[LogEntry(term=1, index=1, value=100)],
        )

        await self.server.on_message(msg)
        self.assertEqual(
            [LogEntry(term=0), LogEntry(term=1, index=1, value=100)], self.server._log
        )

    async def test_follower_server_on_receive_message_where_log_is_empty_and_receives_its_first_value(
        self
    ):

        msg = AppendEntriesMessage(
            0,
            1,
            2,
            prev_log_index=0,
            prev_log_term=0,
            leader_commit=1,
            entries=[LogEntry(term=1, value=100)],
        )

        await self.server.on_message(msg)
        self.assertEqual(
            [LogEntry(term=0), LogEntry(term=1, value=100)], self.server._log
        )

    async def test_follower_server_on_receive_vote_request_message(self):
        msg = RequestVoteMessage(0, 1, 2)

        await self.server.on_message(msg)

        self.assertEqual((2, 0), self.server._state.last_vote)
        msg = await self.oserver._messageBoard.get_message()
        self.assertEqual(True, msg.response)

    async def test_follower_server_on_receive_vote_request_after_sending_a_vote(self):
        msg = RequestVoteMessage(0, 1, 2)

        await self.server.on_message(msg)

        msg = RequestVoteMessage(2, 1, 2, {})
        await self.server.on_message(msg)

        self.assertEqual((2, 0), self.server._state.last_vote)


if __name__ == "__main__":
    unittest.main()
