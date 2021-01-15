#!/usr/bin/env python3

import unittest

from simpleRaft.boards.memory_board import MemoryBoard
from simpleRaft.messages.append_entries import AppendEntriesMessage, LogEntry
from simpleRaft.messages.request_vote import RequestVoteMessage
from simpleRaft.servers.server import ZeroMQServer as Server
from simpleRaft.states.candidate import Candidate
from simpleRaft.states.follower import Follower
from simpleRaft.states.leader import Leader


class TestLeaderServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):

        followers = []
        for i in range(1, 4):
            board = MemoryBoard()
            state = Follower(timeout=1)
            followers.append(Server(i, state, [], board, []))

        board = MemoryBoard()
        state = Leader()

        self.leader = Server(0, state, [], board, followers)

        for i in followers:
            i._neighbors.append(self.leader)

        # Consume the heart beat message sent from set_server()
        for i in self.leader._neighbors:
            await i.on_message(await i._messageBoard.get_message())

    async def _perform_heart_beat(self):
        await self.leader._state._send_heart_beat()
        for i in self.leader._neighbors:
            await i.on_message(await i._messageBoard.get_message())

        for _, i in self.leader._messageBoard._board._queue:
            await self.leader.on_message(i)

    async def test_leader_server_sends_heartbeat_to_all_neighbors(self):

        await self._perform_heart_beat()
        self.assertEqual({1: 1, 2: 1, 3: 1}, self.leader._state._nextIndexes)

    async def test_leader_server_sends_appendentries_to_all_neighbors_and_is_appended_to_their_logs(
        self
    ):

        await self._perform_heart_beat()

        msg = AppendEntriesMessage(
            0, None, 1, leader_commit=1, entries=[LogEntry(term=1, value=100)]
        )

        await self.leader.send_message(msg)

        for i in self.leader._neighbors:
            await i.on_message(await i._messageBoard.get_message())

        for i in self.leader._neighbors:
            self.assertEqual([LogEntry(term=1, value=100)], i._log)

    async def test_leader_server_sends_appendentries_to_all_neighbors_but_some_have_dirtied_logs(
        self
    ):

        self.leader._neighbors[0]._log.append(LogEntry(term=1, value=100))
        self.leader._neighbors[0]._log.append(LogEntry(term=2, value=200))
        self.leader._neighbors[0]._log.append(LogEntry(term=3, value=200))
        self.leader._log.append(LogEntry(term=1, value=100))

        await self._perform_heart_beat()

        msg = AppendEntriesMessage(
            0, None, 1, leader_commit=1, entries=[LogEntry(term=1, value=100)]
        )

        await self.leader.send_message(msg)

        for i in self.leader._neighbors:
            await i.on_message(await i._messageBoard.get_message())

        for i in self.leader._neighbors:
            self.assertEqual([LogEntry(term=1, value=100)], i._log)

    async def test_timeout(self):
        pass
        # await asyncio.sleep(2)
        # Uncomment after figuring out how to mock leader timeout
        # self.assertTrue(mock.on_leader_timeout.called)


if __name__ == "__main__":
    unittest.main()
