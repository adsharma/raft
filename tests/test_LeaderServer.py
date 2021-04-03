#!/usr/bin/env python3

import unittest

from raft.boards.memory_board import MemoryBoard
from raft.messages.append_entries import AppendEntriesMessage, LogEntry
from raft.servers.server import ZeroMQServer as Server
from raft.states.follower import Follower
from raft.states.leader import Leader


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
            i.add_neighbor(self.leader)

        # Consume the heart beat message sent from set_server()
        for i in self.leader._all_neighbors.values():
            await i.on_message(await i._messageBoard.get_message())

    async def _perform_heart_beat(self):
        await self.leader._state._send_heart_beat()
        for i in self.leader._all_neighbors.values():
            await i.on_message(await i._messageBoard.get_message())

        for _, i in self.leader._messageBoard._board._queue:
            await self.leader.on_message(i)

    async def test_leader_server_sends_heartbeat_to_all_neighbors(self):
        await self._perform_heart_beat()
        self.assertEqual({1: 1, 2: 1, 3: 1}, self.leader._state._nextIndex)

    async def test_leader_server_sends_appendentries_to_all_neighbors_and_is_appended_to_their_logs(
        self
    ):

        await self._perform_heart_beat()

        msg = AppendEntriesMessage(
            0, None, 1, leader_commit=1, entries=[LogEntry(term=1, value=100)]
        )

        await self.leader.send_message(msg)

        for i in self.leader._all_neighbors.values():
            await i.on_message(await i._messageBoard.get_message())

        for i in self.leader._all_neighbors.values():
            self.assertEqual([LogEntry(term=0), LogEntry(term=1, value=100)], i._log)

    async def test_leader_server_sends_appendentries_to_all_neighbors_but_some_have_dirtied_logs(
        self
    ):
        n0 = self.leader.get_neighbor(self.leader._neighbors[0])
        n0._log.append(LogEntry(term=1, index=1, value=100))
        n0._log.append(LogEntry(term=2, index=0, value=200))
        n0._log.append(LogEntry(term=3, index=0, value=200))
        self.leader._log.append(LogEntry(term=1, index=1, value=100))

        await self._perform_heart_beat()

        msg = AppendEntriesMessage(
            0, None, 2, leader_commit=1, entries=[LogEntry(term=1, value=100)]
        )

        await self.leader.send_message(msg)

        for i in self.leader._all_neighbors.values():
            await i.on_message(await i._messageBoard.get_message())

        for i in self.leader._all_neighbors.values():
            self.assertEqual([LogEntry(term=0), LogEntry(term=1, value=100)], i._log)

    async def test_timeout(self):
        pass
        # await asyncio.sleep(2)
        # Uncomment after figuring out how to mock leader timeout
        # self.assertTrue(mock.on_leader_timeout.called)


class TestLeaderServerRaftPDF(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        followers = []
        for i in range(1, 5):
            board = MemoryBoard()
            state = Follower(timeout=1)
            followers.append(Server(i, state, [], board, []))

        board = MemoryBoard()
        state = Leader()

        self.leader = Server(0, state, [], board, followers)

        for i in followers:
            i.add_neighbor(self.leader)

        # Consume the heart beat message sent from set_server()
        for i in self.leader._all_neighbors.values():
            await i.on_message(await i._messageBoard.get_message())

    async def _perform_heart_beat(self):
        await self.leader._state._send_heart_beat()
        for i in self.leader._all_neighbors.values():
            await i.on_message(await i._messageBoard.get_message())

        for _, i in self.leader._messageBoard._board._queue:
            await self.leader.on_message(i)

    async def test_raft_fig8(self):
        leader = self.leader

        leader._commitIndex = 1
        leader._currentTerm = 1
        log_entry_index1 = LogEntry(term=1, index=1, value=1)
        leader._log.append(log_entry_index1)
        for i in range(4):
            n_i = self.leader.get_neighbor(self.leader._neighbors[i])
            n_i._log.append(log_entry_index1)
            leader._state._nextIndex[i] = 1
            leader._state._matchIndex[i] = 1

        n_3 = self.leader.get_neighbor(self.leader._neighbors[3])
        n_3._log.append(LogEntry(term=3, index=2, value=3))
        leader._lastLogIndex = 1
        leader._lastLogTerm = 1

        await self._perform_heart_beat()

        # Consume all messages
        while len(self.leader._messageBoard._board._queue):
            i = await self.leader._messageBoard.get_message()
            await self.leader.on_message(i)

        log_entry_index2 = LogEntry(term=2, index=2, value=2)
        leader._log.append(log_entry_index2)
        msg = AppendEntriesMessage(
            0,
            None,
            1,
            prev_log_index=1,
            prev_log_term=1,
            leader_commit=1,
            entries=[log_entry_index2],
        )

        await leader.send_message(msg)

        for i in leader._neighbors[0:3]:
            iserver = leader.get_neighbor(i)
            if iserver is not None:
                await iserver.on_message(await iserver._messageBoard.get_message())

        leader._commitIndex = 1
        leader._currentTerm = 4
        leader._log.append(LogEntry(term=4, index=3, value=4))

        for i in range(3):
            await leader._state.on_response_received(
                await leader._messageBoard.get_message(), msg
            )

        self.assertEqual(leader._commitIndex, 1)


if __name__ == "__main__":
    unittest.main()
