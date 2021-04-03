#!/usr/bin/env python3

import unittest

from raft.servers.server import ZeroMQServer as Server
from raft.states.candidate import Candidate
from raft.states.follower import Follower
from raft.states.leader import Leader


class TestCandidateServer(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.oserver = Server(0, Follower())
        self.server = Server(1, Follower())

        self.server.add_neighbor(self.oserver)
        self.oserver.add_neighbor(self.server)

        candidate = Candidate()
        self.server._state = candidate
        await candidate.set_server(self.server)

    async def test_candidate_server_had_intiated_the_election(self):

        self.assertEqual(1, len(self.oserver._messageBoard._board._queue))

        await self.oserver.on_message(await self.oserver._messageBoard.get_message())

        self.assertEqual(1, len(self.server._messageBoard._board._queue))
        self.assertEqual(True, (await self.server._messageBoard.get_message()).response)

    async def test_candidate_server_had_gotten_the_vote(self):
        await self.oserver.on_message(await self.oserver._messageBoard.get_message())

        self.assertEqual(1, len(self.server._messageBoard._board._queue))
        self.assertEqual(True, (await self.server._messageBoard.get_message()).response)

    async def test_candidate_server_wins_election(self):
        server0 = Server(0, Follower())
        oserver = Server(1, Follower())

        server = Server(2, Candidate())

        server.add_neighbor(oserver)
        server.add_neighbor(server0)

        server0.add_neighbor(server)
        oserver.add_neighbor(server)

        await server._state._start_election()

        await oserver.on_message(await oserver._messageBoard.get_message())
        await server0.on_message(await server0._messageBoard.get_message())

        server._total_nodes = 3

        await server.on_message(await server._messageBoard.get_message())
        await server.on_message(await server._messageBoard.get_message())

        self.assertEqual(type(server._state), Leader)

    async def test_two_candidates_tie(self):
        followers = []

        for i in range(4):
            followers.append(Server(i, Follower()))

        c0 = Server(5, Candidate(), neighbors=followers[0:2])
        c1 = Server(6, Candidate(), neighbors=followers[2:])
        await c0._state._start_election()
        await c1._state._start_election()

        for i in range(2):
            followers[i].add_neighbor(c0)
            await followers[i].on_message(
                await followers[i]._messageBoard.get_message()
            )

        for i in range(2, 4):
            followers[i].add_neighbor(c1)
            await followers[i].on_message(
                await followers[i]._messageBoard.get_message()
            )

        c0._total_nodes = 6
        c1._total_nodes = 6

        for i in range(2):
            await c0.on_message(await c0._messageBoard.get_message())
            await c1.on_message(await c1._messageBoard.get_message())

        self.assertEqual(type(c0._state), Candidate)
        self.assertEqual(type(c1._state), Candidate)

    async def test_two_candidates_one_wins(self):
        followers = []

        for i in range(6):
            followers.append(Server(i, Follower()))

        c0 = Server(7, Candidate(), neighbors=followers[0:2])
        c1 = Server(8, Candidate(), neighbors=followers[2:])
        await c0._state._start_election()
        await c1._state._start_election()

        for i in range(2):
            followers[i].add_neighbor(c0)
            await followers[i].on_message(
                await followers[i]._messageBoard.get_message()
            )

        for i in range(2, 6):
            followers[i].add_neighbor(c1)
            await followers[i].on_message(
                await followers[i]._messageBoard.get_message()
            )

        c0._total_nodes = 7
        c1._total_nodes = 7

        for i in range(2):
            await c0.on_message(await c0._messageBoard.get_message())

        for i in range(4):
            await c1.on_message(await c1._messageBoard.get_message())

        self.assertEqual(type(c0._state), Candidate)
        self.assertEqual(type(c1._state), Leader)

    def test_candidate_fails_to_win_election_so_resend_request(self):
        pass

    def test_multiple_candidates_fail_to_win_so_resend_requests(self):
        pass


if __name__ == "__main__":
    unittest.main()
