#!/usr/bin/env python3

import unittest

from simpleRaft.boards.memory_board import MemoryBoard
from simpleRaft.messages.append_entries import AppendEntriesMessage
from simpleRaft.messages.request_vote import RequestVoteMessage
from simpleRaft.servers.server import ZeroMQServer as Server
from simpleRaft.states.candidate import Candidate
from simpleRaft.states.follower import Follower
from simpleRaft.states.leader import Leader


class TestCandidateServer(unittest.TestCase):
    def setUp(self):
        self.oserver = Server(0, Follower())
        self.server = Server(1, Candidate())

        self.server._neighbors.append(self.oserver)
        self.oserver._neighbors.append(self.server)

        self.server._state._start_election()

    def test_candidate_server_had_intiated_the_election(self):

        self.assertEqual(1, len(self.oserver._messageBoard._board))

        self.oserver.on_message(self.oserver._messageBoard.get_message())

        self.assertEqual(1, len(self.server._messageBoard._board))
        self.assertEqual(True, self.server._messageBoard.get_message().data["response"])

    def test_candidate_server_had_gotten_the_vote(self):
        self.oserver.on_message(self.oserver._messageBoard.get_message())

        self.assertEqual(1, len(self.server._messageBoard._board))
        self.assertEqual(True, self.server._messageBoard.get_message().data["response"])

    def test_candidate_server_wins_election(self):
        server0 = Server(0, Follower())
        oserver = Server(1, Follower())

        server = Server(2, Candidate())

        server._neighbors.append(oserver)
        server._neighbors.append(server0)

        server0._neighbors.append(server)
        oserver._neighbors.append(server)

        server._state._start_election()

        oserver.on_message(oserver._messageBoard.get_message())
        server0.on_message(server0._messageBoard.get_message())

        server._total_nodes = 3

        server.on_message(server._messageBoard.get_message())
        server.on_message(server._messageBoard.get_message())

        self.assertEqual(type(server._state), Leader)

    def test_two_candidates_tie(self):
        followers = []

        for i in range(4):
            followers.append(Server(i, Follower()))

        c0 = Server(5, Candidate(), neighbors=followers[0:2])
        c1 = Server(6, Candidate(), neighbors=followers[2:])
        c0._state._start_election()
        c1._state._start_election()

        for i in range(2):
            followers[i]._neighbors.append(c0)
            followers[i].on_message(followers[i]._messageBoard.get_message())

        for i in range(2, 4):
            followers[i]._neighbors.append(c1)
            followers[i].on_message(followers[i]._messageBoard.get_message())

        c0._total_nodes = 6
        c1._total_nodes = 6

        for i in range(2):
            c0.on_message(c0._messageBoard.get_message())
            c1.on_message(c1._messageBoard.get_message())

        self.assertEqual(type(c0._state), Candidate)
        self.assertEqual(type(c1._state), Candidate)

    def test_two_candidates_one_wins(self):
        followers = []

        for i in range(6):
            followers.append(Server(i, Follower()))

        board = MemoryBoard()
        state = Candidate()
        c0 = Server(7, Candidate(), neighbors=followers[0:2])
        c1 = Server(8, Candidate(), neighbors=followers[2:])
        c0._state._start_election()
        c1._state._start_election()

        for i in range(2):
            followers[i]._neighbors.append(c0)
            followers[i].on_message(followers[i]._messageBoard.get_message())

        for i in range(2, 6):
            followers[i]._neighbors.append(c1)
            followers[i].on_message(followers[i]._messageBoard.get_message())

        c0._total_nodes = 7
        c1._total_nodes = 7

        for i in range(2):
            c0.on_message(c0._messageBoard.get_message())

        for i in range(4):
            c1.on_message(c1._messageBoard.get_message())

        self.assertEqual(type(c0._state), Candidate)
        self.assertEqual(type(c1._state), Leader)

    def test_candidate_fails_to_win_election_so_resend_request(self):
        pass

    def test_multiple_candidates_fail_to_win_so_resend_requests(self):
        pass


if __name__ == "__main__":
    unittest.main()
