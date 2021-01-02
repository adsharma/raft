#!/usr/bin/env python3

import unittest

from simpleRaft.boards.memory_board import MemoryBoard
from simpleRaft.messages.append_entries import AppendEntriesMessage
from simpleRaft.messages.request_vote import RequestVoteMessage
from simpleRaft.servers.server import ZeroMQServer as Server
from simpleRaft.states.follower import Follower


class TestFollowerServer(unittest.TestCase):
    def setUp(self):
        board = MemoryBoard()
        state = Follower()
        self.oserver = Server(0, state, [], board, [])

        board = MemoryBoard()
        state = Follower()
        self.server = Server(1, state, [], board, [self.oserver])

    def test_follower_server_on_message(self):
        msg = AppendEntriesMessage(0, 1, 2, {})
        self.server.on_message(msg)

    def test_follower_server_on_receive_message_with_lesser_term(self):

        msg = AppendEntriesMessage(0, 1, -1, {})

        self.server.on_message(msg)

        self.assertEqual(
            False, self.oserver._messageBoard.get_message().data["response"]
        )

    def test_follower_server_on_receive_message_with_greater_term(self):

        msg = AppendEntriesMessage(0, 1, 2, {})

        self.server.on_message(msg)

        self.assertEqual(2, self.server._currentTerm)

    def test_follower_server_on_receive_message_where_log_does_not_have_prevLogTerm(
        self
    ):
        self.server._log.append({"term": 100, "value": 2000})
        msg = AppendEntriesMessage(
            0,
            1,
            2,
            {
                "prevLogIndex": 0,
                "prevLogTerm": 1,
                "leaderCommit": 1,
                "entries": [{"term": 1, "value": 100}],
            },
        )

        self.server.on_message(msg)

        self.assertEqual(
            False, self.oserver._messageBoard.get_message().data["response"]
        )
        self.assertEqual([], self.server._log)

    def test_follower_server_on_receive_message_where_log_contains_conflicting_entry_at_new_index(
        self
    ):

        self.server._log.append({"term": 1, "value": 0})
        self.server._log.append({"term": 1, "value": 200})
        self.server._log.append({"term": 1, "value": 300})
        self.server._log.append({"term": 2, "value": 400})

        msg = AppendEntriesMessage(
            0,
            1,
            2,
            {
                "prevLogIndex": 0,
                "prevLogTerm": 1,
                "leaderCommit": 1,
                "entries": [{"term": 1, "value": 100}],
            },
        )

        self.server.on_message(msg)
        self.assertEqual({"term": 1, "value": 100}, self.server._log[1])
        self.assertEqual(
            [{"term": 1, "value": 0}, {"term": 1, "value": 100}], self.server._log
        )

    def test_follower_server_on_receive_message_where_log_is_empty_and_receives_its_first_value(
        self
    ):

        msg = AppendEntriesMessage(
            0,
            1,
            2,
            {
                "prevLogIndex": 0,
                "prevLogTerm": 100,
                "leaderCommit": 1,
                "entries": [{"term": 1, "value": 100}],
            },
        )

        self.server.on_message(msg)
        self.assertEqual({"term": 1, "value": 100}, self.server._log[0])

    def test_follower_server_on_receive_vote_request_message(self):
        msg = RequestVoteMessage(
            0, 1, 2, {"lastLogIndex": 0, "lastLogTerm": 0, "entries": []}
        )

        self.server.on_message(msg)

        self.assertEqual(0, self.server._state._last_vote)
        self.assertEqual(
            True, self.oserver._messageBoard.get_message().data["response"]
        )

    def test_follower_server_on_receive_vote_request_after_sending_a_vote(self):
        msg = RequestVoteMessage(
            0, 1, 2, {"lastLogIndex": 0, "lastLogTerm": 0, "entries": []}
        )

        self.server.on_message(msg)

        msg = RequestVoteMessage(2, 1, 2, {})
        self.server.on_message(msg)

        self.assertEqual(0, self.server._state._last_vote)


if __name__ == "__main__":
    unittest.main()
