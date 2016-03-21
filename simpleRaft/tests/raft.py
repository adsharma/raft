#!/usr/bin/env python

import unittest
import sys

sys.path.append('..')

from boards.memory_board import MemoryBoard
from messages.append_entries import AppendEntriesMessage
from messages.request_vote import RequestVoteMessage
from messages.base import BaseMessage
from servers.server import ZeroMQServer
from states.leader import Leader
from states.follower import Follower

N = 5

class TestRaft( unittest.TestCase ):

    @classmethod
    def setUpClass( self ):
        self.servers = []
        for i in range(N):
            if i == 0:
                state = Leader()
            else:
                state = Follower()
            s = ZeroMQServer("S%d" % i, state, [], MemoryBoard(), [], 6666+i)
            if (i == 0):
                self.leader = s
            self.servers.append(s)
        for i in range(N):
            me = self.servers[i]
            neighbors = [ n for n in self.servers if n != me ]
            me.set_neighbors(neighbors)

    @classmethod
    def tearDownClass( self ):
        pass

    def _perform_hearbeat( self ):
        self.leader._state._send_heart_beat()
        for i in self.leader._neighbors:
            i.on_message( i._messageBoard.get_message() )

        for i in self.leader._messageBoard._board:
            self.leader.on_message( i )

    def test_heartbeat( self ):

        self._perform_hearbeat()
        expected = dict(('S%d' % i, 0) for i in range(1, N))
        self.assertEquals( expected, self.leader._state._nextIndexes )

    def test_append( self ):

        self._perform_hearbeat()

        msg = AppendEntriesMessage( 0, None, 1, {
                                                "prevLogIndex": 0,
                                                "prevLogTerm": 0,
                                                "leaderCommit": 1,
                                                "entries": [ { "term": 1, "value": 100 } ] } )

        self.leader.send_message( msg )

        for i in self.leader._neighbors:
            i.on_message( i._messageBoard.get_message() )

        for i in self.leader._neighbors:
            self.assertEquals( [{ "term": 1, "value": 100 } ], i._log )


    def test_dirty( self ):

        self.leader._neighbors[0]._log.append( { "term": 2, "value": 100 } )
        self.leader._neighbors[0]._log.append( { "term": 2, "value": 200 } )
        self.leader._neighbors[1]._log.append( { "term": 3, "value": 200 } )
        self.leader._log.append( { "term": 1, "value": 100 } )

        self._perform_hearbeat()

        msg = AppendEntriesMessage( 0, None, 1, {
                                                "prevLogIndex": 0,
                                                "prevLogTerm": 0,
                                                "leaderCommit": 1,
                                                "entries": [ { "term": 1, "value": 100 } ] } )

        self.leader.send_message( msg )

        for i in self.leader._neighbors:
            i.on_message( i._messageBoard.get_message() )

        for i in self.leader._neighbors:
            self.assertEquals( [{ "term": 1, "value": 100 } ], i._log )


if __name__ == '__main__':
    unittest.main()
