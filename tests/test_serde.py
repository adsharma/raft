#!/usr/bin/env python3

import unittest

from raft.messages.append_entries import AppendEntriesMessage, LogEntry
from raft.messages.base import BaseMessage, Term
from serde.msgpack import from_msgpack, to_msgpack


class TestSerde(unittest.TestCase):
    def test_serde(self):
        message = AppendEntriesMessage(
            "test", "foo", Term(0), entries=[LogEntry(term=Term(0), index=0)]
        )
        message_bytes = to_msgpack(message, ext_dict=BaseMessage.EXT_DICT_REVERSED)
        decoded_message = from_msgpack(
            BaseMessage, message_bytes, ext_dict=BaseMessage.EXT_DICT
        )
        self.assertEqual(message.id, decoded_message.id)
        self.assertEqual(message, decoded_message)

    def test_hash(self):
        message = AppendEntriesMessage(
            "test",
            "foo",
            Term(0),
            id=1,
            timestamp=0,
            entries=[LogEntry(term=Term(0), index=0)],
        )
        self.assertEqual(
            message.hash().hexdigest(),
            "edf251804da904e6c51513166fed1491c3a48135f9670c98c1ee7368725888b1",
        )


if __name__ == "__main__":
    unittest.main()
