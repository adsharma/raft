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


if __name__ == "__main__":
    unittest.main()
