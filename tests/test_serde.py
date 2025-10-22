#!/usr/bin/env python3

import unittest

from raft.messages import BaseMessage, Term, AppendEntriesMessage, LogEntry, Message
from serde.msgpack import from_msgpack, to_msgpack


class TestSerde(unittest.TestCase):
    def test_serde(self):
        message = AppendEntriesMessage(
            "test", "foo", Term(0), entries=[LogEntry(term=Term(0), index=0)]
        )
        message_bytes = to_msgpack(message, cls=Message)
        decoded_message = from_msgpack(
            Message, message_bytes
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
            "341e6380a4b0ff6f7676042fe17e5c8a034c51eb0d77ae87656f7b16a3252fe0",
        )


if __name__ == "__main__":
    unittest.main()
