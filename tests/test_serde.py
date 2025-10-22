#!/usr/bin/env python3

import unittest

from serde.msgpack import from_msgpack, to_msgpack

from raft.messages import AppendEntriesMessage, LogEntry, Message, Term


class TestSerde(unittest.TestCase):
    def test_serde(self):
        message = AppendEntriesMessage(
            "test", "foo", Term(0), entries=[LogEntry(term=Term(0), index=0)]
        )
        message_bytes = to_msgpack(message, cls=Message)
        decoded_message = from_msgpack(Message, message_bytes)
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
            "5f9dbf4bdd49da62f2c8f3c9181ad4abdc9bdbadbfcd52463ff8c89258be8fe6",
        )


if __name__ == "__main__":
    unittest.main()
