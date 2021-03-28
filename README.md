raft
====

This is an implementation of raft forked from [SimpleRaft](https://github.com/streed/simpleRaft).

Why?
====

Python is an easy to learn language, only marginally more complex than pseudo code, but something you
can write unit tests against and try new ideas. Python3 also supports type annotations, which can 
improve readability.

How?
====

This implementation has a few layers. You can navigate the code base as follows:

* [raft/messages](https://github.com/adsharma/raft/raft/messages) Raft message types
* [raft/state](https://github.com/adsharma/raft/tree/master/raft/states) Actual logic. Should be a superset
  of what you see in the one pager on the raft paper. It's a bit closer to the practice in the theory <-> practice
  spectrum than the pseudo code in the raft paper. Still fits in about 500 lines of code.
* [raft/servers](https://github.com/adsharma/raft/tree/master/raft/servers) A server is the networking infra that
  is needed to make the logic above useful. There are two implementations:
  * ZeromqServer: Single process, multiple thread implementation suitable for unit testing. Uses PUB/SUB sockets.
  * ZREServer: Multi-process implementation suitable for a simple chat client for example. Uses [ZRE](https://rfc.zeromq.org/spec/20/)
* [raft/board](https://github.com/adsharma/raft/tree/master/raft/boards) This is where the state `(x <- 3, y <- 1)` in the raft paper
  is stored. `db_board.py` is probably the most useful one.

Details
=======

Unlike simpleRaft, this code uses python3's `async/await` to replace the threading
logic that existed before and caused unit tests to hang in the presence of many
threads.

Async RPC is implemented on top of zeromq sockets using [pyserde](https://github.com/yukinarit/pyserde).
Each message has a `UUID`. Responses from raft quorum participants contain the `UUID` of the message being
responded to. The server maintains a cache of recently sent out messages. So it's able to infer which message
is being responded to in the presence of out of order, pipelined delivery of RPCs (as opposed to traditional
request/response pattern on TCP sockets in traditional RPC implementations).

Testing
=======

```
$ apt install python3-pip python3-pytest
$ pip3 install -r requirements.txt --user
$ alias t=pytest-3
$ t
============================= test session starts =============================
platform linux -- Python 3.9.1, pytest-6.2.2, py-1.10.0, pluggy-0.13.1
rootdir: /home/foo/raft
collected 25 items

tests/test_CandidateServer.py .......                                   [ 28%]
tests/test_FollowerServer.py ........                                   [ 60%]
tests/test_LeaderServer.py .....                                        [ 80%]
tests/test_MemoryBoard.py ..                                            [ 88%]
tests/test_raft.py ...                                                  [100%]

============================= 25 passed in 1.96s ==============================
```

References:
==========
* [In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)
* [Raft Lecture](http://www.youtube.com/watch?v=YbZ3zDzDnrw)
