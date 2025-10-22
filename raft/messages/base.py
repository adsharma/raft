import hashlib
import uuid
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, Optional, NewType, Union

from serde import serde, InternalTagging
from serde.msgpack import to_msgpack

Term = NewType("Term", int)
Peer = Union[int, str, uuid.UUID]  # int used only on tests
HashType = hashlib._hashlib.HASH


@serde(tagging=InternalTagging("_type"))
@dataclass
class BaseMessage:
    sender: Peer
    receiver: Optional[Peer]
    term: int  # TODO: Change to Term
    id: Union[int, uuid.UUID] = 0
    data: int = 0
    timestamp: int = 0
    group: Optional[str] = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

    @property
    def type(self):
        return self._type  # type: ignore

    def __lt__(self, other):
        return self.timestamp < other.timestamp

    @staticmethod
    def default() -> "BaseMessage":
        return BaseMessage(0, 0, Term(0), 0, 0, 0)

    def __post_init__(self):
        if self.id == 0:
            self.id = uuid.uuid4()

    def hash(self) -> 'HashType':
        return hashlib.sha256(to_msgpack(self))
