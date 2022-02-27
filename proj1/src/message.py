from collections.abc import Iterable
from enum import Enum
import math
import time
from utils import CobsHandler, bytes_to_int


class Message(Enum):
    SUBSCRIBE = 1
    UNSUBSCRIBE = 2
    GET = 3
    PUT = 4
    ACK = 5
    INVALID = 6
    NO_MESSAGES = 7
    NOT_SUBSCRIBED = 8
    ALREADY_SUBSCRIBED = 9
    NO_SUBSCRIBER = 10


class RouterMessage:
    def __init__(self, identifier, empty, content):

        self._identifier = identifier
        self._empty = empty
        self.content = content

    def response(self, content):
        return [self._identifier, self._empty, content]

    @classmethod
    def from_multipart(cls, msg):

        if not isinstance(msg, list):
            raise ValueError("Ooof")
        if not all(map(lambda x: isinstance(x, bytes), msg)):
            raise ValueError("Yikes")

        if len(msg[1]) != 0:
            raise ValueError("Middle frame should be empty")

        return cls(msg[0], msg[1], msg[2])


class ReliableMessageFrame:

    decoder = CobsHandler("type", "content")

    @staticmethod
    def decode(msg):

        x = msg
        if isinstance(msg, RouterMessage):
            x = msg.content
        m = ReliableMessageFrame.decoder.get_components(x)
        m["type"] = Message(bytes_to_int(m["type"]))
        return m

    @staticmethod
    def encode(typ, content=b""):
        return CobsHandler.encode([typ.value, content])


class GetMessage:

    decoder = CobsHandler("num_msgs", "msgs")

    @staticmethod
    def encode(content):

        if not isinstance(content, Iterable):
            raise ValueError("....")
        ret = CobsHandler.encode([len(content), CobsHandler.encode(content)])
        return ret

    @staticmethod
    def decode(msg):
        components = GetMessage.decoder.get_components(msg)

        msg_headers = [f"msg_{i}" for i in range(bytes_to_int(components["num_msgs"]))]

        content_decoder = CobsHandler(*msg_headers)

        inner_components = content_decoder.get_components(components["msgs"])
        return list(inner_components.values())
