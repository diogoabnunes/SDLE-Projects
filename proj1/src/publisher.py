import zmq
from zeromq_entities import ZeroMqSocketWithBackoff
from message import Message, ReliableMessageFrame
from utils import CobsHandler
from argument_parser import parse_arguments


class Publisher(ZeroMqSocketWithBackoff):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._socket = self._context.socket(zmq.REQ)

    def put(self, topic, content, blocking=True):

        if blocking:
            self.reset_backoff()

        while True:
            msg = CobsHandler.encode([topic, content])
            self._socket.send(ReliableMessageFrame.encode(Message.PUT, msg))
            resp = ReliableMessageFrame.decode(self._socket.recv())

            if resp["type"] == Message.NO_SUBSCRIBER:
                if blocking:
                    self.exponential_backoff()
                    continue

            return resp

    def activate(self):

        i = 0
        while True:
            if i == 10:
                break
            h = self.put("bye", f"cebolas_{i}")
            i += 1


def main():

    args = parse_arguments()

    getter = Publisher()
    print("Publisher created")

    getter.start(args.host, args.port, args.connect_type)
    getter.activate()


if __name__ == "__main__":
    main()
