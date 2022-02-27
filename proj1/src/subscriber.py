import zmq

from zeromq_entities import ZeroMqSocketWithBackoff
from message import Message, ReliableMessageFrame, GetMessage
from utils import CobsHandler
from argument_parser import parse_arguments


class Subscriber(ZeroMqSocketWithBackoff):
    def __init__(self, identifier, **kwargs):
        super().__init__(**kwargs)
        self._socket = self._context.socket(zmq.REQ)
        self.identifier = identifier

    def unsubscribe(self, topic):
        msg = CobsHandler.encode([self.identifier, topic])
        self._socket.send(ReliableMessageFrame.encode(Message.UNSUBSCRIBE, msg))
        resp = ReliableMessageFrame.decode(self._socket.recv())

        if resp["type"] == Message.ACK or resp["type"] == Message.NOT_SUBSCRIBED:
            return True
        return False

    def subscribe(self, topic):
        msg = CobsHandler.encode([self.identifier, topic])
        self._socket.send(ReliableMessageFrame.encode(Message.SUBSCRIBE, msg))
        resp = ReliableMessageFrame.decode(self._socket.recv())

        if resp["type"] == Message.ACK or resp["type"] == Message.ALREADY_SUBSCRIBED:
            return True
        return False

    def get(self, topic, num_msgs=5, blocking=True):

        if blocking:
            self.reset_backoff()

        while True:
            msg = CobsHandler.encode([self.identifier, topic, num_msgs])
            self._socket.send(ReliableMessageFrame.encode(Message.GET, msg))
            resp = ReliableMessageFrame.decode(self._socket.recv())

            if resp["type"] == Message.NO_MESSAGES:
                if blocking:
                    self.exponential_backoff()
                    continue

            if resp["type"] != Message.ACK:
                return None

            return GetMessage.decode(resp["content"])

    def activate(self):

        self.subscribe("bye")

        while True:
            h = self.get("bye")
            print(h)
            break
        # self.unsubscribe('bye')


def main():

    args = parse_arguments({}, [("--id", str, True, None, None)])

    getter = Subscriber(args.id)
    print("Subscriber created")

    getter.start(args.host, args.port, args.connect_type)
    getter.activate()


if __name__ == "__main__":
    main()
