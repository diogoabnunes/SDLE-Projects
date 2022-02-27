import zmq
import math
import time


class GenericZeroMqEntity:
    def __init__(self):
        self._context = zmq.Context.instance()
        self._socket = None

    def start(self, host, port, connect_type, socket=None):
        function = None
        socket = self._socket if socket is None else socket
        if connect_type == "connect":
            function = socket.connect
        elif connect_type == "bind":
            function = socket.bind

        if function is None:
            raise ValueError(f"No function defined for {connect_type}")

        return function(f"tcp://{host}:{port}")


class ZeroMqSocketWithBackoff(GenericZeroMqEntity):
    def __init__(self, max_exponential_backoff=60, exponential_backoff_base=2):
        super().__init__()
        self.backoff_counter = 1
        self.max_exponential_backoff = max_exponential_backoff
        self.exponential_backoff_base = exponential_backoff_base
        self.max_power = round(
            math.log(self.max_exponential_backoff, self.exponential_backoff_base)
        )

    def reset_backoff(self):
        self.backoff_counter = 1

    def exponential_backoff(self):
        time.sleep(
            min(
                self.exponential_backoff_base ** self.backoff_counter,
                self.max_exponential_backoff,
            )
        )
        self.backoff_counter = min(self.backoff_counter + 1, self.max_power)
