"""
a Python NATS client

>>> from goingnats import Client, one

Check if __name__ == "__main__" for full example
"""
__version__ = "2021.3.7"

import json
import queue
import socket
import threading
import uuid
import warnings
from collections import namedtuple
from time import monotonic


class Client:
    def __init__(self, *, name, host="127.0.0.1", port=4222):
        self.host = host
        self.port = port
        self.name = name
        self.information = {}
        # get relies on _messages being queue.Queue to handle wait
        self._messages = queue.Queue()
        self._response = queue.Queue(maxsize=1)
        self._sid = 0
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._run = False

    def get(self, *, wait=None):
        """returns list of messages received since get was last called

        waits for `wait` milliseconds
        """
        result = self._get()
        if result:
            return result
        elif wait is None:
            return result
        elif wait < 0:
            raise ValueError("wait must be a non-negative number")
        else:
            # check the implementation of queue.Queue.get (_messages)
            # read about threading.Condition.wait (not_empty)
            with self._messages.not_empty:
                self._messages.not_empty.wait(wait / 1_000)
            return self._get()

    def _get(self):
        return [self._messages.get_nowait() for _ in range(self._messages.qsize())]

    def publish(self, *, subject, payload=""):
        """publish payload on subject"""
        self._send(f"PUB {subject} {len(payload)}\r\n{payload}")

    def subscribe(self, *, subject):
        """subscribe to subject"""
        self._sid += 1
        self._send(f"SUB {subject} {self._sid}")

    def request(self, *, subject, payload=""):
        """request subject for a response to payload"""
        inbox = f"INBOX.{uuid.uuid4().hex}"
        self._sid += 1
        self._send(f"SUB {inbox} {self._sid}")
        self._send(f"UNSUB {self._sid} 1")
        self._send(f"PUB {subject} {inbox} {len(payload)}\r\n{payload}")
        return self._response.get()

    def __enter__(self):
        try:
            self._sock.connect((self.host, self.port))
        except ConnectionRefusedError as e:
            raise ConnectionRefusedError(
                f"can't connect to {self.host}:{self.port}"
            ) from e
        self._run = True
        threading.Thread(target=self._thread).start()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self._run = False
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    def _send(self, string):
        try:
            self._sock.sendall(f"{string}\r\n".encode("utf-8"))
        except BrokenPipeError as e:
            raise BrokenPipeError("send failed") from e
        except OSError as e:
            # socket
            if self._run:
                raise OSError("send failed") from e

    def _thread(self):
        # https://docs.nats.io/nats-protocol/nats-protocol
        buffer_ = b""
        while self._run:
            try:
                received = self._sock.recv(4096)
            except ConnectionResetError as e:
                raise ConnectionResetError("recv failed") from e
            except OSError as e:
                if self._run:
                    raise OSError("recv failed") from e

            segments = received.split(b"\r\n")
            if len(segments) > 1:
                # buffer contains tail of previous received \r\ntail
                segments[0] = buffer_ + segments[0]
                # will be b"" if received end with \r\n
                buffer_ = segments[-1]
            else:
                # no \r\n received nothing to do but buffer
                buffer_ += received
                continue

            for segment in segments[:-1]:
                if segment == b"PING":
                    self._send("PONG")
                elif segment == b"PONG":
                    pass
                elif segment == b"+OK":
                    pass
                elif segment.startswith(b"-ERR"):
                    warnings.warn(segment.decode("utf-8"))
                elif segment.startswith(b"INFO"):
                    self.information = json.loads(segment[4:])
                    information = {
                        "name": self.name,
                        "verbose": False,
                        "version": f"goingnats-{__version__}",
                    }
                    self._send(f"CONNECT {json.dumps(information)}")
                elif segment.startswith(b"MSG"):
                    # MSG ...  \r\n[payload]\r\n
                    split = segment.split(b" ")
                    subject = split[1]
                    # is it a request?
                    inbox = False
                    if len(split) >= 5:
                        inbox = split[3]
                else:
                    # should be e a payload
                    # request
                    if inbox:
                        self._messages.put(
                            Request(
                                subject.decode("utf-8"), inbox.decode("utf-8"), segment
                            )
                        )
                    # response
                    elif subject.startswith(b"INBOX."):
                        self._response.put(Response(segment))
                    # vanilla message
                    else:
                        self._messages.put(Message(subject.decode("utf-8"), segment))


Message = namedtuple("Message", "subject payload")
Request = namedtuple("Request", "subject inbox payload")
Response = namedtuple("Response", "payload")


def one(*, subject, host="0.0.0.0", port=4222, name="one"):
    """returns one (the first) message received on a subject"""
    with Client(host=host, port=port, name=name) as client:
        client.subscribe(subject=subject)
        while True:
            for message in client.get():
                return message


if __name__ == "__main__":
    import datetime as dt
    import time

    def publisher():
        """publish time.time() every second"""
        with Client(name="publisher") as client:
            while True:
                time.sleep(1)
                client.publish(subject="time.time", payload=f"{time.time()}")

    threading.Thread(target=publisher, daemon=True).start()

    def responder():
        """respond to request for today with the date"""
        with Client(name="responder") as client:
            client.subscribe(subject="today")
            while True:
                for request in client.get():
                    if request.subject != "today":
                        continue
                    # slow responder
                    time.sleep(2)
                    # will format the date according to payload or defaults to ...
                    format = (
                        request.payload.decode("utf-8")
                        if request.payload
                        else "%Y-%m-%d"
                    )
                    client.publish(
                        subject=request.inbox,
                        payload=f"{dt.date.today():{format}}",
                    )

    threading.Thread(target=responder, daemon=True).start()

    # application
    with Client(name="consumer") as client:
        client.subscribe(subject="time.time")
        received = 0
        response = None
        while received < 5:
            # waits for at most 10 ms for messages
            for message in client.get(wait=10):
                print(message)
                received += 1
            if received == 3 and response is None:
                # request response are blocking
                response = client.request(subject="today", payload="%Y%m%d")
                print(response)
