"""a Python NATS client"""

__version__ = "2022.3.0"

import json
import queue
import socket
import threading
import uuid
import warnings
from collections import namedtuple

SPACE = b" "
CRLF = b"\r\n"


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

        waits for messages for at most `wait` milliseconds (or does not)
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

    def publish(self, *, subject, payload=b""):
        """publish payload on subject"""
        try:
            self._send(
                [
                    b"PUB",
                    SPACE,
                    subject,
                    SPACE,
                    _int_to_bytes(len(payload)),
                    CRLF,
                    payload,
                ]
            )
        except TypeError:
            raise TypeError("subject and payload must be bytes-like") from None

    def subscribe(self, *, subject):
        """subscribe to subject"""
        self._sid += 1
        try:
            self._send([b"SUB", SPACE, subject, SPACE, _int_to_bytes(self._sid)])
        except TypeError:
            raise TypeError("subject must be bytes-like") from None

    def request(self, *, subject, payload=b""):
        """request subject for a response to payload"""
        inbox = f"INBOX.{uuid.uuid4().hex}".encode()
        self._sid += 1
        self._send([b"SUB", SPACE, inbox, SPACE, _int_to_bytes(self._sid)])
        self._send([b"UNSUB", SPACE, _int_to_bytes(self._sid), SPACE, b"1"])
        try:
            self._send(
                [
                    b"PUB",
                    SPACE,
                    subject,
                    SPACE,
                    inbox,
                    SPACE,
                    _int_to_bytes(len(payload)),
                    CRLF,
                    payload,
                ]
            )
        except TypeError:
            raise TypeError("subject and payload must be bytes-like") from None
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

    def _send(self, payload):
        payload.append(CRLF)
        try:
            self._sock.sendall(b"".join(payload))
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
                    self._send([b"PONG"])
                elif segment == b"PONG":
                    pass
                elif segment == b"+OK":
                    pass
                elif segment.startswith(b"-ERR"):
                    warnings.warn(segment.decode())
                elif segment.startswith(b"INFO"):
                    self.information = json.loads(segment[4:])
                    information = {
                        "name": self.name,
                        "verbose": False,
                        "version": f"goingnats-{__version__}",
                    }
                    self._send(
                        [
                            b"CONNECT",
                            SPACE,
                            json.dumps(information).encode(),
                        ]
                    )
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
                        self._messages.put(Request(subject, inbox, segment))
                    # response
                    elif subject.startswith(b"INBOX."):
                        self._response.put(Response(segment))
                    # vanilla message
                    else:
                        self._messages.put(Message(subject, segment))


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


def _int_to_bytes(i):
    return f"{i}".encode()


if __name__ == "__main__":
    import datetime as dt
    import time

    def publisher():
        """publish time.time() every second"""
        with Client(name="publisher") as client:
            while True:
                time.sleep(1)
                client.publish(subject=b"time.time", payload=f"{time.time()}".encode())

    threading.Thread(target=publisher, daemon=True).start()

    def responder():
        """respond to request for today with the date"""
        with Client(name="responder") as client:
            client.subscribe(subject=b"today")
            while True:
                for request in client.get():
                    if request.subject != b"today":
                        continue
                    # slow responder
                    time.sleep(2)
                    # will format the date according to payload or defaults to ...
                    format = request.payload.decode() if request.payload else "%Y-%m-%d"
                    client.publish(
                        subject=request.inbox,
                        payload=f"{dt.date.today():{format}}".encode(),
                    )

    threading.Thread(target=responder, daemon=True).start()

    # application
    with Client(name="consumer") as client:
        client.subscribe(subject=b"time.time")
        received = 0
        response = None
        while received < 5:
            # waits for at most 10 ms for messages
            for message in client.get(wait=10):
                print(message)
                received += 1
            if received == 3 and response is None:
                # request response are blocking
                response = client.request(subject=b"today", payload=b"%Y%m%d")
                print(response)
