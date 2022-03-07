"""a Python NATS client

Checkout `if __name__ == "__main__"` block for usage

$ python -m goingnats
--- one ---
Message(subject=b'time.time', payload=b'1646389270.6568809')
--- client.subscribe + client.request ---
Message(subject=b'time.time', payload=b'1646389271.6775382')
Message(subject=b'time.time', payload=b'1646389272.690817')
Message(subject=b'time.time', payload=b'1646389273.70843')
Response(payload=b'20220304')
Message(subject=b'time.time', payload=b'1646389274.724187')
Message(subject=b'time.time', payload=b'1646389275.733218')
--- request ---
Response(payload=b'6')
no respponse received from b'today' in 100 ms
"""

__version__ = "2022.3.3"

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
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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

    def request(self, *, subject, payload=b"", wait=None):
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
        try:
            return self._response.get(timeout=wait / 1_000 if wait else None)
        except queue.Empty:
            raise TimeoutError(f"no respponse received from {subject} in {wait} ms")

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
        messages = Messages()
        while self._run:
            try:
                received = self._sock.recv(4096)
            except ConnectionResetError as e:
                raise ConnectionResetError("recv failed") from e
            except OSError as e:
                if self._run:
                    raise OSError("recv failed") from e
            for message in messages(received):
                if message == b"PING":
                    self._send([b"PONG"])
                elif message == b"PONG":
                    pass
                elif message == b"+OK":
                    pass
                elif message.startswith(b"-ERR"):
                    warnings.warn(message.decode())
                elif message.startswith(b"INFO"):
                    self.information = json.loads(message[4:])
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
                elif message.startswith(b"MSG"):
                    # MSG ...  \r\n[payload]\r\n
                    split = message.split(b" ")
                    subject = split[1]
                    # is it a request?
                    inbox = False
                    if len(split) >= 5:
                        inbox = split[3]
                else:
                    # should be e a payload
                    # request
                    if inbox:
                        self._messages.put(Request(subject, inbox, message))
                    # response
                    elif subject.startswith(b"INBOX."):
                        self._response.put(Response(message))
                    # vanilla message
                    else:
                        self._messages.put(Message(subject, message))


class Messages:
    def __init__(self, separator=CRLF):
        self.separator = separator
        self._buffer = b""

    def __call__(self, received):
        self._buffer = b"".join([self._buffer, received])
        return self

    def __iter__(self):
        messages = self._buffer.split(self.separator)
        try:
            last = self._buffer[-1]
        except IndexError:
            return
        if last != self.separator:
            self._buffer = messages[-1]
            messages = messages[:-1]
        for message in messages:
            yield (message)


Message = namedtuple("Message", "subject payload")
Request = namedtuple("Request", "subject inbox payload")
Response = namedtuple("Response", "payload")


def one(*, subject, host="127.0.0.1", port=4222, name="one"):
    """returns one (the first) message received on a subject"""
    with Client(host=host, port=port, name=name) as client:
        client.subscribe(subject=subject)
        while True:
            for message in client.get():
                return message


def request(*, subject, payload=b"", wait=None, host="127.0.0.1", port=4222):
    with Client(host=host, port=port, name=f"request.{subject.decode()}") as client:
        return client.request(subject=subject, payload=payload, wait=wait)


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
            client.subscribe(subject=b"add")
            while True:
                for request in client.get():
                    if request.subject == b"today":
                        # slow responder
                        time.sleep(2)
                        # will format the date according to payload or defaults to ...
                        format = request.payload.decode() if request.payload else "%Y-%m-%d"
                        response = f"{dt.date.today():{format}}".encode()
                    elif request.subject == b"add":
                        response = _int_to_bytes(sum(json.loads(request.payload)))
                    else:
                        continue
                    client.publish(
                        subject=request.inbox,
                        payload=response,
                    )

    threading.Thread(target=responder, daemon=True).start()

    # application
    with Client(name="consumer") as client:
        print("--- one ---")
        print(one(subject=b"time.time"))
        print("--- client.subscribe + client.request ---")
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
        print("--- request ---")
        print(request(subject=b"add", payload=b"[1, 2, 3]"))
        try:
            print(request(subject=b"today", wait=100))
        except TimeoutError as e:
            print(e)
