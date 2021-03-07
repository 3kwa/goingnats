"""
a Python NATS client

>>> from goingnats import Client

Check if __name__ == "__main__" for full example
"""

import queue
import socket
import threading
import uuid
from collections import namedtuple


class Client:
    def __init__(self, *, name, host="0.0.0.0", port=4222):
        self.host = host
        self.port = port
        self.name = name
        self._buffer = b""
        self._messages = queue.Queue()
        self._response = queue.Queue(maxsize=1)
        self._sid = 0
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def get(self):
        """returns list of messages received since get was last called"""
        return [self._messages.get_nowait() for _ in range(self._messages.qsize())]

    def publish(self, *, subject, payload=""):
        """publish payload on subject"""
        self._sock.send(
            f"PUB {subject} {len(payload)}\r\n{payload}\r\n".encode("utf-8")
        )

    def subscribe(self, *, subject):
        """subscribe to subject"""
        self._sid += 1
        self._sock.send(f"SUB {subject} {self._sid}\r\n".encode("utf-8"))

    def request(self, *, subject, payload=""):
        """request subject for a response to payload"""
        inbox = f"INBOX.{uuid.uuid4().hex}"
        self._sid += 1
        self._sock.send(f"SUB {inbox} {self._sid}\r\n".encode("utf-8"))
        self._sock.send(f"UNSUB {self._sid} 1\r\n".encode("utf-8"))
        self._sock.send(
            f"PUB {subject} {inbox} {len(payload)}\r\n{payload}\r\n".encode("utf-8")
        )
        return self._response.get()

    def _connect(self):
        self._sock.connect((self.host, self.port))
        threading.Thread(target=self._thread, daemon=True).start()

    def _disconnect(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    def __enter__(self):
        self._connect()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self._disconnect()

    def _thread(self):
        # https://docs.nats.io/nats-protocol/nats-protocol
        while True:
            received = self._sock.recv(4096)
            current, eom, next = received.partition(b"\r\n")
            self._buffer += current
            # End Of Message
            if eom:
                if self._buffer == b"PING":
                    self._sock.send(b"PONG\r\n")
                elif self._buffer == b"PONG":
                    pass
                elif self._buffer == b"+OK":
                    pass
                elif self._buffer.startswith(b"-ERR"):
                    print(self._buffer)
                elif self._buffer.startswith(b"INFO"):
                    self._sock.send(
                        f'CONNECT {{"name": "{self.name}", "verbose": false}}\r\n'.encode(
                            "utf-8"
                        )
                    )
                elif self._buffer.startswith(b"MSG"):
                    # MSG ...  \r\n[payload]\r\n
                    split = self._buffer.split(b" ")
                    subject = split[1]
                    # is it a request?
                    inbox = False
                    if len(split) == 5:
                        inbox = split[3]
                    # End Of Payload
                    payload, eop, _ = next.partition(b"\r\n")
                    if eop:
                        # request
                        if inbox:
                            self._messages.put(Request(subject, inbox, payload))
                        # response
                        elif subject.startswith(b"INBOX."):
                            self._response.put(Response(payload))
                        # vanilla message
                        else:
                            self._messages.put(Message(subject, payload))
                        next = b""
                else:
                    # payload over multiple recv calls
                    if inbox:
                        self._messages.put(Request(subject, inbox, payload))
                    elif subject.startswith(b"INBOX."):
                        self._response.put(Response(payload))
                    else:
                        self._messages.put(Message(subject, self._buffer))
            self._buffer = next


Message = namedtuple("Message", "subject payload")
Request = namedtuple("Request", "subject inbox payload")
Response = namedtuple("Response", "payload")

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
                    # slow responder
                    time.sleep(2)
                    # will format the date according to payload or defaults to ...
                    format = (
                        request.payload.decode("utf-8")
                        if request.payload
                        else "%Y-%m-%d"
                    )
                    client.publish(
                        subject=request.inbox.decode("utf-8"),
                        payload=f"{dt.date.today():{format}}",
                    )

    threading.Thread(target=responder, daemon=True).start()

    with Client(name="consumer") as client:
        client.subscribe(subject="time.time")
        received = 0
        response = None
        while received < 5:
            for message in client.get():
                print(message)
                received += 1
            if received == 3 and response is None:
                # request response are blocking
                response = client.request(subject="today", payload="%Y%m%d")
                print(response)
