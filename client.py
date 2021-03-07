"""
simplistic NATS client

https://docs.nats.io/nats-protocol/nats-protocol
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
        return self._messages.get()

    def publish(self, *, subject, payload=""):
        self._sock.send(f"PUB {subject} {len(payload)}\r\n{payload}\r\n".encode("utf-8"))

    def subscribe(self, *, subject):
        self._sid += 1
        self._sock.send(f"SUB {subject} {self._sid}\r\n".encode("utf-8"))

    def request(self, *, subject, payload=""):
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
    from contextlib import suppress

    def publisher():
        with Client(name="publisher") as client:
            while True:
                time.sleep(1)
                client.publish(subject="time.time", payload=f"{time.time()}")

    threading.Thread(target=publisher, daemon=True).start()

    def responder():
        with Client(name="responder") as client:
            client.subscribe(subject="today")
            while True:
                request = client.get()
                client.publish(
                    subject=request.inbox.decode("utf-8"), payload=f"{dt.date.today()}"
                )

    threading.Thread(target=responder, daemon=True).start()

    with suppress(KeyboardInterrupt), Client(name="consumer") as client:
        client.subscribe(subject="time.time")
        requested = False
        while True:
            print(client.get())
            if not requested:
                print(client.request(subject="today"))
                requested = True
