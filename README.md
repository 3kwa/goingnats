a Python NATS client
====================

Why?
----

I like [NATS](https://nats.io/) a lot.

`asyncio` less so hence never played with the [official Python client](https://github.com/nats-io/nats.py).

While I got a lot out of [nats-python](https://github.com/Gr1N/nats-python), it has
some rought edges I would rather not have to work around as an end user.

I wanted to teach myself enough `socket` programming and gain a deeper understanding
of the [NATS protocol](https://docs.nats.io/nats-protocol/nats-protocol).

a Python NATS client that fits the way I use NATS.

* Client must have a name
* Client PONGs on PING behind the scene
* Encourage to only use Client as a contextmanager
* non blocking when receiving messages from subscriptions
* blocking on request / response

It is far from feature complete.

It implements the features I use, which maybe all you need too.

How?
----

A contrived example illustrating how an `application` would interact with a
`publisher` and a `responder` via NATS.

Fire up NATS on your dev box and `python -m goingnats` to see it run.

```Python
import datetime as dt
import time
import threading

from goingnats import Client


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

# application
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
```

One more thing
--------------

```Python
>>> from goingnats import one
>>> one(">")
Message(...)
```

`one` is a very handy little helper that waits to receive a message on a given subject and returns it.


