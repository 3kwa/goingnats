a Python NATS client
====================

Why?
----

I like [NATS](https://nats.io/) a lot.

`asyncio` less so hence never played with the [official Python client](https://github.com/nats-io/nats.py).

While I got a lot out of [nats-python](https://github.com/Gr1N/nats-python), it has
some rough edges I would rather not have to work around as an end user.

I wanted to teach myself enough `socket` programming and gain a deeper understanding
of the [NATS protocol](https://docs.nats.io/nats-protocol/nats-protocol).

a Python NATS client that fits the way I use NATS.

* Client must have a name
* Client PONGs on PING behind the scene
* Encourage to only use Client as a contextmanager
* non blocking when receiving messages from subscriptions
* blocking on request / response

It is far from feature complete.

It implements the features I use, which may be all you need too.

How?
----

A contrived example illustrating how an `application` would interact with a
`publisher` and a `responder` via NATS.

Fire up NATS on your dev box and `python -m goingnats` to see it run.

```Python
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
            # publish
            publish(subject=b"time.time", payload=b"hijack")
            # request response are blocking
            response = client.request(subject=b"today", payload=b"%Y%m%d")
            print(response)
    print("--- request ---")
    print(request(subject=b"add", payload=b"[1, 2, 3]"))
    try:
        print(request(subject=b"today", wait=100))
    except TimeoutError as e:
        print(e)
```

`one` more thing ... three actually
-----------------------------------

```Python
>>> from goingnats import one
>>> one(subject=b">")
Message(...)
```

`one` is a very handy little helper that waits to receive a message on a given subject and returns it.


```Python
>>> from goingnats import request
>>> request(subject=b"add", payload=b"[1, 2, 3]")
Message(payload=b"6")
```

`request` is another handy helper when developing services running on NATS.

```Python
>>> from goingnats import publish
>>> publish(subject=b"something.important", payload=b"OR_NOR")
```

`publish` is similar to `request` but does not expect a response ... another convenient helper.
