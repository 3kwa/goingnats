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

How?
----

```Python
    import datetime as dt
    import time
    from contextlib import suppress

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

    with suppress(KeyboardInterrupt), Client(name="consumer") as client:
        client.subscribe(subject="time.time")
        received = 0
        response = None
        while received < 5:
            for message in client.get():
                print(message)
                received += 1
            if received == 3 and response is None:
                response = client.request(subject="today", payload="%Y%m%d")
                print(response)
```
