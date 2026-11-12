"""Earthquakes -> Kafka topic "quakes".

  python quakes_producer.py history 7   # PULL: the last 7 days from the EMSC catalogue
  python quakes_producer.py live        # PUSH: new and updated events from the EMSC WebSocket

Adapted from Professor Magee's stream-seismic.py (learn-kafka).
"""
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky-posts"
# FDSN_URL = "https://www.seismicportal.eu/fdsnws/event/1/query"
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


def send(producer, message):
    """One Kafka record per message. Key = the event id, so every
    version of the same earthquake goes to the same partition, in order."""
    unid = message["data"].get("did")
    producer.produce(topic=TOPIC, key=unid, value=json.dumps(message))
    return unid


def history(producer, days):
    """PULL: ask the catalogue for one day at a time (far below the 20,000 limit)."""
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    total = 0
    for back in range(days - 1, -1, -1):  # the last N days, today included
        start = today - timedelta(days=back)
        end = start + timedelta(days=1)
        r = requests.get(
            FDSN_URL,
            params={
                "start": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "end": end.strftime("%Y-%m-%dT%H:%M:%S"),
                "format": "json",
                "limit": 20000,
                "orderby": "time-asc",
            },
            timeout=60,
        )
        if r.status_code == 204:  # no events that day
            continue
        r.raise_for_status()
        features = r.json()["features"]
        for feature in features:
            send(producer, {"action": "history", "data": feature})
        total += len(features)
        logging.info("%s  %4d events   total %d", start.date(), len(features), total)
        time.sleep(1)  # be polite to a free public service
    logging.info("History done: %d events sent to %s", total, TOPIC)




WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
SUBSCRIBE = {"type": "subscribe",
             "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD",
                             "XRP-USD", "DOGE-USD", "ADA-USD"],
             "channels": ["NONE"]} 

async def live(producer):
    """PUSH over a WebSocket, with a reconnect loop."""
    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
            if SUBSCRIBE: 
                await ws.write_message(json.dumps(SUBSCRIBE))
            while True:
                text = await ws.read_message()
                if text is None:
                    break  # the server closed the connection
                try:
                    send(producer, {"action": "live", "data": json.loads(text)})
                except (ValueError, KeyError, TypeError) as e:
                    logging.warning("Skipped a message: %r", e)
        except Exception as e:
            logging.warning("Connection failed: %r", e)
        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "live"
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        if mode == "history":
            history(producer, int(sys.argv[2]) if len(sys.argv) > 2 else 7)
        else:
            asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
