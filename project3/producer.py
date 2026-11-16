"""Earthquakes -> Kafka topic "quakes".

  python quakes_producer.py history 7   # PULL: the last 7 days from the EMSC catalogue
  python quakes_producer.py live        # PUSH: new and updated events from the EMSC WebSocket

Adapted from Professor Magee's stream-seismic.py (learn-kafka).
"""
import asyncio
import json
import logging
import sys
from quixstreams import Application
from tornado.websocket import websocket_connect


BROKER = "127.0.0.1:19092"
TOPIC = "bluesky-posts"
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


def send(producer, message):
    """One Kafka record per message. Key = the event id, so every
    version of the same earthquake goes to the same partition, in order."""
    unid = message["data"].get("did")
    producer.produce(topic=TOPIC, key=unid, value=json.dumps(message))
    return unid



async def live(producer):
    """PUSH over a WebSocket, with a reconnect loop."""
    while True:
        try:
            ws = await websocket_connect(WSS_URL, ping_interval=15)
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
        asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
