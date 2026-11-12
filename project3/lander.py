"""Kafka topic "quakes" -> raw Parquet files in data/raw/.

A batch is written when it has 1000 records or is 60 seconds old.
Offsets are stored only AFTER the file exists: at-least-once.

  python quakes_lander.py
  python quakes_lander.py crash   # experiment: crash after the first file, before storing offsets
"""
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import duckdb
from quixstreams import Application

BROKER = "127.0.0.1:19092"
TOPIC = "bluesky-posts"
OUT_DIR = "data/raw"
BATCH_SIZE = 1000
BATCH_SECONDS = 60
CRASH_AFTER_WRITE = "crash" in sys.argv[1:]

SCHEMA = """kafka_partition INTEGER, kafka_offset BIGINT, kafka_key VARCHAR,
            action VARCHAR, kind VARCHAR, operation VARCHAR, did VARCHAR, rkey VARCHAR,
            time_us BIGINT, lang VARCHAR, text VARCHAR,
            landed_at TIMESTAMP, raw VARCHAR"""


def number(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def to_row(msg):
    """Keep the whole message (raw) plus a few columns. Never crash on bad input."""
    raw = msg.value().decode("utf8", errors="replace") if msg.value() else ""
    try:
        m = json.loads(raw)
        d = m["data"]
        action = m.get("action", "unknown")
        c = d.get("commit") or {} 
        r = c.get("record") or {}   
    except (ValueError, KeyError, TypeError, AttributeError):
        d, c, r, action = {}, {}, {}, "malformed"
    langs = r.get("langs") or [None]
    key = msg.key().decode("utf8", errors="replace") if msg.key() else None
    return (msg.partition(), msg.offset(), key, action, d.get("kind"),
            c.get("operation"), d.get("did"), c.get("rkey"), d.get("time_us"),
            langs[0], r.get("text"),
            datetime.now(timezone.utc).replace(tzinfo=None), raw)


def write_batch(rows):
    """Write one Parquet file. Write to .tmp first, then rename,
    so the flow never reads a half-written file."""
    os.makedirs(OUT_DIR, exist_ok=True)
    name = f"{OUT_DIR}/bluesky_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}.parquet"
    con = duckdb.connect()  # in memory: no database file, no lock
    con.execute(f"CREATE TABLE batch ({SCHEMA})")
    con.executemany(f"INSERT INTO batch VALUES ({', '.join(['?'] * len(rows[0]))})", rows)
    con.execute(f"COPY batch TO '{name}.tmp' (FORMAT parquet)")
    con.close()
    os.replace(f"{name}.tmp", name)
    return name


def main():
    app = Application(broker_address=BROKER, consumer_group="bluesky_posts_lander",
                      auto_offset_reset="earliest", loglevel="WARNING")
    with app.get_consumer() as consumer:
        consumer.subscribe([TOPIC])
        rows, last, started = [], {}, time.monotonic()
        print("Landing records from", TOPIC, "into", OUT_DIR, "(Ctrl+C to stop)")
        while True:
            msg = consumer.poll(1)
            if msg is not None:
                if msg.error() is not None:
                    raise Exception(msg.error())
                if not rows:
                    started = time.monotonic()
                rows.append(to_row(msg))
                last[msg.partition()] = msg  # newest message per partition
            full = len(rows) >= BATCH_SIZE
            old = rows and time.monotonic() - started >= BATCH_SECONDS
            if full or old:
                path = write_batch(rows)
                print(f"Landed {len(rows):4d} records -> {path}")
                if CRASH_AFTER_WRITE:
                    print("CRASH (on purpose): the file exists, the offsets were NOT stored")
                    os._exit(1)
                for m in last.values():
                    consumer.store_offsets(message=m)  # only now: "these are done"
                rows, last = [], {}


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass  # records not landed yet were not stored, so they are read again next time
