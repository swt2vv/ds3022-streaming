"""Prefect flow: raw Parquet -> DuckDB (two kinds of duplicates removed) -> one chart.

  python quakes_flow.py
"""
import os
import sys

import duckdb
import matplotlib

matplotlib.use("Agg")  # draw to a file, no window
import matplotlib.pyplot as plt
from prefect import flow, get_run_logger, task

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
RAW = SRC.rstrip("/\\") + "/*.parquet"
NAME = os.path.basename(SRC.rstrip("/\\"))
TAG = "" if NAME == "raw" else "_" + NAME
DB = f"bluesky{TAG}.duckdb" 
CHART = f"charts/posts_by_hour{TAG}.png" 

@task(retries=2, retry_delay_seconds=5)
def load_raw():
    with duckdb.connect(DB) as con:  # the flow is the ONLY writer of quakes.duckdb
        con.execute(f"CREATE OR REPLACE TABLE raw_posts AS SELECT * FROM read_parquet('{RAW}')")
        return con.execute("SELECT count(*) FROM raw_posts").fetchone()[0]


@task
def stage():
    """Kind 1: the same Kafka record landed twice. DISTINCT keeps one copy."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE stg_records AS
            SELECT DISTINCT * EXCLUDE (landed_at)
            FROM raw_posts
            WHERE action <> 'malformed'
            AND kind = 'commit'
            AND did IS NOT NULL AND rkey IS NOT NULL
        """)
        q = lambda sql: con.execute(sql).fetchone()[0]
        return {"malformed": q("SELECT count(*) FROM raw_posts WHERE action = 'malformed'"),
                "records": q("SELECT count(*) FROM stg_records"),
                "posts": q("SELECT count(*) FROM stg_records")}


@task
def entities():
    """Kind 2: one row per post (did, rkey), with its latest operation."""
    with duckdb.connect(DB) as con:
        con.execute("""
            CREATE OR REPLACE TABLE posts AS
            SELECT did, rkey, operation, lang, text,
                make_timestamp(time_us) AS event_time, -- microseconds -> UTC
                (raw::JSON ->> '$.data.commit.record.reply.parent.uri')
                    IS NOT NULL AS is_reply -- parsed from raw
            FROM stg_records
            QUALIFY row_number() OVER (PARTITION BY did, rkey
                                       ORDER BY time_us DESC, kafka_offset DESC) = 1
        """)
        return {"posts": con.execute("SELECT count(*) FROM posts").fetchone()[0]}


@task
def mart():
    with duckdb.connect(DB) as con:
        con.execute("""CREATE OR REPLACE TABLE mart_hourly AS
            SELECT time_bucket(INTERVAL '1 hour', event_time) AS hour,
            lang, count(*) AS posts
            FROM posts
            GROUP BY hour, lang
            ORDER BY hour, lang""")


@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        rows = con.execute("""
            SELECT hour, lang, posts
            FROM mart_hourly
            WHERE lang IN (
                SELECT lang FROM mart_hourly
                GROUP BY lang ORDER BY sum(posts) DESC LIMIT 5)
            ORDER BY lang, hour
        """).fetchall()
    
    by_lang = {}
    for hour_of_day, lang, posts in rows:
        by_lang.setdefault(lang, {})[hour_of_day] = posts

    os.makedirs("charts", exist_ok=True)
    days = [r[0].strftime("%d %b") for r in rows]
    plt.figure(figsize=(8, 4))
    plt.bar(days, [r[1] for r in rows], color="#2a7fb8")
    plt.title("Posts per Hour by Language")
    plt.xlabel("Hour (UTC)")
    plt.ylabel("Posts")
    plt.legend()
    plt.tight_layout()
    plt.savefig(CHART, dpi=120)
    return CHART


@flow(name="quakes-pipeline")
def quakes_pipeline():
    log = get_run_logger()
    raw = load_raw()
    stage_result = stage()
    entity_results = entities()
    mart_results = mart()
    path = chart()
    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", stage_result["malformed"])
    log.info("Unique Kafka records:      %d", stage_result["records"])
    log.info("Unique posts:        %d",       entity_results["posts"])
    log.info("Chart: %s", path)


if __name__ == "__main__":
    quakes_pipeline()
