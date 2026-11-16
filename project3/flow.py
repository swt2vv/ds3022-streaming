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
CHART2 = f"charts/posts_by_lang{TAG}.png" 


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
            SELECT time_bucket(INTERVAL '1 hour', event_time) AS hour, lang, COUNT(*) AS posts
            FROM posts
            GROUP BY hour, lang
            ORDER BY hour, lang""")

import numpy as np
@task
def chart():
    with duckdb.connect(DB, read_only=True) as con:
        df = con.execute("""
            SELECT hour, lang, posts
            FROM mart_hourly
            WHERE hour >= TIMESTAMP '2026-11-12 00:00:00'
                AND hour <  TIMESTAMP '2026-11-13 00:00:00'
                AND lang IN (
                    SELECT lang 
                    FROM mart_hourly
                    GROUP BY lang 
                    ORDER BY sum(posts) DESC 
                    LIMIT 5)
            ORDER BY hour, lang
        """).df()
    df = df.replace('en','General English').replace('es', 'Spanish').replace('ko', 'Korean').replace('ja', 'Japanese').replace('de', 'German').replace('fr', 'French').replace('pt', 'Portuguese').replace('nl', 'Dutch').replace('en-US', 'U.S. English').replace('ar', 'Arabic').replace('it', 'Italian').replace('tr', 'Turkish')
    pivot = df.pivot(index="hour", columns="lang", values="posts").fillna(0)
    hours, langs = pivot.index, pivot.columns

    x = np.arange(len(hours))
    width = 0.8 / len(langs)

    os.makedirs("charts", exist_ok=True)
    fig, ax = plt.subplots(figsize=(max(8, len(hours) * 0.7), 4))
    for i, lang in enumerate(langs):
        offset = (i - (len(langs) - 1) / 2) * width
        ax.bar(x + offset, pivot[lang], width, label=lang)

    ax.set_xticks(x)
    ax.set_xticklabels([h.strftime("%H:%M") for h in hours],
                       rotation=45, ha="right")
    ax.set_title("Posts per Hour by Language")
    ax.set_xlabel("Hour (UTC) on Nov 12th")
    ax.set_ylabel("Posts")
    ax.legend(title="Language")
    fig.tight_layout()
    fig.savefig(CHART, dpi=120)
    plt.close(fig)
    return CHART


@task
def chart2():
    with duckdb.connect(DB, read_only=True) as con:
        df = con.execute("""
            SELECT lang, SUM(posts) AS posts
            FROM mart_hourly
            WHERE lang IS NOT NULL
            GROUP BY lang
            ORDER BY posts DESC, lang
            LIMIT 10
        """).df()
        
    df = df.replace('en','General English').replace('es', 'Spanish').replace('ko', 'Korean').replace('ja', 'Japanese').replace('de', 'German').replace('fr', 'French').replace('pt', 'Portuguese').replace('nl', 'Dutch').replace('en-US', 'U.S. English').replace('ar', 'Arabic').replace('it', 'Italian').replace('tr', 'Turkish')

    x = np.arange(len(df['lang']))
    width = 0.8 / len(df['posts'])


    os.makedirs("charts", exist_ok=True)
    plt.figure(figsize=(8, 4))
    plt.bar(x = df['lang'], height = df['posts'], color="#2a7fb8")
    plt.title("Total Posts on BlueSky Per Top 10 Languages Within Sample Data")
    plt.xlabel("Language")
    plt.xticks(rotation = 45)
    plt.ylabel("Posts")
    plt.tight_layout()
    plt.savefig(CHART2, dpi=120)
    return CHART2

@flow(name="quakes-pipeline")
def quakes_pipeline():
    log = get_run_logger()
    raw = load_raw()
    stage_result = stage()
    entity_results = entities()
    mart_results = mart()
    path = chart()
    path2 = chart2()

    log.info("Raw records landed:        %d", raw)
    log.info("Malformed (kept in raw):   %d", stage_result["malformed"])
    log.info("Unique Kafka records:      %d", stage_result["records"])
    log.info("Unique posts:        %d",       entity_results["posts"])
    log.info("Chart: %s", path)
    log.info("Chart 2: %s", path2)

if __name__ == "__main__":
    quakes_pipeline()
