# Source Design Card (checkpoint A, Session 1 · Thu 05 Nov)

Fill this in during Session 1 (Thu 05 Nov) with **measured** numbers from `python probe.py <source>`. Then tick the self-check in section 7 **before** you build the producer: it is how you check that your source will work for the whole project.

**Name:**
**Date:**

## 1. The source
- Source and URL: BlueSky wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
- Method: WebSocket (push)
- Account, key or User-Agent needed: N/A, stream is public
- Limits I know about (rate limits, paging, reconnects): No limits from probe run of 60 seconds. No errors or authorization needed, but errors could show up in a longer runtime, such as connection drop. Also, some posts are missing fields such as langs or texts. 

## 2. One real message
Paste one message from the probe (shorten long text fields):

```json

--- sample message 1 ---
{
  "did": "did:plc:e53zwi73ttdsz5jq6kxsatqq",
  "time_us": 1793971018095044,
  "cursor": 26882540193,
  "kind": "commit",
  "commit": {
    "rev": "3mznhqnofoq2k",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3mznhqmg6ic2x",
    "record": {
      "$type": "app.bsky.feed.post",
      "createdAt": "2026-11-06T13:16:56.042Z",
      "embed": {
        "$type": "app.bsky.embed.external",
        "external": {
          "description": "YouTube video by ちゃおチャンネル【公式】CIAO",
          "thumb": {
            "$type": "blob",
            "mimeType": "image/jpeg",
            "ref": {
              "$link": "bafkreifo6lksz3zcrwstfkuw3xmaq2ycvzu3g4eraqvstr5k63asubnayq"
            },
            "size": 84070
          },
          "title": "【ちゃお特別セレクション　ミルモの里編】わがまま☆フェアリー ミルモでポン！　第50話 「昔のミルモをたおせ！」",
          "uri": "https://youtube.com/watch?v=6p4h1us8buw&si=JeMBSQ
  ... (cut)

```

## 3. Volume maths
- Measured rate (from the probe): 54.32 per second
- Live capture needed for 50,000: 15 minutes
- History / backfill available? How many days? Jetstream can replay a limited window with ?cursor=<time_us>. Not needed as live capture allows for 50,000 within 15 minutes. 
- My plan to reach 50,000 raw records by the start of Session 3 (Thu 12 Nov): Only needs roughly 15 minutes. Run the producer Wednesday, a day before class so that 50,000 are collected, even with errors or disconnections, and if I have issues, I can ask the professor.

## 4. Topic design
- Topic name: bluesky-posts (created with `-p 3 -r 3` **before** the producer runs)
- Kafka key: did
- Why this key (ordering, spread across partitions): This key is order sensitive as some posts may be deleted after it's posted. Also, did is present in 100% of the messages. 

## 5. Duplicates
- What makes two records **the same Kafka record**? (landed twice by the lander) 

When there is a crash and reset and the same records are sent to the lander. 


- What makes two records **the same thing** (the same trade, post, aircraft position)? 

When there are different messages, but referring to the same post. 


- If the same thing can arrive in several versions, which one do I keep? 

Keep the one with the most recent timestamp. 


## 6. Research question (draft)
- One question I could answer with time windows, a comparison, an anomaly or a join: How does the posting rate by language change by hour of the day

## 7. Self-check (checkpoint A)
Tick a box by changing `[ ]` to `[x]`. If a box stays empty, fix the card or choose another source from SOURCES.md. `kit/check_project.py` counts these boxes.

- [x] I ran the probe for this source today and messages arrived (own idea: it worked here, in class)
- [x] The rate in section 3 is measured, not guessed
- [x] My volume plan reaches 50,000 raw records before Session 3 (Thu 12 Nov), with margin
- [x] It needs no API key or account that takes days to get
- [x] My key has a reason that fits my question
- [x] I named both kinds of duplicates for my source
- [x] My draft question can be answered with a time window, a comparison, an anomaly or a join
