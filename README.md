# Real-Time Sentiment & Trend Pipeline

A learning/portfolio data-engineering project: stream public social text, score
sentiment and (later) embeddings **as messages flow through the pipeline**, roll
them up into time windows, detect trends, and show it live on a dashboard. The
point is freshness — the dashboard reflects what's happening *right now*, not
yesterday's batch job.

Built in phases, each one runnable end to end before the next is added. This
README is current through **Phase 3**. Phases 4-5 are described below as the
plan, not yet implemented.

## Architecture

```
 sources            producer              Redpanda          processor                 sinks
┌─────────┐      ┌───────────────┐      ┌──────────┐      ┌──────────────────┐      ┌──────────┐
│ Bluesky │─────▶│ polls/streams │─────▶│  topic   │─────▶│ sentiment         │─────▶│ SQLite   │  (Phase 1)
│ Jetstream│      │ -> publish    │      │raw_posts │      │ + windowing       │      │ClickHouse│  (Phase 3+)
│ Reddit  │      │   JSON        │      │          │      │ + trend detection │      │ Qdrant   │  (Phase 4+)
└─────────┘      └───────────────┘      └──────────┘      │ + embeddings (P4) │      └──────────┘
                                                            └──────────────────┘            │
                                                                                             ▼
                                                                                    Grafana (Phase 3+)
                                                                                   Streamlit (Phase 1)
```

Everything upstream of the sinks is the same for every phase; phases mainly add
**what happens inside the processor** and **where results land**.

## Repo layout

```
common/            shared schema, config, sentiment interface, source interface
  sources/           pluggable source adapters (bluesky.py, reddit.py)
  terms.py           Phase 3: post text -> set of terms
  trends.py          Phase 3: rolling z-score trend detection (pure logic)
producer/          Phase 2+: source -> Kafka producer
processor/         Phase 2+: Kafka consumer -> inference -> windows/trends -> sinks
  clickhouse.py      Phase 3: ClickHouse DDL + batching sinks
phase1/            Phase 1: standalone script + Streamlit view (no broker)
infra/             docker-compose.yml (Redpanda, producer, processor, ClickHouse, Grafana)
  grafana/           Phase 3: provisioned datasource + dashboards-as-code
tests/             unit tests for schema, sentiment, sources, store
.env.example       all config, copy to .env
```

## Message schema

`raw_posts` topic, one JSON object per message:

```json
{ "id": "...", "source": "bluesky", "author": "...", "text": "...",
  "created_at": "2026-01-01T00:00:00Z", "fetched_at": "2026-01-01T00:00:01Z",
  "url": "..." }
```

Scored messages (what lands in storage) add `sentiment_label` and
`sentiment_score` (confidence/magnitude, always ≥ 0), plus from Phase 3
`polarity` (score signed by label: positive `+score`, negative `-score`,
neutral `0`, so it can be averaged) and `processed_at`. Embeddings (Phase 4) are written straight to Qdrant rather
than carried on the topic, to keep messages small and the hot path light.

**Idempotency**: every row is keyed by `id` and written with an upsert, so
re-processing the same message (e.g. a consumer restart re-reading
not-yet-committed offsets) overwrites the same row instead of double-counting
it.

---

## Phase 1 — Prove the inference loop (no broker)

One process, no Kafka: `source.stream() -> sentiment.classify() -> SqliteStore`.
This exists to validate the two riskiest, most swappable pieces — a live
source adapter and the sentiment interface — in isolation, before Phase 2
makes failures harder to localize (is it the source? the broker? the
consumer?).

**Verified**: ran live against the real Bluesky firehose, ~1,200 messages
scored and written in ~27s (~45 msg/s sustained, bursts higher), graceful
`SIGTERM` shutdown confirmed, Streamlit dashboard confirmed serving HTTP 200
and reading live rows.

### Run it

```bash
cp .env.example .env            # defaults: SOURCE=bluesky, SENTIMENT_BACKEND=vader
uv sync                         # installs everything with uv (needs Python 3.11+)
uv run python -m phase1.run     # terminal 1: streams + scores, Ctrl+C to stop
uv run streamlit run phase1/dashboard.py   # terminal 2: live view at localhost:8501
```

No credentials needed for the default (`SOURCE=bluesky`). To try Reddit
instead, set `SOURCE=reddit` and fill in `REDDIT_CLIENT_ID` /
`REDDIT_CLIENT_SECRET` in `.env` (free "script" app at
https://www.reddit.com/prefs/apps — a signup step, not a paid service).

---

## Phase 2 — Decouple with Redpanda

Splits Phase 1's one process into two, connected by a Kafka-API-compatible
broker (Redpanda), so the source-polling and the inference work can fail,
restart, and scale independently:

- **`producer/`**: `source.stream() -> confluent-kafka Producer -> topic raw_posts`,
  keyed by message `id`.
- **`processor/`**: a Quix Streams `Application` consumes `raw_posts` under a
  named consumer group, scores each message, upserts it into SQLite.

> **Superseded by Phase 3**: the processor now writes ClickHouse instead of
> SQLite, and compose also brings up ClickHouse and Grafana. To run it, see
> [Phase 3 → Run it](#run-it-1). The SQLite-only Phase 2 processor is at
> commit `11996b3`.

### Tradeoffs worth knowing about

- **Producer uses `confluent-kafka` directly; processor uses Quix Streams.**
  The spec names Quix Streams as *the* stream processor, and it earns its
  keep on the consumer side — it wraps consumer-group management and offset
  checkpointing, and Phase 3's windowing needs it anyway. The producer has no
  windowing or consumer-group concerns, just "publish messages reliably," so
  a plain `confluent-kafka` producer is less machinery for the same result.
  One extra library in the dependency tree; explicit tradeoff, not an
  oversight.
- **Default sentiment backend is VADER, not the transformer.** VADER is
  instant and needs no download, which keeps "does this run at all" fast to
  verify. The transformer backend (`cardiffnlp/twitter-roberta-base-sentiment-latest`)
  is implemented behind the same interface and is a one-line `.env` change
  (`SENTIMENT_BACKEND=transformer`) — just note the processor's Docker image
  bundles `torch` + `transformers` regardless of which backend is active (so
  the model can be swapped at runtime without rebuilding), meaning the first
  `docker compose build` downloads ~1GB+ and takes a few minutes even though
  the default run doesn't use any of it yet.
- **Storage was still SQLite, not ClickHouse.** The spec introduces ClickHouse
  in Phase 3 alongside windowed aggregation; moving to it before there was
  anything to aggregate would have been earlier plumbing for no benefit.
- **Graceful shutdown is "flush, not instant."** On `SIGTERM`, the producer
  stops after its current message and flushes Kafka's send buffer; Quix
  Streams' `Application.run()` handles `SIGTERM`/`SIGINT` for the processor
  internally (stops polling, commits offsets, exits `run()`). For a
  low-volume source, "stops after the current message" can mean a short wait
  for the next message to arrive before the shutdown check is reached —
  acceptable for a learning project, worth knowing if you need sub-second
  shutdown guarantees.
- **Bluesky events carry the author's DID, not their handle.** Resolving a
  human-readable handle is a separate API call per author; doing it inline
  would add latency and a rate-limit dependency to the hot path, so the DID
  is stored as-is.
- **Processing-time, not event-time, throughout.** "Processed at" (when the
  processor saw the message) drives everything so far, not `created_at` (when
  the post was actually made) — simpler, and the two are usually seconds
  apart on a live firehose. Phase 3's tumbling windows key off
  processing time too; the note there explains where event-time would
  actually change the answer (late/out-of-order arrivals).

---

## Phase 3 — Windowed aggregation, trend detection, real stores

The processor goes from "score and store each message" to a small stream
topology. One consumer reads `raw_posts`, scores each post, and fans out into
three branches:

```
raw_posts -> score -+-> scored_posts                                  every post
                    +-> group_by(source) -> 5-min tumbling window
                    |     (count, avg polarity, label counts)
                    |     -> sentiment_windows                        1 row / source / window
                    +-> explode into terms -> group_by(term)
                          -> 5-min tumbling count -> z-score vs the
                          term's trailing baseline -> term_windows    1 row / busy term / window
```

- **Windows**: tumbling, 5 minutes (`WINDOW_MINUTES`), processing-time.
  `group_by` re-keys the stream (raw messages are keyed by post id) via
  internal `repartition__…` topics that Quix creates in Redpanda, so each
  source's or term's messages land on one partition. Window state lives in
  RocksDB and is backed up to Quix-managed changelog topics.
- **Trend detection** (`common/trends.py`): each term keeps its last 12
  window counts in state. When a term's window closes, its count is scored
  as `z = (count − mean) / max(std, 1)` against that baseline, and it's
  flagged when `z ≥ 2.5`, `count ≥ 10`, and the baseline has ≥ 3 windows
  (all configurable in `.env`). Volume counts *posts mentioning a term*,
  not raw occurrences, so one spammy post can't fake a spike.
- **Storage**: ClickHouse, three `ReplacingMergeTree` tables (DDL in
  `processor/clickhouse.py`, created by the processor on startup). Raw posts
  have a 7-day TTL; the window tables keep everything.
- **Dashboard**: Grafana with the ClickHouse datasource and a dashboard
  provisioned from `infra/grafana/` (dashboards-as-code). It shows live
  post volume and polarity, average sentiment and sentiment mix per window,
  trending terms, the top terms in the latest window, and the latest posts.

### Run it

```bash
cp .env.example .env     # if you haven't already
docker compose -f infra/docker-compose.yml up --build
```

| service            | what it does                                             | exposed at                      |
|--------------------|----------------------------------------------------------|---------------------------------|
| `redpanda`         | the broker                                               | `localhost:19092` (Kafka API)   |
| `redpanda-console` | browse topics/messages in a UI                           | http://localhost:8080           |
| `topic-init`       | one-shot: creates `raw_posts` (3 partitions), then exits | —                               |
| `producer`         | streams from Bluesky (or Reddit) into `raw_posts`        | —                               |
| `processor`        | scores, windows, detects trends, writes ClickHouse      | —                               |
| `clickhouse`       | storage                                                  | http://localhost:8123 (`/play` for a SQL UI; user/pass `pipeline`/`pipeline`) |
| `grafana`          | dashboards                                               | http://localhost:3000 (anonymous view; `admin`/`admin` to log in) |

Raw posts show up in Grafana within seconds. The first window rows appear
after the first 5-minute window closes, and the earliest a term can be
flagged as trending is after 3 windows (15 minutes) of baseline.

```bash
# structured logs, including a line per trending term
docker compose -f infra/docker-compose.yml logs -f processor

# query ClickHouse directly
curl -s 'http://localhost:8123/?user=pipeline&password=pipeline&database=sentiment' \
  --data-binary 'SELECT * FROM term_windows FINAL WHERE is_trending ORDER BY window_start DESC LIMIT 20 FORMAT PrettyCompact'
```

Stop with `docker compose -f infra/docker-compose.yml down`; add `-v` to also
drop Redpanda, ClickHouse, Grafana and processor-state volumes.

### Tradeoffs worth knowing about

- **At-least-once into ClickHouse, deduped by the table engine.** Rows go
  through Quix `BatchingSink`s, which write at each checkpoint (every 5s)
  *before* offsets are committed. A crash between write and commit replays
  that batch, and `ReplacingMergeTree` collapses the duplicates on its next
  merge. Merges are eventual, so the dashboard queries use `FINAL` for
  exact counts. Batching also matters on its own terms: ClickHouse handles
  a few large inserts well and many single-row inserts badly.
- **Windows close on the partition watermark, not per key.** Quix's default
  closes a key's window only when that same key gets a new message. For
  terms that would mean a spike gets reported whenever the term next shows
  up, possibly hours later. Partition closing emits every window once
  *any* newer message arrives on the partition. The catch: if the stream
  stops entirely, the last window stays open until traffic resumes.
- **Processing-time windows.** Each message is stamped with the wall clock
  when the processor reads it. On a live firehose that's seconds from
  `created_at`. Where it changes the answer is a backlog: if the processor
  is down for an hour, the catch-up lands that hour's posts in the
  catch-up windows. Event-time would fix that, but it needs a grace period
  for late data and trusting client-supplied `created_at`, which on Bluesky
  can be anything.
- **Zero-filling the baseline.** A term only produces a window when it was
  mentioned, so its quiet windows would otherwise be missing from the
  baseline and inflate the mean. Gaps are filled with zeros when the term
  next appears. A brand-new term is treated as having had 0 mentions in
  every window since the processor started, which is what lets a word
  nobody was using an hour ago get flagged at all. That start point is
  in-memory, so after a restart new terms get a shorter implied baseline,
  which errs toward *not* flagging.
- **Term state grows with vocabulary.** Every term ever seen keeps a small
  history in RocksDB. Nothing expires it yet; fine at this scale, worth a
  TTL if this ran for months.
- **Only terms with ≥ `TREND_MIN_COUNT` mentions are written to
  ClickHouse.** All terms update their history, but the long tail of 1-2
  mention terms would be most of the table and none of the signal.
- **Tokenization is naive** (`common/terms.py`): English stopwords only, no
  stemming, and scripts without spaces (Japanese, Chinese) come out as
  whole-phrase tokens that effectively never trend.
- **ClickHouse credentials are fixed dev values in compose**, not read from
  `.env`, so ClickHouse, Grafana and the processor can't drift apart. Not
  for anything exposed beyond localhost.

---

## Planned: Phases 4-5 (not yet built)

**Phase 4 — Embeddings + emerging topics.** Each message embedded
(`all-MiniLM-L6-v2`), vectors stored in Qdrant, clustered to surface *what*
people are discussing, not just sentiment. Top emerging clusters exposed on
the dashboard.

**Phase 5 — Reliability.** Batch inference (32-64 messages per `classify()`
call instead of one at a time). Dead-letter topic for parse/inference
failures. Backpressure handling (more partitions + parallel consumers, or
heavier batching — documented once decided). Consumer lag and throughput
surfaced in Grafana/logs.

---

## Tests

```bash
uv run pytest
```

29 tests covering schema round-tripping, both sentiment backends' interface
contract and signed polarity, Bluesky event parsing (including
malformed/irrelevant events), SQLite upsert idempotency, term extraction,
the trend z-score logic (gap filling, new terms, replays, thresholds), and
ClickHouse row mapping.

## Config reference

See `.env.example` for every variable with inline explanation. Nothing in
code reads secrets directly — everything goes through `common/config.py`, and
`.env` is gitignored.
