# Real-Time Sentiment & Trend Pipeline

A learning/portfolio data-engineering project: stream public social text, score
sentiment and (later) embeddings **as messages flow through the pipeline**, roll
them up into time windows, detect trends, and show it live on a dashboard. The
point is freshness — the dashboard reflects what's happening *right now*, not
yesterday's batch job.

Built in phases, each one runnable end to end before the next is added. This
README is current through **Phase 2**. Phases 3-5 are described below as the
plan, not yet implemented.

## Architecture

```
 sources            producer              Redpanda          processor                 sinks
┌─────────┐      ┌───────────────┐      ┌──────────┐      ┌──────────────────┐      ┌──────────┐
│ Bluesky │─────▶│ polls/streams │─────▶│  topic   │─────▶│ batched sentiment │─────▶│ SQLite   │  (Phase 1-2)
│ Jetstream│      │ -> publish    │      │raw_posts │      │ + embeddings      │      │ClickHouse│  (Phase 3+)
│ Reddit  │      │   JSON        │      │          │      │ + windowing       │      │ Qdrant   │  (Phase 4+)
└─────────┘      └───────────────┘      └──────────┘      │ + trend detection │      └──────────┘
                                                            └──────────────────┘            │
                                                                                             ▼
                                                                                    Grafana (Phase 3+)
                                                                                 Streamlit (Phase 1-2)
```

Everything upstream of the sinks is the same for every phase; phases mainly add
**what happens inside the processor** and **where results land**.

## Repo layout

```
common/            shared schema, config, sentiment interface, source interface
  sources/           pluggable source adapters (bluesky.py, reddit.py)
producer/          Phase 2+: source -> Kafka producer
processor/         Phase 2+: Kafka consumer -> inference -> sinks
phase1/            Phase 1: standalone script + Streamlit view (no broker)
infra/             docker-compose.yml (Redpanda + producer + processor)
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
`sentiment_score`. Embeddings (Phase 4) are written straight to Qdrant rather
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

### Run it

```bash
cp .env.example .env     # if you haven't already
docker compose -f infra/docker-compose.yml up --build
```

This brings up:

| service           | what it does                                        | exposed at              |
|--------------------|------------------------------------------------------|--------------------------|
| `redpanda`         | the broker                                            | `localhost:19092` (Kafka API) |
| `redpanda-console` | browse topics/messages in a UI                        | http://localhost:8080   |
| `topic-init`       | one-shot: creates `raw_posts` (3 partitions), then exits | —                    |
| `producer`         | streams from Bluesky (or Reddit) into `raw_posts`     | —                        |
| `processor`        | consumes, scores, writes to `/data/phase2.db` (mounted from `./data`) | — |

Watch it work:

```bash
# tail the processor's structured logs
docker compose -f infra/docker-compose.yml logs -f processor

# or point the Phase 1 dashboard at the Phase 2 database
SQLITE_PATH=data/phase2.db uv run streamlit run phase1/dashboard.py
```

Stop everything with `docker compose -f infra/docker-compose.yml down` (add
`-v` to also drop Redpanda's data volume).

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
- **Storage is still SQLite, not ClickHouse.** The spec introduces ClickHouse
  in Phase 3 alongside windowed aggregation; moving to it before there's
  anything to aggregate would just be earlier plumbing for no present benefit.
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
  apart on a live firehose. Phase 3's tumbling windows will key off
  processing time too; the note there explains where event-time would
  actually change the answer (late/out-of-order arrivals).

---

## Planned: Phases 3-5 (not yet built)

**Phase 3 — Windowed aggregation, trend detection, real stores.** Processor
computes tumbling 5-minute windows (avg sentiment + volume per source,
processing-time). Storage moves to ClickHouse; Grafana added with
dashboards-as-code provisioning. Trend detection: rolling z-score on
per-term volume, flagging a term when its current window is ≥2.5 std devs
above its trailing baseline.

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

14 tests covering schema round-tripping, both sentiment backends' interface
contract, Bluesky event parsing (including malformed/irrelevant events), and
SQLite upsert idempotency.

## Config reference

See `.env.example` for every variable with inline explanation. Nothing in
code reads secrets directly — everything goes through `common/config.py`, and
`.env` is gitignored.
