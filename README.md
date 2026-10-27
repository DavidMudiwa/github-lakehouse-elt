# GitHub Lakehouse ELT

A batch, asset-first ELT pipeline built with **Dagster**. It pulls GitHub data
into **Google Cloud Storage** (raw JSON → typed Parquet) and loads it into
**BigQuery** for analytics.

```
GitHub API ──▶ Dagster ──▶ GCS (raw JSON ──▶ processed Parquet) ──▶ BigQuery
```

## Problem

GitHub is where a lot of engineering work actually happens: repositories,
commits, authors, activity over time. Teams want to answer questions like
"which repos changed this week?", "how active was this project?", or "how does
commit volume trend over time?" — but the raw API is not built for that:

- Responses are **nested, verbose, and paginated**, not analytics-ready.
- The API is **rate-limited** (60 requests/hour unauthenticated), so re-fetching
  everything on every run is expensive and fragile.
- Hand-written scripts **re-download data, keep no history, and are not
  idempotent** — a re-run can duplicate rows or silently overwrite good data.
- There is **no lineage, no schema contract, no quality checks, and no cost
  control** — so nobody trusts the output.

This project solves that by treating ingestion as a pipeline, not a script:

- Land **immutable raw data** so every load can be audited and replayed.
- Shape it into **typed, partitioned** tables that are cheap to query.
- Make **re-runs and backfills safe** (idempotent by construction).
- Add **lineage, a schedule, retries, and asset checks** so the pipeline is
  operable, not just runnable.

It is also a reusable pattern: any REST API can follow the same
`raw → parquet → warehouse` shape.

## What it does

Two independent asset chains share the same pattern (`raw → parquet → bq`):

```
github_repositories_raw ─▶ github_repositories_parquet ─▶ github_repositories_bq
       (snapshot)                  (Parquet in GCS)            (BigQuery table)

github_commits_raw ─────▶ github_commits_parquet ─────▶ github_commits_bq
   (daily partition)              (Parquet in GCS)        (dt-partitioned table)
```

- **raw** — faithful, immutable, append-only JSON in GCS (replayable/auditable).
- **processed** — a normalised, typed Parquet contract written to a deterministic
  key (safe to overwrite → idempotent).
- **bq** — loaded with an explicit schema into a `dt`-partitioned table using a
  partition decorator and `WRITE_TRUNCATE` (re-runs replace one partition).

Lineage is declared with `AssetIn`: each downstream asset takes the upstream's
returned `gs://` URI as an argument, so Dagster infers ordering and draws the
graph. Small values (URIs) are carried between assets by an `InMemoryIOManager`;
the real bytes live in GCS/BigQuery. Because an in-memory IO manager only works
in a single process, `Definitions(executor=in_process_executor)` is set — otherwise
scheduler/daemon runs use the multiprocess executor and cannot load the upstream
values.

## Data sources

Both sources are the **GitHub REST API** (`https://api.github.com`), unauthenticated.

| Source | Endpoint | Shape | Notes |
|---|---|---|---|
| Repositories | `GET /repositories` | Global list of public repositories | Paginated 100/page (capped at 3 pages here). Returns GitHub's **minimal** repo representation — no `created_at` / `updated_at`. Treated as an unpartitioned **snapshot**. |
| Commits | `GET /repos/{owner}/{repo}/commits` | Commit history for one repo | Paginated 100/page (capped at 3 pages). Supports `since`/`until`, so each **daily partition** is a real day slice. Default repo configurable via run config (currently `dagster-io/dagster`). |

Constraints that shaped the design:

- **60 requests/hour** unauthenticated; a backfill costs ~1 request per partition
  plus pagination.
- **No "all commits" endpoint** — commits are always per-repository, so full
  coverage requires fan-out (out of scope for this phase).
- The repositories endpoint has **no date filter**, so daily partitions would
  mislabel snapshot data; it stays unpartitioned.

## Architecture decisions

**Why Dagster (orchestration).**
The pipeline is modelled as **assets**, not tasks: each node is a persisted
dataset with declared dependencies. That gives lineage, per-partition
materialisation state, resources for external systems, schedules, and asset
checks in one place. Partitioned assets and `RunRequest`s make daily incremental
runs and backfills first-class.
*Trade-off:* Dagster is another system to operate (daemon, instance, UI), and the
asset model has a learning curve.

**Why Google Cloud Storage (data lake).**
GCS is cheap, durable object storage that decouples storage from compute. Raw
JSON is kept immutable for audit/replay, and Parquet is written next to it. The
bucket uses uniform bucket-level access, public-access prevention, and lifecycle
tiering (raw cools over time; processed stays hot). Object-store prefixes map
naturally to Hive-style `dt=` partitions.
*Trade-off:* it is not a database — no schema enforcement or queries; you manage
keys, naming, and consistency yourself.

**Why Parquet (processed layer).**
Parquet is **columnar, typed, and compressed**, so analytical reads and warehouse
loads touch only the columns they need and move far fewer bytes. In this project a
300-row repositories extract drops from ~1.33 MB of JSON to tens of KB of Parquet
once projected onto the explicit contract.
*Trade-off:* you must manage schema and rewriting; it is not human-readable like
JSON, which is why raw JSON is retained.

**Why BigQuery (warehouse).**
Serverless, fully managed SQL warehouse with **column-based partitioning** and
**clustering**, cheap storage, and a native Parquet load path. A load job into
`table$YYYYMMDD` with `WRITE_TRUNCATE` replaces exactly one day's partition, which
makes re-runs idempotent.
*Trade-off:* you pay for bytes scanned, so partitioning and clustering matter;
dataset location is immutable and must be co-located with the GCS bucket.

**Why the raw/processed split.**
Raw is the source of truth (append-only, replayable); processed is analytics-ready
(deterministic key, overwrite-safe). Separating them means a transformation bug can
be fixed and re-run without re-fetching the source, and every derived row is
traceable to its raw input.

## Tech stack

- **Dagster** (assets, resources, partitions, schedules, asset checks)
- **Google Cloud Storage** (data lake) and **BigQuery** (warehouse)
- **pyarrow** (Parquet), **requests** (GitHub API)
- Python 3.11+, managed with **uv**; runs in **Docker Compose**

## Project layout

```
assets/       Dagster assets
  github.py       raw    (GitHub API → GCS JSON)
  processed.py    parquet (JSON → Parquet in GCS)
  bigquery.py     bq     (Parquet → BigQuery)
  checks.py       asset checks
resources/    injectable handles: GCSResource, BigQueryResource
common/       shared logic
  keys.py         GCS key/naming convention
  normalize.py    raw record → warehouse contract (adds dt, parses timestamps)
  parquet.py      JSON → Parquet
  schemas.py      explicit BigQuery schemas + partition/cluster fields
  partitions.py   daily partition definition
  policies.py     shared retry policy
schedules/    commits_daily_schedule + commits_job
infra/        cloud config as code (gcs-lifecycle.json)
scripts/      standalone runners / verification helpers
tests/        stdlib unittest suite
secrets/      gitignored credentials (ADC)
```

## Data conventions

**GCS object keys** (executable in `common/keys.py`, locked by tests):

```
raw/<dataset>/[repo=<owner>__<repo>/]dt=YYYY-MM-DD/<dataset>_<UTCtimestamp>.json
processed/<dataset>/[repo=<owner>__<repo>/]dt=YYYY-MM-DD/<dataset>.parquet
```

- `dt=YYYY-MM-DD` becomes the BigQuery partition column; `repo=` anticipates
  multi-repo fan-out. Slashes in partition values are flattened
  (`dagster-io/dagster` → `dagster-io__dagster`).
- Lifecycle: `raw/` tiers `STANDARD → NEARLINE@30d → COLDLINE@90d → ARCHIVE@365d`;
  `processed/` stays `STANDARD`.

**BigQuery contract** (`common/schemas.py`, populated by `common/normalize.py`):

| table | columns (type) |
|---|---|
| `repositories` | repository_id INT64, name STRING, full_name STRING, private BOOL, html_url STRING, description STRING, fork BOOL, owner_login STRING, dt DATE |
| `commits` | sha STRING, repository STRING, author STRING, message STRING, committed_at TIMESTAMP, dt DATE |

Both tables are partitioned by `dt`; `commits` is clustered on `repository`.

## Partitions, schedule, checks

- `github_commits_*` are **daily-partitioned** (start `2026-09-01` UTC); the
  partition key becomes the data's `dt`, and raw commits slice the day via
  GitHub `?since=&until=`. `github_repositories_*` are unpartitioned snapshots
  (the `/repositories` endpoint has no date filter).
- `commits_daily_schedule` (cron `0 9 * * *`, UTC) requests **yesterday's**
  partition. It ships **STOPPED**; enable it in the UI to arm it (the
  `SchedulerDaemon` must be running).
- Asset checks assert the BigQuery tables are non-empty and contain no rows
  missing `dt`.

## Incremental Processing

The two sources use deliberately different strategies.

**Commits — partition-based incremental.** Each run processes one day — the
Dagster partition key (e.g. `2026-09-22`). The raw asset turns that key into a
GitHub `?since=&until=` window, so the fetched data is genuinely that day's
commits, not "whatever was latest". The partition key is the
**watermark/checkpoint**: "2026-09-22 is materialised" means that slice is done,
and Dagster tracks materialised partitions in its instance. There is no separate
watermark table, because the source is enumerable by day — the set of
materialised partitions *is* the checkpoint. To fill a gap or correct data,
re-materialise or backfill that partition; the partition-scoped
`WRITE_TRUNCATE` makes the re-run idempotent.

**Repositories — full refresh.** `/repositories` has no date filter, so there is
no bounded slice to checkpoint. Each run re-fetches the snapshot and writes it to
that run day's `dt` partition (replacing that partition). Daily-partitioning the
Dagster asset would only relabel today's data as an old day.

**Limits.** Partition-based incremental works because the source is
date-sliceable. A high-frequency or non-enumerable source would instead need a
persisted **high-water mark** (last processed timestamp or ID) plus upserts, and
would have to handle late-arriving data. This project deliberately covers the
partition-based case only.

## Orchestration

**DAG dependencies.** Lineage is declared with `AssetIn`: each `*_parquet` asset
consumes the raw asset's returned `gs://` URI, and each `*_bq` asset consumes the
Parquet URI. Dagster derives the DAG and execution order from those inputs, so
materialising a downstream asset pulls its upstream chain. The run executes
in-process (`in_process_executor`) so the `InMemoryIOManager` can carry URIs
between steps.

**Retries.** Every asset shares `common/policies.py::RETRY_POLICY`: 3 retries, a
5s base delay, exponential backoff, and full jitter. This absorbs transient 5xx
and network errors. It does not rescue a 403 rate-limit — that needs a token, not
another retry.

**Schedule.** `commits_daily_schedule` (cron `0 9 * * *`, UTC) computes
yesterday's partition at evaluation time and emits
`RunRequest(partition_key=yesterday, run_key=yesterday)`. The `run_key` dedupes
one run per partition across scheduler recoveries.

**Backfills.** A backfill is many partition-scoped runs. Use the Dagster UI
against a partition range, or `scripts/backfill_commits.py` for an in-process
range (capped at 5 partitions to respect the GitHub rate limit). Each partition
run is independent and idempotent, so a failure on one day does not affect the
others.

## Testing

Three layers:

- **Unit tests** (`tests/`) — stdlib `unittest`, no extra framework, covering the
  pure logic: GCS key construction (`tests/test_keys.py`) and record
  normalisation (`tests/test_normalize.py`). Run:
  `uv run python -m unittest discover -s tests -t . -v`.
- **Integration / smoke scripts** (`scripts/`) — exercise the real cloud services
  instead of mocks: auth (`verify_gcp.py`), the GCS resource round-trip
  (`check_gcs_resource.py`), the full `raw → parquet → bq` runs, the partition the
  schedule would request (`check_schedule.py`), and definitions loading
  (`validate_defs.py`). These are run manually against a sandbox GCP project.
- **Data-quality tests** (`assets/checks.py`) — Dagster asset checks assert the
  warehouse tables are non-empty and contain no rows missing `dt`. Run them
  standalone with `scripts/run_checks.py`, or view their results in the UI.

CI — running unit tests and definitions validation on every push — is not
included here; the scripts above are run manually.

## Getting started

**Prerequisites:** WSL with Docker, `uv`, and a GCP project with a GCS bucket
and BigQuery dataset. Provisioning is done with `gcloud` from Windows.

**Configuration** (`.env`, gitignored):

```
DAGSTER_HOME=/app/.dagster
GOOGLE_CLOUD_PROJECT=<project>
GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/adc.json
GCS_BUCKET=<bucket>
BIGQUERY_DATASET=github
```

Place Application Default Credentials at `secrets/adc.json`.

**Run the Dagster UI (Docker):**

```bash
docker compose up --build        # http://localhost:3000
```

When you change dependencies, also sync the container venv (the named
`.venv` volume shadows image rebuilds):

```bash
docker compose exec -T elt-platform uv sync --frozen
docker compose restart elt-platform
```

**Run pieces locally (from the repo root):**

```bash
uv run python scripts/validate_defs.py          # load/validate Definitions
uv run python scripts/describe_graph.py         # print lineage/partitions/checks
uv run python scripts/run_repositories_asset.py # raw repositories (hits GitHub)
PARTITION=2026-09-22 uv run python scripts/run_bigquery_assets.py   # full chain
BACKFILL_START=2026-09-21 BACKFILL_END=2026-09-22 \
  uv run python scripts/backfill_commits.py     # partition backfill
uv run python scripts/run_checks.py             # run BigQuery asset checks
uv run python -m unittest discover -s tests -t . -v
```

## Failure Scenarios — what happens when things break?

Every layer is idempotent, so the default recovery is **re-run the failed asset or
backfill the failed partition**. Nothing needs manual cleanup or a rollback.

| Failure | What happens | Recovery |
|---|---|---|
| GitHub 5xx / timeout | `RetryPolicy` retries 3× with exponential backoff + jitter. If it keeps failing, the raw asset fails and downstream assets never run. | Re-run the asset; it is safe. |
| GitHub 403 rate limit (60 req/hr) | Retries do not help (not transient). The run fails after the retry budget. | Wait for the limit to reset, add a token, or backfill a smaller range. |
| Short page / page cap hit | `extract_paginated` stops at an empty page or `max_pages`. The run still succeeds with whatever was fetched. | Raise `max_pages` and re-run; raw keys are timestamped, so nothing is overwritten. |
| GCS upload fails | That asset fails; downstream is blocked. A failed upload does not leave a usable object. | Re-run; the upload retries/overwrites. |
| Parquet conversion error (bad record) | The parquet asset fails; the raw JSON is already safe in GCS. | Fix the code and re-run. Because of `AssetIn`, re-running pulls upstream, so raw is re-fetched. |
| BigQuery load fails (schema, permissions, quota) | The load job fails on `job.result()`; the target partition is left unchanged because loads are atomic. | Fix the cause, re-run the partition. |
| Missing table/dataset | `ensure_table()` creates the table with partitioning/clustering on first load. | A permissions error is IAM, not code: fix IAM and re-run. |
| Scheduler run: upstream value not found (`KeyError`) | Happens if a run uses the multiprocess executor while the IO manager is in-memory. | Fixed by `in_process_executor` in `Definitions`; keep it. |
| Credentials expired/invalid | Auth error on the first cloud call; the run fails. | Refresh ADC (or re-mount it) and re-run. |
| Scheduler/daemon down | No runs fire; commits partitions go stale silently (there is no freshness check). | Restart the daemon and backfill the missing partitions. |
| Data-quality check fails (empty table, missing `dt`) | The asset still materialises, but the check is marked failed in the UI. Checks are non-blocking by default. | Investigate, fix, and backfill the affected partition. |

**Partial runs.** Dependencies stop downstream work, so a failure leaves the
upstream materialised and the downstream absent. Re-running the downstream pulls
the upstream chain again — correct, but it can repeat API calls, so prefer
backfilling the narrowest unit.

**What is *not* automatically handled.**
- A truncated extract (page cap) is not caught by a check.
- A stale partition (missed schedule) is not caught by a freshness check.
- Rate limits need credentials, not retries.

These are deliberate gaps with clear next steps: a token resource, a freshness
check, and CI.

