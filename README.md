# api-elt-platform

A reusable, asset-first ELT platform built with **Dagster**. Phase 2 ingests
data from the GitHub API, lands it in **Google Cloud Storage** as raw JSON and
processed Parquet, and loads it into **BigQuery**.

```
GitHub API ──▶ Dagster ──▶ GCS (raw JSON ──▶ processed Parquet) ──▶ BigQuery
```

## Architecture

Two independent asset chains share the same pattern
(`raw → parquet → bq`):

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
the real bytes live in GCS/BigQuery.

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

## Failure & recovery

All assets share a retry policy (3 retries, exponential backoff + jitter).
Recovery is safe because every layer is idempotent: raw appends, processed
overwrites a deterministic key, and BigQuery replaces a single `dt` partition.
Re-run the failed asset or backfill the failed partition.

## Roadmap

- **Phase 1** — GitHub API → local JSON.
- **Phase 2** — GitHub → Dagster → GCS → BigQuery (this repo).
- **Phase 3** — incremental ingestion with a time-based API (watermarks,
  backfills) and dbt for warehouse modelling.
