# api-elt-platform — Interview Guide

A study companion for explaining this project. Read the pitch first, then the
file-by-file walkthrough, then the Q&A. The goal is that you can draw the
architecture on a whiteboard and justify every design decision.

---

## 1. The pitch

### 30 seconds
> "I built an asset-first ELT platform with Dagster that ingests GitHub data,
> lands it in Google Cloud Storage as raw JSON and processed Parquet, and loads
> it into BigQuery. It's partitioned by day for incremental loads, scheduled
> daily, idempotent by construction, with explicit schemas, IAM least-privilege,
> asset checks, and unit tests. The design separates the raw bronze layer from a
> typed silver layer and a warehouse layer."

### 2 minutes (the story)
> "The pipeline has two source chains: global repositories and daily commits.
> Each chain is `raw → parquet → bq`. Raw is the faithful, immutable JSON from
> the API — append-only, so it's replayable. Processed is Parquet with an
> explicit schema, written to a deterministic key so re-runs overwrite safely.
> The warehouse asset loads that Parquet into BigQuery with an explicit schema
> and a partition decorator, so a re-run replaces exactly one day's partition.
>
> Dagster carries the `gs://` URI from asset to asset through `AssetIn`, so
> lineage is declared in code, not implicit. Commits are daily-partitioned —
> the partition key becomes the data's `dt` and raw slicing uses GitHub's
> `since/until`, which makes backfills genuinely correct. Repositories has no
> date filter, so it stays an unpartitioned snapshot.
>
> On the operations side: a daily schedule requests yesterday's partition,
> every asset has retry with exponential backoff and jitter, asset checks fail
> loudly on empty or partial loads, and everything is idempotent so recovery is
> just re-running or backfilling. On the security side: no key files, a
> bucket-scoped and dataset-scoped service account, uniform bucket access, and
> lifecycle tiering to control storage cost."

---

## 2. Architecture at a glance

```
                          GitHub API
                              │
       ┌──────────────────────┴──────────────────────┐
       ▼                                             ▼
github_repositories_raw                       github_commits_raw
  (snapshot, JSON in GCS)                       (daily partition, JSON in GCS)
       │                                             │
       ▼                                             ▼
github_repositories_parquet                   github_commits_parquet
  (typed Parquet in GCS)                        (typed Parquet in GCS)
       │                                             │
       ▼                                             ▼
github_repositories_bq                        github_commits_bq
  (BigQuery snapshot table)                     (BigQuery dt-partitioned, clustered)
```

- **Medallion/lakehouse pattern:** raw = bronze, processed = silver, BigQuery = gold/serving.
- **Asset-first Dagster:** every node is an asset with declared dependencies,
  not a task in a DAG of steps.
- **Two chains, one pattern:** the same `raw → parquet → bq` shape, differing
  only in partitioning because the *sources* differ.

---

## 3. What happens in a single run (say the daily commits run)

1. **Schedule fires** (`commits_daily_schedule`, `0 9 * * *` UTC) and emits a
   `RunRequest(partition_key=yesterday, run_key=yesterday)`.
2. **`github_commits_raw`** receives the partition key, computes a `since/until`
   window for that day, pages the GitHub commits API, and uploads the raw JSON to
   `raw/commits/repo=dagster-io__dagster/dt=YYYY-MM-DD/commits_<ts>.json`. Returns
   the `gs://` URI.
3. **`github_commits_parquet`** receives that URI via `AssetIn`, downloads the
   JSON, projects each record onto the warehouse contract (types + `dt`), writes
   Parquet to `processed/.../dt=YYYY-MM-DD/commits.parquet` (deterministic key).
   Returns its URI.
4. **`github_commits_bq`** receives the Parquet URI, and runs a BigQuery load job
   into `commits$YYYYMMDD` with an explicit schema and `WRITE_TRUNCATE`.
5. **Asset checks** run and assert the table is non-empty with no missing `dt`.

Values between steps are tiny `gs://` strings carried by `InMemoryIOManager`;
the real bytes stay in GCS/BigQuery.

---

## 4. File-by-file walkthrough (techniques + why)

### `assets/github.py` — raw extraction
- **Technique:** a shared `extract_paginated(context, url, item_label, per_page,
  max_pages, extra_params)` helper does the HTTP paging once for both sources.
  Fetch → `raise_for_status()` → stop on an empty page or page cap.
- **Technique:** `CommitsConfig(Config)` makes the repo/paging **run-configurable**
  (`repo`, `per_page`, `max_pages`) with defaults — you can point it at another
  repo or cap pages without code changes.
- **Technique:** partition-driven slicing — `partition_date = date.fromisoformat(
  context.partition_key)`, then a `since`/`until` window passed as `extra_params`.
- **Technique:** keys built by shared helpers, not f-strings inline.
- **Why:** one reason to change per concern; the raw layer never interprets data.
- **Say:** "Extraction is stateless and idempotent — raw appends because the key
  embeds a run timestamp, so re-running never destroys history."

### `common/keys.py` — naming as executable convention
- **Technique:** pure functions `raw_key`, `processed_key`, `raw_prefix`,
  `processed_prefix`, `repo_partition`, `partition_date_from_key`.
- **Technique:** Hive-style partitions `dt=YYYY-MM-DD`, `repo=<owner>__<repo>`
  (slashes flattened so they don't create pseudo-directories).
- **Why:** the object key is a **data contract**; centralising it prevents drift
  across raw/processed/warehouse, and `dt=` becomes the BigQuery partition column.
- **Say:** "The naming convention is code, locked by tests, not a wiki page."

### `common/normalize.py` — data contract enforcement
- **Technique:** `normalize_repository` / `normalize_commit` **project** raw
  records onto an explicit shape, parse ISO-8601 timestamps into real datetimes,
  and inject `dt`.
- **Technique:** defensive `.get()`/`or {}` access for nested objects.
- **Why:** BigQuery rejects string→TIMESTAMP drift; typing happens once, here.
- **Say:** "Normalisation is the thin silver transformation — no business logic,
  just contract and typing."

### `common/parquet.py` — format conversion
- **Technique:** `pa.Table.from_pylist(records)` infers a schema and
  `pq.write_table(..., compression="snappy")`.
- **Why:** columnar + compressed is the right analytics hand-off format; raw JSON
  stays for fidelity/replay.
- **Evidence:** the 300-row repositories dataset drops from ~1.33 MB of JSON to
  ~14 KB of Parquet (~100×) once projected onto the 9-column contract.

### `common/schemas.py` — schema as code
- **Technique:** `REPOSITORIES_SCHEMA` / `COMMITS_SCHEMA` as typed column tuples,
  plus partition/cluster metadata, and `to_table_schema()` to render them as
  Dagster `TableSchema` metadata in the UI.
- **Technique:** honest contract — `/repositories` returns GitHub's *minimal*
  repo shape (no `created_at`/`updated_at`), so those columns were removed.
- **Say:** "Defining the schema surfaced a real mismatch with the source — the
  contract must describe data you can actually get."

### `common/partitions.py` — `DailyPartitionsDefinition(start_date="2026-09-01")`
- **Why:** the partition key *is* the watermark for an enumerable, sliceable source.

### `common/policies.py` — resilience
- **Technique:** a single `RetryPolicy(max_retries=3, delay=5,
  backoff=EXPONENTIAL, jitter=FULL)` shared by all assets.
- **Why:** backoff spacing avoids synchronized retries; jitter spreads load.
  Note: a 403 rate-limit is *not* transient, so retries won't fix it.

### `resources/gcs.py` — the GCS resource
- **Technique:** Dagster `ConfigurableResource` with `bucket`/`project` from
  `EnvVar`, a **lazily cached client** (`PrivateAttr`), and high-level helpers
  `upload_bytes/upload_json/download_*`, `object_uri`, `key_from_uri`, `list_keys`.
- **Why:** infrastructure lives in one place; assets stay declarative; config is
  environment-driven so the same code runs locally and in a container.

### `resources/bigquery.py` — the BigQuery resource
- **Technique:** `ConfigurableResource`; `ensure_table()` creates the table with
  `TimePartitioning(field="dt")` and `clustering_fields`; `load_parquet()` runs a
  load job to `table$YYYYMMDD` with `SourceFormat.PARQUET`, explicit schema, and
  `WRITE_TRUNCATE`, then `job.result()`.
- **Why:** partition-scoped truncate makes re-runs idempotent and non-destructive
  to other days.

### `assets/processed.py` and `assets/bigquery.py` — the chain
- **Technique:** dependencies via `@asset(ins={"raw_uri": AssetIn("...")})` and
  `ins={"parquet_uri": AssetIn("...")}` — downstream consumes the upstream's
  returned URI (consumer-driven lineage).
- **Technique:** typed output metadata (`MetadataValue.int/url/text/json`) plus a
  `TableSchema`, and `kinds={"gcs","parquet","bigquery"}` badges.
- **Why:** the graph is derived from code; the UI shows lineage and per-run facts.

### `assets/checks.py` — data quality
- **Technique:** `@asset_check` on each warehouse asset: row count > 0, and for
  commits, zero rows missing `dt`.
- **Why:** catches "load succeeded but wrote nothing" and partial loads.

### `schedules/commits.py` — orchestration
- **Technique:** `define_asset_job` over the commits chain + `@schedule` yielding
  `RunRequest(partition_key=yesterday, run_key=yesterday)`.
- **Why `run_key`:** dedupes one run per partition across scheduler recoveries.
- **Why STOPPED by default:** schedules don't fire until armed; the daemon must
  run.

### `main.py` — composition
```python
Definitions(
    assets=[...6...], schedules=[commits_daily_schedule],
    asset_checks=[...], resources={"gcs":..., "bigquery":..., "io_manager": InMemoryIOManager()},
)
```
- **Technique:** `InMemoryIOManager` because we only pass URIs between assets.

### `infra/gcs-lifecycle.json` + bucket config
- **Technique:** lifecycle tiering `STANDARD → NEARLINE@30d → COLDLINE@90d →
  ARCHIVE@365d` scoped to `raw/`; `processed/` stays STANDARD; uniform
  bucket-level access; public access prevention; versioning off.
- **Why:** cost policy matched to access pattern, permissions via IAM only.

### `tests/` and `scripts/`
- **Technique:** stdlib `unittest` for pure logic (keys, normalisation) — no
  framework dependency; standalone scripts validate `Definitions`, print the
  graph, run checks, and backfill — sidestepping the `.env`/DAGSTER_HOME CLI
  conflicts.
- **Why:** fast feedback and reproducibility without spinning up the orchestrator.

---

## 5. Core techniques cheat-sheet

| Technique | What it does | Why it matters |
|---|---|---|
| Asset-first Dagster | Assets + declared deps, not imperative tasks | Lineage, ordering, observability, idempotent re-runs |
| `AssetIn` dependency | Downstream takes upstream's returned URI | Consumer-driven lineage; graph inferred from code |
| Resource vs IO manager | Resource = external system; IO manager = how outputs move between assets | Clean separation of concerns |
| Medallion layers | raw (bronze) → processed (silver) → BigQuery (gold) | Fidelity + replay vs analytics-ready |
| Idempotency by construction | Deterministic processed key; `table$YYYYMMDD` + `WRITE_TRUNCATE` | Safe re-runs/backfills, no duplicates |
| Hive partition keys in the path | `dt=`, `repo=` | Becomes BQ partition/cluster; portable convention |
| Partition-driven extraction | `since/until` from partition key | Genuine daily slices; correct backfills |
| Explicit schema contract | Typed columns in `schemas.py`, applied at load | Fails loudly on drift; BQ rejects string→timestamp |
| Parquet | Columnar + snappy | ~100× smaller, cheap to scan |
| Batch load jobs | Parquet from GCS → BigQuery | Serverless, no per-row inserts |
| `ConfigurableResource` + `EnvVar` | Env-driven infra config | Same code local + container; no hardcoded secrets |
| Retry + backoff + jitter | 3 retries, exponential, full jitter | Transient-failure resilience without thundering herd |
| Asset checks | Non-empty, no missing `dt` | Detects silent/partial failures |
| IAM least privilege | Bucket-scoped storage, dataset-scoped BigQuery, project jobUser | Blast-radius reduction |
| Lifecycle policies | Tier cold `raw/`, keep `processed/` hot | Cost control matched to access |
| Immutable raw + deterministic processed | Append JSON, overwrite Parquet | Audit/replay vs idempotence |

---

## 6. Numbers and evidence to quote

- Two chains, **6 assets**, **2 resources**, **1 schedule**, **2 asset checks**,
  **11 unit tests**.
- Repositories: 300 records; commits: a genuine day slice is only a handful
  (e.g. 9 rows for 2026-09-21) — a backfill *corrected* a previously
  mis-labelled 100-row load.
- Compression: 300-row repositories JSON ~1.33 MB → Parquet ~14 KB after
  projection (~100×).
- Backfill cost: ~1 request per partition (+ pagination), against a **60
  requests/hour** unauthenticated limit.
- BigQuery `commits` held three partitions (`2026-09-21`, `-22`, `-23`) with old
  partitions preserved after a new load.

---

## 7. Trade-offs you chose (and can defend)

- **Raw is append-only, not strictly idempotent.** It's "safe" (never destroys
  history, replayable) but produces a new object per run. Strict idempotency is
  enforced in the processed/warehouse layers instead — a deliberate bronze/silver
  split.
- **Repositories is a snapshot, not partitioned.** `/repositories` has no date
  filter; daily partitions would mislabel data. Partitioning only where the
  source is sliceable.
- **User ADC in the container, not a key.** Org policy forbids SA keys; the SA +
  bucket/dataset IAM exists for a future keyless path (impersonation / Workload
  Identity).
- **No custom IO manager to GCS.** The assets already persist to GCS/BigQuery;
  the IO manager only ferries URIs, so in-memory is right. A custom GCS IO
  manager would only matter if assets passed *dataframes* between steps.
- **Table-level checks, not per-partition.** Per-partition asset checks are a
  preview feature; you chose stable APIs.

---

## 8. If I had more time / how I'd extend it

- **Authenticate to GitHub** with a token resource (5,000 req/hr) to enable real
  backfills.
- **Multi-repo fan-out**: iterate the repositories asset to fetch many repos'
  commits (dynamic partitions / a partitioned asset keyed by repo).
- **Watermarks** for a high-frequency, non-enumerable source (Phase 3, with a
  time-based API).
- **Data-quality depth**: dbt tests / freshness checks; per-partition checks when
  they leave preview.
- **CI**: run unit tests + `validate_defs` on every PR; `ruff`/mypy.
- **Alerting**: schedule/sensor failure hooks to Slack/email.
- **Infra as code**: Terraform/`gcloud` scripts for the bucket, dataset, and IAM
  so provisioning is reproducible.

---

## 9. Likely interview questions

**Q: Why Dagster instead of Airflow?**
> Dagster is asset-first: I declare data assets and their dependencies, and it
> infers lineage, ordering, and materialisation state. It also has first-class
> partitions, resources, and asset checks. Airflow is task-centric — you model
> the DAG of operators; lineage is less direct.

**Q: Resource vs IO manager?**
> A resource is an external system an asset *uses* (my GCS and BigQuery handles).
> An IO manager controls how an asset's *output* is stored and loaded between
> steps. I use `InMemoryIOManager` because I only pass `gs://` URIs; the real
> bytes live in GCS/BigQuery.

**Q: What is idempotency and how did you achieve it?**
> Re-running yields the same end state. Processed uses a deterministic key so it
> overwrites; BigQuery loads target `table$YYYYMMDD` with `WRITE_TRUNCATE`, so a
> re-run replaces one partition without duplicates. Raw is append-only, so it's
> "safe" rather than strictly idempotent.

**Q: Why Parquet?**
> Columnar, typed, compressed, predicate-friendly — ideal for analytics and
> warehouse loads. Raw JSON is kept for fidelity and replay.

**Q: How do backfills work?**
> A backfill is many partition-scoped runs. The commits asset slices a day via
> `since/until`, and the load replaces that day's partition, so re-running any
> day is correct and cheap. I also capped in-process backfills to respect the
> API rate limit.

**Q: How do you handle schema drift?**
> The contract lives in `schemas.py` and is applied at load with an explicit
> schema; a mismatch fails the load instead of silently mistyping. Normalisation
> is also typed and unit-tested.

**Q: How would you onboard a new source?**
> Add a raw asset (with a `Config` for source params), a normaliser + schema
> entry, a processed asset, and a load asset, wire them with `AssetIn`, and
> register in `Definitions`. Partitions only if the source is date-sliceable.

**Q: Late-arriving data?**
> For partition-based ingestion, you backfill the affected (already-closed)
> partition. Watermarks handle it differently in streaming; a watermark alone
> doesn't catch back-dated changes.

**Q: Least privilege / security?**
> No key files; a service account scoped to one bucket and one dataset, plus
> project-level jobUser. Uniform bucket-level access, public access prevention,
> credentials gitignored and dockerignored, and lifecycle rules for cost.

**Q: What was the trickiest bug?**
> A named Docker volume shadowing the image's virtualenv — adding dependencies
> didn't take effect until the volume was synced/recreated. Also, `EnvVar`
> defaults are resolved by Dagster's config system, not pydantic, so direct
> instantiation in scripts needs explicit values.

**Q: How do you test?**
> Unit tests for pure logic (key conventions, normalisers) with stdlib
> `unittest`, plus standalone scripts that validate `Definitions`, run checks,
> print the graph, and exercise the resources against real GCS/BigQuery.

**Q: How do you control cost?**
> Snappy Parquet (compact), batch load jobs (free), partition pruning for
> queries, clustering on the column we filter by, and lifecycle tiering that
> only cools the never-read `raw/` prefix.

---

## 10. Whiteboard script

1. Draw GitHub in the middle-left, GCS in the centre, BigQuery on the right.
2. Add two rows: repositories and commits, each `raw → parquet → bq`.
3. Annotate: raw = append-only JSON (`dt=`/`repo=` paths); processed =
   deterministic Parquet; bq = explicit schema, `dt`-partitioned, partition
   decorator + truncate.
4. Draw arrows labelled `gs://` URI between assets (AssetIn + IO manager).
5. Circle commits and label "daily partition, since/until, backfillable".
6. Add a clock icon for the schedule (yesterday's partition) and a shield for
   IAM/lifecycle.

---

## 11. Glossary

- **Asset** — a persisted data object Dagster knows how to materialise.
- **AssetIn** — declares an upstream dependency by consuming its output.
- **Materialization** — a run that produces an asset's current value.
- **Resource** — an injectable handle to an external system.
- **IO manager** — moves asset outputs to their load location.
- **Partition** — a named slice of an asset (here, a day).
- **Backfill** — materialising many partitions.
- **Idempotent** — re-running produces the same result.
- **Watermark / high-water mark** — stored marker of how far ingestion has run.
- **Medallion (bronze/silver/gold)** — raw → cleaned → modelled layers.
- **Partition decorator (`table$YYYYMMDD`)** — targets one BigQuery partition.
