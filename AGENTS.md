# AGENTS.md — api-platform

## Working agreement (learning-first)

This is a learning project. Optimize for my understanding, not for speed.

- **Teach alongside every change.** For each major step explain the concept, the
  architecture, and *why* a decision was made before/while showing code.
- **Stage by stage.** Do not jump ahead or implement future stages. Work the
  phases in order (see Phase 2 plan) and stop for confirmation between stages.
- **I want to contribute.** Handle repetitive/boilerplate work, but leave the
  meaningful decisions, small edits, and reasoning to me. Ask me to make the
  interesting calls rather than doing everything.
- **No copy-paste black boxes.** After each stage I should be able to explain
  the result to someone else, modify it, and extend it.
- **Flag trade-offs and gotchas** explicitly (idempotency, cost, least
  privilege, failure modes).

## Project

Dagster ELT platform. Phase 1 = GitHub API → local JSON.
Phase 2 = GitHub → Dagster → GCS → BigQuery (asset chain: raw → parquet → bq).

## Asset dependencies

- `raw → parquet → bq` is wired via `AssetIn` (downstream takes the upstream's
  returned `gs://` URI as an argument), so Dagster infers lineage and ordering.
- Values pass between assets via `InMemoryIOManager` (we only shuttle URIs; real
  bytes live in GCS/BigQuery). Registered as `io_manager` in `Definitions`.
- Consequence: materializing a downstream asset pulls its upstream chain. A
  standalone `materialize([...])` must include the full subgraph (see runners).
- Assets carry `kinds` badges and rich output metadata: typed values
  (`MetadataValue.int/url/text/json`) plus a `TableSchema` rendered from
  `common/schemas.py` via `to_table_schema`.

## Partitions

- `github_commits_*` are daily-partitioned (`common/partitions.py`, start
  2026-09-01 UTC). `github_repositories_*` stay unpartitioned snapshots (the
  `/repositories` endpoint has no date filter, so daily partitions would be fake).
- The Dagster partition key becomes the data's `dt` (GCS path + BigQuery
  partition); commits raw slices the day via GitHub `?since=&until=`.
- Materialize one partition: `PARTITION=2026-09-22 uv run python scripts/run_bigquery_assets.py`
  (or `run_commits_asset.py` / `run_parquet_assets.py`).
- Backfills cost ~1 request per partition (+pagination). Unauthenticated GitHub
  is 60 req/hr, so backfill small ranges until a `GITHUB_TOKEN` resource exists.

## Failure & recovery

- All assets share `common/policies.py::RETRY_POLICY`: 3 retries, 5s base delay,
  exponential backoff + full jitter (spreads load, helps transient 5xx).
- Recovery is safe because everything is idempotent: raw appends, processed
  overwrites a deterministic key, bq replaces a single `dt` partition. Re-run the
  failed asset or backfill the failed partition.
- Asset checks (`assets/checks.py`) assert the BigQuery tables are non-empty and
  have no rows missing `dt`. Run standalone via `scripts/run_checks.py`.

## Schedules

- `schedules/commits.py` defines `commits_daily_schedule` (cron `0 9 * * *`,
  UTC) which requests **yesterday's** commits partition via `RunRequest`.
  Default status is STOPPED — enable it in the UI to arm it; the container's
  `SchedulerDaemon` must be running for it to fire.
- `commits_job` is an explicit asset job over the commits chain (raw→parquet→bq).

## Data contracts

- `common/schemas.py` holds explicit BigQuery column types plus partition/cluster fields.
- `common/normalize.py` projects raw records onto that contract (adds `dt`, parses ISO timestamps).
- repositories come from `/repositories`, which returns GitHub's **minimal** repo
  representation — no `created_at`/`updated_at`, so those were dropped from the contract.
- Both tables partition by `dt`; `commits` clusters on `repository`.

## GCS object naming

- raw (immutable, append-only):
  `raw/<dataset>/[repo=<owner>__<repo>/]dt=YYYY-MM-DD/<dataset>_<UTCtimestamp>.json`
- processed (deterministic, overwrite-safe/idempotent):
  `processed/<dataset>/[repo=<owner>__<repo>/]dt=YYYY-MM-DD/<dataset>.parquet`
- Slashes in partition values are flattened (`dagster-io/dagster` → `dagster-io__dagster`).
- `dt=` becomes the BigQuery partition column; `repo=` anticipates multi-repo fan-out.
- Convention is executable in `common/keys.py` and locked by `tests/test_keys.py`.

## Environment facts

- Code lives in WSL (`Ubuntu`) at `/home/david/api-platform`; Docker daemon is in WSL.
- `gcloud` runs from **Windows** (account `davidmudiwa13@gmail.com`). Running it
  from WSL sees a different config, so do all provisioning from Windows.
- GCP project: `kestra-sandbox-492709`, region `us-central1`.
- Org policy `iam.disableServiceAccountKeyCreation` blocks SA JSON keys.
  Container authenticates with **user ADC** copied to `secrets/adc.json`
  (gitignored + dockerignored), via `GOOGLE_APPLICATION_CREDENTIALS` in `.env`.
- Service account `api-platform-dev@kestra-sandbox-492709.iam.gserviceaccount.com`
  reserved for a future keyless production path. Roles are least-privilege:
  `roles/bigquery.jobUser` at project level; `roles/bigquery.dataEditor` scoped
  to dataset `github`; `roles/storage.objectAdmin` scoped to the GCS bucket.
  Dataset access managed via `scripts/set_dataset_iam.py`.
- BigQuery dataset `github` (location `us-central1`, no default table
  expiration). Tables: `repositories`, `commits`; both partitioned by `dt`,
  `commits` also clustered on `repository`. Access via `BigQueryResource`
  (project/dataset from env: `GOOGLE_CLOUD_PROJECT`, `BIGQUERY_DATASET`).
  Loads target a partition decorator (`table$YYYYMMDD`, `WRITE_TRUNCATE`) so
  re-runs replace only that partition (idempotent).
- Phase 2 GCS bucket: `gs://github-data-492709` (region us-central1, uniform
  bucket-level access, public access prevention, lifecycle
  raw/ only: STANDARD→NEARLINE@30d→COLDLINE@90d→ARCHIVE@365d; processed/ stays
  STANDARD, versioning off).
  Config kept in `infra/gcs-lifecycle.json`.
- SA is granted `roles/storage.objectAdmin` **bucket-scoped**; user
  `davidmudiwa13@gmail.com` has `roles/iam.serviceAccountTokenCreator` on the SA
  (enables keyless impersonation).
- Pre-existing resources (unrelated): bucket `ecommerce_product_files`,
  BQ datasets `products_raw`, `takealot_raw`.
- Named volume `elt_platform_venv` mounts at `/app/.venv` and shadows image
  rebuilds: after changing deps, an image rebuild alone is NOT enough. Sync the
  container venv in place: `docker compose exec -T elt-platform uv sync --frozen`
  (then `docker compose restart elt-platform`), or recreate the volume with
  `docker compose down -v && docker compose up --build`.
- Dagster auto-loads `.env` and its values **override** the process env, so the
  container `DAGSTER_HOME=/app/.dagster` breaks local CLI runs (`dagster
  definitions validate`, `dagster dev`). Use the Python import checks below, or
  run inside Docker.
- `EnvVar` defaults are resolved by Dagster's config system, not by pydantic:
  instantiating e.g. `GCSResource()` directly in plain Python leaves the field
  as the literal env-var name. Pass explicit values in standalone scripts.

## Commands

- Smoke-test cloud auth: `uv run python scripts/verify_gcp.py`
- Smoke-test the GCS resource round-trip: `uv run python scripts/check_gcs_resource.py`
- Load/validate Definitions without the Dagster CLI: `uv run python scripts/validate_defs.py`
- Check what partition the commits schedule would request: `uv run python scripts/check_schedule.py`
- Run the BigQuery asset checks standalone: `uv run python scripts/run_checks.py`
- Print the full asset graph (lineage, kinds, partitions, checks, schedules): `uv run python scripts/describe_graph.py`
- Materialize the repositories asset locally (hits GitHub + GCS): `uv run python scripts/run_repositories_asset.py`
- Materialize the commits asset locally; override with `PARTITION=YYYY-MM-DD` / `MAX_PAGES=N`: `uv run python scripts/run_commits_asset.py`
- Materialize parquet assets locally (pulls raw upstream → hits GitHub); commits partition via `PARTITION=YYYY-MM-DD`: `uv run python scripts/run_parquet_assets.py`
- Materialize the BigQuery assets locally (pulls the whole chain → hits GitHub); commits partition via `PARTITION=YYYY-MM-DD`: `uv run python scripts/run_bigquery_assets.py`
- Backfill a commits partition range in-process (capped at 5): `BACKFILL_START=2026-09-21 BACKFILL_END=2026-09-22 uv run python scripts/backfill_commits.py`
- Run unit tests: `uv run python -m unittest discover -s tests -t . -v`
- Add deps: `uv add <pkg>` (update `uv.lock`; Docker build uses `uv sync --frozen`)

## Layout

- `assets/github.py` — raw assets (GitHub API → GCS raw JSON)
- `assets/processed.py` — processed assets (raw JSON → Parquet in GCS)
- `assets/bigquery.py` — load assets (GCS Parquet → BigQuery)
- `assets/checks.py` — asset checks; `common/policies.py` — shared retry policy
- `resources/` — injectable external-system handles (`GCSResource`, `BigQueryResource`)
- `common/keys.py` — GCS key/naming convention
- `common/normalize.py` — raw record → warehouse contract (adds `dt`, parses timestamps)
- `common/parquet.py` — JSON→Parquet; `common/schemas.py` — explicit BQ schemas
- `common/partitions.py` — Dagster partitions; `schedules/commits.py` — daily schedule/job
- `infra/` — cloud config as code (e.g. `gcs-lifecycle.json`)
- `scripts/` — standalone verification/smoke-test helpers
- `tests/` — stdlib `unittest` suite
- `secrets/` — gitignored + dockerignored credentials (ADC)
