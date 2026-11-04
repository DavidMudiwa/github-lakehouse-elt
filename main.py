from dagster import Definitions, InMemoryIOManager, in_process_executor

from assets.bigquery import github_commits_bq, github_repositories_bq
from assets.checks import (
    commits_freshness,
    commits_rows_present,
    repositories_rows_present,
)
from assets.github import github_commits_raw, github_repositories_raw
from assets.processed import github_commits_parquet, github_repositories_parquet
from resources.bigquery import BigQueryResource
from resources.gcs import GCSResource
from schedules.commits import commits_daily_schedule
from sensors.alerts import email_on_run_failure

defs = Definitions(
    assets=[
        github_repositories_raw,
        github_commits_raw,
        github_repositories_parquet,
        github_commits_parquet,
        github_repositories_bq,
        github_commits_bq,
    ],
    schedules=[commits_daily_schedule],
    sensors=[email_on_run_failure],
    asset_checks=[
        repositories_rows_present,
        commits_rows_present,
        commits_freshness,
    ],
    executor=in_process_executor,
    resources={
        "gcs": GCSResource(),
        "bigquery": BigQueryResource(),
        "io_manager": InMemoryIOManager(),
    },
)
