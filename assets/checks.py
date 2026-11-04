from datetime import datetime, time, timedelta, timezone

from dagster import AssetCheckResult, asset_check

from resources.bigquery import BigQueryResource

REPOSITORIES_TABLE = "repositories"
COMMITS_TABLE = "commits"

FRESHNESS_MAX_AGE_HOURS = 36


@asset_check(asset="github_repositories_bq", name="repositories_rows_present")
def repositories_rows_present(bigquery: BigQueryResource) -> AssetCheckResult:
    """Fail if the repositories table is empty."""
    table_id = bigquery.table_id(REPOSITORIES_TABLE)
    row = list(
        bigquery.get_client()
        .query(f"SELECT COUNT(*) AS n FROM `{table_id}`")
        .result()
    )[0]
    return AssetCheckResult(
        passed=row["n"] > 0,
        metadata={"rows": row["n"], "table": table_id},
    )


@asset_check(asset="github_commits_bq", name="commits_rows_present")
def commits_rows_present(bigquery: BigQueryResource) -> AssetCheckResult:
    """Fail if the commits table is empty or has rows missing their dt."""
    table_id = bigquery.table_id(COMMITS_TABLE)
    row = list(
        bigquery.get_client()
        .query(
            f"SELECT COUNT(*) AS n, COUNTIF(dt IS NULL) AS missing "
            f"FROM `{table_id}`"
        )
        .result()
    )[0]
    return AssetCheckResult(
        passed=row["n"] > 0 and row["missing"] == 0,
        metadata={"rows": row["n"], "missing_dt": row["missing"], "table": table_id},
    )


@asset_check(asset="github_commits_bq", name="commits_freshness")
def commits_freshness(bigquery: BigQueryResource) -> AssetCheckResult:
    """Fail if the newest commits partition is older than the freshness threshold.

    Partitions are daily, so freshness is measured from the end of the latest
    partition (its UTC midnight boundary); the expected maximum age is 36 hours.
    """
    table_id = bigquery.table_id(COMMITS_TABLE)
    row = list(
        bigquery.get_client()
        .query(f"SELECT MAX(dt) AS latest FROM `{table_id}`")
        .result()
    )[0]
    latest = row["latest"]

    if latest is None:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": "no partitions", "table": table_id},
        )

    partition_end = datetime.combine(
        latest + timedelta(days=1), time.min, tzinfo=timezone.utc
    )
    age_hours = (datetime.now(timezone.utc) - partition_end).total_seconds() / 3600

    return AssetCheckResult(
        passed=age_hours <= FRESHNESS_MAX_AGE_HOURS,
        metadata={
            "latest_partition": str(latest),
            "age_hours": round(age_hours, 1),
            "threshold_hours": FRESHNESS_MAX_AGE_HOURS,
            "table": table_id,
        },
    )
