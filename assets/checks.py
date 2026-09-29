from dagster import AssetCheckResult, asset_check

from resources.bigquery import BigQueryResource

REPOSITORIES_TABLE = "repositories"
COMMITS_TABLE = "commits"


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
