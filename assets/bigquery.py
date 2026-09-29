from datetime import date

from dagster import (
    AssetExecutionContext,
    AssetIn,
    MetadataValue,
    asset,
)

from common.keys import COMMITS, REPOSITORIES, partition_date_from_key
from common.partitions import COMMITS_PARTITIONS
from common.policies import RETRY_POLICY
from common.schemas import (
    COMMITS_CLUSTERING_FIELDS,
    COMMITS_PARTITION_FIELD,
    COMMITS_SCHEMA,
    REPOSITORIES_PARTITION_FIELD,
    REPOSITORIES_SCHEMA,
    to_table_schema,
)
from resources.bigquery import BigQueryResource
from resources.gcs import GCSResource


@asset(
    ins={"parquet_uri": AssetIn("github_repositories_parquet")},
    kinds={"bigquery"},
    retry_policy=RETRY_POLICY,
)
def github_repositories_bq(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
    parquet_uri: str,
) -> str:
    """Load the processed repositories Parquet into BigQuery."""

    partition = partition_date_from_key(gcs.key_from_uri(parquet_uri))

    rows = bigquery.load_parquet(
        table=REPOSITORIES,
        source_uri=parquet_uri,
        schema=REPOSITORIES_SCHEMA,
        partition_field=REPOSITORIES_PARTITION_FIELD,
        partition_date=partition,
    )

    table_id = bigquery.table_id(REPOSITORIES)
    context.add_output_metadata(
        {
            "rows": MetadataValue.int(rows),
            "table": MetadataValue.text(table_id),
            "source_parquet": MetadataValue.url(parquet_uri),
            "partition_field": MetadataValue.text(REPOSITORIES_PARTITION_FIELD),
            "schema": to_table_schema(REPOSITORIES_SCHEMA),
        }
    )
    context.log.info(f"Loaded {rows} rows into {table_id}")

    return table_id


@asset(
    ins={"parquet_uri": AssetIn("github_commits_parquet")},
    partitions_def=COMMITS_PARTITIONS,
    kinds={"bigquery"},
    retry_policy=RETRY_POLICY,
)
def github_commits_bq(
    context: AssetExecutionContext,
    gcs: GCSResource,
    bigquery: BigQueryResource,
    parquet_uri: str,
) -> str:
    """Load the processed commits Parquet into BigQuery."""

    partition = date.fromisoformat(context.partition_key)

    rows = bigquery.load_parquet(
        table=COMMITS,
        source_uri=parquet_uri,
        schema=COMMITS_SCHEMA,
        partition_field=COMMITS_PARTITION_FIELD,
        partition_date=partition,
        clustering_fields=COMMITS_CLUSTERING_FIELDS,
    )

    table_id = bigquery.table_id(COMMITS)
    context.add_output_metadata(
        {
            "rows": MetadataValue.int(rows),
            "table": MetadataValue.text(table_id),
            "partition": MetadataValue.text(context.partition_key),
            "source_parquet": MetadataValue.url(parquet_uri),
            "partition_field": MetadataValue.text(COMMITS_PARTITION_FIELD),
            "clustering_fields": MetadataValue.json(COMMITS_CLUSTERING_FIELDS),
            "schema": to_table_schema(COMMITS_SCHEMA),
        }
    )
    context.log.info(f"Loaded {rows} rows into {table_id}")

    return table_id
