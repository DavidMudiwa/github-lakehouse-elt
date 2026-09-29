from datetime import date

from dagster import (
    AssetExecutionContext,
    AssetIn,
    Config,
    MetadataValue,
    asset,
)

from common.keys import (
    COMMITS,
    REPOSITORIES,
    partition_date_from_key,
    processed_key,
)
from common.normalize import normalize_commit, normalize_repository
from common.parquet import records_to_parquet_bytes
from common.partitions import COMMITS_PARTITIONS
from common.policies import RETRY_POLICY
from common.schemas import COMMITS_SCHEMA, REPOSITORIES_SCHEMA, to_table_schema
from resources.gcs import GCSResource

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"


class CommitsProcessedConfig(Config):
    """Which repository's raw commits to convert to Parquet."""

    repo: str = "dagster-io/dagster"


@asset(
    ins={"raw_uri": AssetIn("github_repositories_raw")},
    kinds={"gcs", "parquet"},
    retry_policy=RETRY_POLICY,
)
def github_repositories_parquet(
    context: AssetExecutionContext,
    gcs: GCSResource,
    raw_uri: str,
) -> str:
    """Convert the raw repositories JSON into Parquet in GCS."""

    source_key = gcs.key_from_uri(raw_uri)
    raw_records = gcs.download_json(source_key)
    partition = partition_date_from_key(source_key)
    records = [normalize_repository(record, partition) for record in raw_records]

    target_key = processed_key(REPOSITORIES, partition)
    parquet_bytes = records_to_parquet_bytes(records)
    uri = gcs.upload_bytes(
        target_key, parquet_bytes, content_type=PARQUET_CONTENT_TYPE
    )

    context.add_output_metadata(
        {
            "records": MetadataValue.int(len(records)),
            "source_raw": MetadataValue.text(source_key),
            "gcs_uri": MetadataValue.url(uri),
            "size_bytes": MetadataValue.int(len(parquet_bytes)),
            "schema": to_table_schema(REPOSITORIES_SCHEMA),
        }
    )
    context.log.info(f"Converted {len(records)} repositories -> {uri}")

    return uri


@asset(
    ins={"raw_uri": AssetIn("github_commits_raw")},
    partitions_def=COMMITS_PARTITIONS,
    kinds={"gcs", "parquet"},
    retry_policy=RETRY_POLICY,
)
def github_commits_parquet(
    context: AssetExecutionContext,
    config: CommitsProcessedConfig,
    gcs: GCSResource,
    raw_uri: str,
) -> str:
    """Convert the raw commits JSON for a repo into Parquet in GCS."""

    source_key = gcs.key_from_uri(raw_uri)
    raw_records = gcs.download_json(source_key)
    partition = date.fromisoformat(context.partition_key)
    records = [
        normalize_commit(record, config.repo, partition) for record in raw_records
    ]

    target_key = processed_key(COMMITS, partition, repo=config.repo)
    parquet_bytes = records_to_parquet_bytes(records)
    uri = gcs.upload_bytes(
        target_key, parquet_bytes, content_type=PARQUET_CONTENT_TYPE
    )

    context.add_output_metadata(
        {
            "records": MetadataValue.int(len(records)),
            "repo": MetadataValue.text(config.repo),
            "partition": MetadataValue.text(context.partition_key),
            "source_raw": MetadataValue.text(source_key),
            "gcs_uri": MetadataValue.url(uri),
            "size_bytes": MetadataValue.int(len(parquet_bytes)),
            "schema": to_table_schema(COMMITS_SCHEMA),
        }
    )
    context.log.info(f"Converted {len(records)} commits -> {uri}")

    return uri
