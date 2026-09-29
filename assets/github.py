from datetime import date, datetime, time, timedelta, timezone

import requests
from dagster import (
    AssetExecutionContext,
    Config,
    MetadataValue,
    asset,
)

from common.keys import COMMITS, REPOSITORIES, raw_key
from common.partitions import COMMITS_PARTITIONS
from common.policies import RETRY_POLICY
from resources.gcs import GCSResource


def extract_paginated(
    context: AssetExecutionContext,
    url: str,
    item_label: str,
    per_page: int,
    max_pages: int,
    extra_params: dict | None = None,
) -> list[dict]:
    """Fetch GitHub API pages until a page is empty or max_pages is reached."""
    items: list[dict] = []

    context.log.info(f"Starting {item_label} extraction")
    context.log.info(f"{item_label} per page: {per_page}")
    context.log.info(f"Maximum pages: {max_pages}")

    for page in range(1, max_pages + 1):
        context.log.info(f"Requesting GitHub {item_label} page {page}")

        params = {
            "page": page,
            "per_page": per_page,
            **(extra_params or {}),
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            page_data = response.json()
        except requests.RequestException as error:
            context.log.error(
                f"GitHub API request failed on {item_label} page {page}: {error}"
            )
            raise

        context.log.info(f"Page {page} returned {len(page_data)} {item_label}")

        if not page_data:
            context.log.info(f"Page {page} is empty. No more {item_label}.")
            break

        items.extend(page_data)

    return items


@asset(
    kinds={"github", "gcs"},
    retry_policy=RETRY_POLICY,
)
def github_repositories_raw(
    context: AssetExecutionContext,
    gcs: GCSResource,
) -> str:
    """Extract public repositories from GitHub and store the raw JSON in GCS."""

    repositories = extract_paginated(
        context=context,
        url="https://api.github.com/repositories",
        item_label="repositories",
        per_page=100,
        max_pages=3,
    )

    now = datetime.now(timezone.utc)
    key = raw_key(REPOSITORIES, now)

    uri = gcs.upload_json(key, repositories)

    context.add_output_metadata(
        {
            "records": MetadataValue.int(len(repositories)),
            "gcs_uri": MetadataValue.url(uri),
        }
    )

    context.log.info(f"Total repositories collected: {len(repositories)}")
    context.log.info(f"Raw data written to: {uri}")

    return uri


class CommitsConfig(Config):
    """Which repository's commits to extract, and how much."""

    repo: str = "dagster-io/dagster"
    per_page: int = 100
    max_pages: int = 3


@asset(
    partitions_def=COMMITS_PARTITIONS,
    kinds={"github", "gcs"},
    retry_policy=RETRY_POLICY,
)
def github_commits_raw(
    context: AssetExecutionContext,
    config: CommitsConfig,
    gcs: GCSResource,
) -> str:
    """Extract one day's commits from a GitHub repo and store raw JSON in GCS."""

    partition_date = date.fromisoformat(context.partition_key)
    since = datetime.combine(partition_date, time.min, tzinfo=timezone.utc)
    until = since + timedelta(days=1)

    commits = extract_paginated(
        context=context,
        url=f"https://api.github.com/repos/{config.repo}/commits",
        item_label="commits",
        per_page=config.per_page,
        max_pages=config.max_pages,
        extra_params={"since": since.isoformat(), "until": until.isoformat()},
    )

    now = datetime.now(timezone.utc)
    key = raw_key(COMMITS, now, repo=config.repo, partition_date=partition_date)

    uri = gcs.upload_json(key, commits)

    context.add_output_metadata(
        {
            "records": MetadataValue.int(len(commits)),
            "repo": MetadataValue.text(config.repo),
            "partition": MetadataValue.text(context.partition_key),
            "gcs_uri": MetadataValue.url(uri),
        }
    )

    context.log.info(f"Total commits collected: {len(commits)}")
    context.log.info(f"Raw data written to: {uri}")

    return uri