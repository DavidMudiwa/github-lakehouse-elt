import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

PROJECT = "kestra-sandbox-492709"
DATASET = "github"

adc = REPO_ROOT / "secrets" / "adc.json"
if adc.exists():
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(adc))

from dagster import build_asset_check_context  # noqa: E402

from assets.checks import commits_rows_present, repositories_rows_present  # noqa: E402
from resources.bigquery import BigQueryResource  # noqa: E402


def main() -> None:
    resources = {"bigquery": BigQueryResource(project=PROJECT, dataset=DATASET)}
    context = build_asset_check_context(resources=resources)

    for name, check in (
        ("repositories_rows_present", repositories_rows_present),
        ("commits_rows_present", commits_rows_present),
    ):
        result = check(context)
        print(f"{name}: passed={result.passed} metadata={result.metadata}")


if __name__ == "__main__":
    main()
