import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("GCS_BUCKET", "github-data-492709")
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "kestra-sandbox-492709")
os.environ.setdefault("BIGQUERY_DATASET", "github")

from main import defs  # noqa: E402


def main() -> None:
    specs = {spec.key.to_user_string(): spec for spec in defs.resolve_all_asset_specs()}

    print("Assets")
    for key in sorted(specs):
        spec = specs[key]
        upstream = sorted(dep.asset_key.to_user_string() for dep in spec.deps)
        kinds = ",".join(sorted(spec.kinds)) if spec.kinds else "-"
        partitions = (
            type(spec.partitions_def).__name__ if spec.partitions_def else "unpartitioned"
        )
        print(f"  {key}")
        print(f"    upstream   : {upstream or '-'}")
        print(f"    kinds      : {kinds}")
        print(f"    partitions : {partitions}")

    print("Checks")
    for check in defs.asset_checks or []:
        for key in check.check_keys:
            print(f"  {key.asset_key.to_user_string()}:{key.name}")

    print("Schedules")
    for schedule in defs.schedules or []:
        print(f"  {schedule.name}  cron={schedule.cron_schedule}  status={schedule.default_status}")

    print("Jobs")
    repository = defs.get_repository_def()
    for job in sorted(repository.get_all_jobs(), key=lambda job: job.name):
        executor = job.executor_def.name if job.executor_def else "default"
        print(f"  {job.name}  executor={executor}")

    print("Resources")
    for name in sorted(defs.resources):
        print(f"  {name}")


if __name__ == "__main__":
    main()
