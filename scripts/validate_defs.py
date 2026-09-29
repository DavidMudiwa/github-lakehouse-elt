import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("GCS_BUCKET", "github-data-492709")
os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "kestra-sandbox-492709")

from main import defs  # noqa: E402


def main() -> None:
    assets = sorted(spec.key.to_user_string() for spec in defs.resolve_all_asset_specs())
    schedules = sorted(schedule.name for schedule in defs.schedules or [])
    checks = sorted(
        f"{key.asset_key.to_user_string()}:{key.name}"
        for check in defs.asset_checks or []
        for key in check.check_keys
    )
    print("assets    :", assets)
    print("resources :", list(defs.resources.keys()))
    print("schedules :", schedules)
    print("checks    :", checks)
    print("definitions loaded OK")


if __name__ == "__main__":
    main()
