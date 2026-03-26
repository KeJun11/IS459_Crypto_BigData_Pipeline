from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Kaggle CSV to Bronze parquet smoke test inside the Spark container."
    )
    parser.add_argument(
        "--container",
        default="crypto-spark-master",
        help="Spark container used to run the smoke test.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    command = [
        "docker",
        "exec",
        args.container,
        "spark-submit",
        "/opt/project/src/jobs/test_kaggle_csv_to_bronze_workflow.py",
    ]
    result = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode != 0:
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
