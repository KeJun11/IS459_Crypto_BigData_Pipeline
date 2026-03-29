from __future__ import annotations

import argparse
import os
import subprocess
import sys

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional for plain Python environments
    def load_dotenv() -> bool:
        return False


load_dotenv()


DEFAULT_DOCKER_CONTAINER = "crypto-spark-master"
DEFAULT_SPARK_MASTER = "spark://spark-master:7077"
DEFAULT_STREAM_SCRIPT = "/opt/project/src/jobs/stream_to_silver.py"


def env_or_default(name: str, default: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


def parse_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description="Run the stream_to_silver Spark consumer inside the Spark master container."
    )
    parser.add_argument(
        "--docker-container",
        default=env_or_default("STREAM_SPARK_CONTAINER", DEFAULT_DOCKER_CONTAINER),
        help="Docker container that has spark-submit available.",
    )
    parser.add_argument(
        "--master",
        default=env_or_default("STREAM_SPARK_MASTER", DEFAULT_SPARK_MASTER),
        help="Spark master URL passed to spark-submit.",
    )
    parser.add_argument(
        "--packages",
        default=os.getenv("STREAM_SPARK_PACKAGES", ""),
        help="Optional Maven package coordinates injected into spark-submit.",
    )
    parser.add_argument(
        "--jars",
        default=os.getenv("STREAM_EXTRA_JARS", ""),
        help="Optional comma-separated jar paths or URIs injected into spark-submit.",
    )
    parser.add_argument(
        "--stream-script",
        default=env_or_default("STREAM_SPARK_SCRIPT", DEFAULT_STREAM_SCRIPT),
        help="Path to the stream_to_silver.py script inside the Spark container.",
    )
    parser.add_argument(
        "--print-command",
        action="store_true",
        help="Print the generated docker exec command before running it.",
    )
    return parser.parse_known_args()


def build_command(args: argparse.Namespace, stream_args: list[str]) -> list[str]:
    command = [
        "docker",
        "exec",
        "-i",
        args.docker_container,
        "spark-submit",
        "--master",
        args.master,
    ]
    if args.packages:
        command.extend(["--packages", args.packages])
    if args.jars:
        command.extend(["--jars", args.jars])
    command.append(args.stream_script)
    command.extend(stream_args)
    return command


def main() -> None:
    args, stream_args = parse_args()
    command = build_command(args, stream_args)

    if args.print_command:
        print(" ".join(command))

    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        raise SystemExit(completed.returncode)


if __name__ == "__main__":
    main()
