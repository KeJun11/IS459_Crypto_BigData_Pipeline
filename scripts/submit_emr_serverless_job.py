from __future__ import annotations

import argparse
import json

import boto3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Submit a Spark batch job to EMR Serverless."
    )
    parser.add_argument("--application-id", required=True)
    parser.add_argument("--execution-role-arn", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--entry-point", required=True, help="S3 URI to the Spark job entry point.")
    parser.add_argument(
        "--entry-point-argument",
        action="append",
        default=[],
        help="Repeatable Spark entry-point arguments passed through to the job.",
    )
    parser.add_argument(
        "--spark-submit-parameters",
        default="--conf spark.executor.memory=2g --conf spark.driver.memory=1g",
        help="Raw spark-submit parameters for EMR Serverless.",
    )
    parser.add_argument(
        "--py-files",
        action="append",
        default=[],
        help="Repeatable S3 URI passed through as --py-files for Spark import dependencies.",
    )
    parser.add_argument(
        "--log-uri",
        default="",
        help="Optional S3 URI for EMR Serverless job logs.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = boto3.client("emr-serverless", region_name=args.region)

    request: dict = {
        "applicationId": args.application_id,
        "executionRoleArn": args.execution_role_arn,
        "name": args.job_name,
        "jobDriver": {
            "sparkSubmit": {
                "entryPoint": args.entry_point,
                "entryPointArguments": args.entry_point_argument,
                "sparkSubmitParameters": (
                    f"{args.spark_submit_parameters} --py-files {','.join(args.py_files)}".strip()
                    if args.py_files
                    else args.spark_submit_parameters
                ),
            }
        },
    }

    if args.log_uri:
        request["configurationOverrides"] = {
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": args.log_uri,
                }
            }
        }

    response = client.start_job_run(**request)
    print(json.dumps(response, indent=2, default=str))


if __name__ == "__main__":
    main()
