import boto3

from pipeline.config import settings


def _session() -> boto3.Session:
    return boto3.Session(
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_REGION,
    )


def get_s3_client():
    return _session().client("s3")


def get_glue_client():
    return _session().client("glue")


def get_emr_client():
    return _session().client("emr")
