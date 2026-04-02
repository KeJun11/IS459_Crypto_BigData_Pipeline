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

def get_emr_cluster_id(cluster_name = settings.EMR_CLUSTER_NAME):
    client = get_emr_client()
    
    # List clusters (you can filter by state if needed)
    paginator = client.get_paginator('list_clusters')
    
    for page in paginator.paginate(ClusterStates=['RUNNING', 'WAITING', 'STARTING']):
        for cluster in page.get('Clusters', []):
            if cluster['Name'] == cluster_name:
                return cluster['Id']
    
    raise ValueError(f"EMR cluster '{cluster_name}' not found in RUNNING/WAITING/STARTING state")