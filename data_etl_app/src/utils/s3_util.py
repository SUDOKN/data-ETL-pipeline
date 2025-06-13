import boto3
import os
from mypy_boto3_s3.client import S3Client

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY or not AWS_REGION:
    raise ValueError(
        "AWS credentials or region are not set. Please set them in your .env file."
    )


def read_s3_file(bucket: str, key: str) -> str:
    """
    Read a file from an AWS S3 bucket and return its content as a string.
    Credentials are loaded from the .env file.

    Example usage:
    content = read_s3_file("sudokn-ontology", "SUDOKN.rdf")
    """
    s3: S3Client = boto3.client(  # type: ignore
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")
