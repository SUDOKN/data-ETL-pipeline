import boto3
import os
from dotenv import load_dotenv
from mypy_boto3_s3.client import S3Client

# Load environment variables from .env at startup
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))


def read_s3_file(bucket: str, key: str) -> str:
    """
    Read a file from an AWS S3 bucket and return its content as a string.
    Credentials are loaded from the .env file.

    Example usage:
    content = read_s3_file("sudokn-ontology", "SUDOKN.rdf")
    """
    s3: S3Client = boto3.client(  # type: ignore
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read().decode("utf-8")
