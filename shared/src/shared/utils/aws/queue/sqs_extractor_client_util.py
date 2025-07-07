import os

AWS_REGION = os.getenv("AWS_REGION")
AWS_SQS_EXTRACTOR_USER_ACCESS_KEY_ID = os.getenv("AWS_SQS_EXTRACTOR_USER_ACCESS_KEY_ID")
AWS_SQS_EXTRACTOR_USER_SECRET_ACCESS_KEY = os.getenv(
    "AWS_SQS_EXTRACTOR_USER_SECRET_ACCESS_KEY"
)

if (
    not AWS_REGION
    or not AWS_SQS_EXTRACTOR_USER_ACCESS_KEY_ID
    or not AWS_SQS_EXTRACTOR_USER_SECRET_ACCESS_KEY
):
    raise ValueError(
        "AWS SQS scraper credentials or region are not set. Please set them in your .env file."
    )


def make_sqs_extractor_client(
    session,
):
    """
    can be used for priority queue as well
    """
    # returns an async context‐manager
    return session.create_client(
        "sqs",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_SQS_EXTRACTOR_USER_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SQS_EXTRACTOR_USER_SECRET_ACCESS_KEY,
    )
