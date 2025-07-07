import os

AWS_REGION = os.getenv("AWS_REGION")
AWS_S3_SCRAPER_USER_ACCESS_KEY_ID = os.getenv("AWS_S3_SCRAPER_USER_ACCESS_KEY_ID")
AWS_S3_SCRAPER_USER_SECRET_ACCESS_KEY = os.getenv(
    "AWS_S3_SCRAPER_USER_SECRET_ACCESS_KEY"
)

if (
    not AWS_REGION
    or not AWS_S3_SCRAPER_USER_ACCESS_KEY_ID
    or not AWS_S3_SCRAPER_USER_SECRET_ACCESS_KEY
):
    raise ValueError(
        "AWS S3 scraper credentials or region are not set. Please set them in your .env file."
    )


def make_s3_client(
    session,
):
    return session.create_client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_S3_SCRAPER_USER_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_S3_SCRAPER_USER_SECRET_ACCESS_KEY,
    )
