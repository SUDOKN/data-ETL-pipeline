"""Environment variables required by `infra`, grouped by concern.

Entrypoints compose only the groups they actually use and pass the result to
`pure_utils.env_util.load_env`. Only variables without a default belong here.
"""

MONGO = ["MONGO_DB_URI"]

# GRAPH_DB_UPDATE_TIMEOUT_SECONDS is optional (default "120").
GRAPH_DB = ["GRAPH_DB_BASE_URL"]

SQS_SCRAPE = [
    "SCRAPE_QUEUE_URL",
    "GT_SCRAPE_QUEUE_URL",
    "PRIORITY_SCRAPE_QUEUE_URL",
    "AWS_REGION",
    "AWS_SCRAPE_QUEUE_USER_ACCESS_KEY_ID",
    "AWS_SCRAPE_QUEUE_USER_SECRET_ACCESS_KEY",
]

SQS_EXTRACT = [
    "EXTRACT_QUEUE_URL",
    "GT_EXTRACT_QUEUE_URL",
    "PRIORITY_EXTRACT_QUEUE_URL",
    "AWS_REGION",
    "AWS_EXTRACT_QUEUE_USER_ACCESS_KEY_ID",
    "AWS_EXTRACT_QUEUE_USER_SECRET_ACCESS_KEY",
]

S3_SCRAPED_TEXT = [
    "SCRAPED_TEXT_BUCKET",
    "AWS_REGION",
    "AWS_SCRAPED_BUCKET_USER_ACCESS_KEY_ID",
    "AWS_SCRAPED_BUCKET_USER_SECRET_ACCESS_KEY",
]

S3_PROMPTS = [
    "PROMPT_BUCKET",
    "AWS_REGION",
    "AWS_RDF_AND_PROMPT_USER_ACCESS_KEY_ID",
    "AWS_RDF_AND_PROMPT_USER_SECRET_ACCESS_KEY",
]

S3_RDF = [
    "RDF_BUCKET",
    "RDF_FILENAME",
    "AWS_REGION",
    "AWS_RDF_AND_PROMPT_USER_ACCESS_KEY_ID",
    "AWS_RDF_AND_PROMPT_USER_SECRET_ACCESS_KEY",
]

# SES_REGION is optional (default "us-east-1").
EMAIL_SES = ["SES_FROM_EMAIL"]
