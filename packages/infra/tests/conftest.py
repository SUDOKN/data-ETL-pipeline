from pure_utils.env_util import load_env

from infra.required_env import S3_SCRAPED_TEXT

# Entrypoint for the infra test session: bucket names are resolved at module import time.
load_env(S3_SCRAPED_TEXT)
