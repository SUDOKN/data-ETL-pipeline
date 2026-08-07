"""Per-entrypoint environment variable bundles.

Each entrypoint calls `load_env(<BUNDLE>)` exactly once, before doing any work.
Duplicates across groups are fine; `load_env` dedupes.
"""

from infra import required_env as infra_env
from llm_providers import required_env as llm_env
from scraper import required_env as scraper_env

from data_etl_app import required_env as app_env

WEB_APP_ENV = [
    *infra_env.MONGO,
    *infra_env.GRAPH_DB,
    *infra_env.S3_PROMPTS,
    *infra_env.S3_RDF,
    *infra_env.S3_SCRAPED_TEXT,
    *infra_env.SQS_SCRAPE,
    *infra_env.SQS_EXTRACT,
    *infra_env.EMAIL_SES,
    *app_env.ONTOLOGY_URIS,
    *llm_env.LITELLM,
    *app_env.APP_HOSTING,
    *app_env.GOOGLE_MAPS,
]

SCRAPE_BOT_ENV = [
    *infra_env.MONGO,
    *infra_env.SQS_SCRAPE,
    *infra_env.SQS_EXTRACT,
    *infra_env.S3_SCRAPED_TEXT,
    *scraper_env.CHROME,
]

EXTRACT_BOT_ENV = [
    *infra_env.MONGO,
    *infra_env.SQS_EXTRACT,
    *infra_env.S3_SCRAPED_TEXT,
    *infra_env.S3_PROMPTS,
    *infra_env.GRAPH_DB,
    *app_env.ONTOLOGY_URIS,
    *llm_env.LITELLM,
    *app_env.APP_HOSTING,
]

MIGRATION_ENV = [
    *infra_env.MONGO,
]

ONTOLOGY_SCRIPT_ENV = [
    *infra_env.MONGO,
    *infra_env.GRAPH_DB,
    *infra_env.S3_RDF,
    *app_env.ONTOLOGY_URIS,
]

TEST_ENV = [
    *infra_env.MONGO,
]
