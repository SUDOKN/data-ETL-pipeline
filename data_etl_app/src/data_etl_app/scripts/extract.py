import os
import asyncio
import logging
from dotenv import load_dotenv

from shared.utils.time_util import get_current_time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOT_ENV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"
)
logger.info(f"Loading environment variables from: {DOT_ENV_PATH}")
load_dotenv(dotenv_path=DOT_ENV_PATH)

from data_etl_app.services.llm_powered.extraction.extract_concept_service import (
    extract_industries,
)

logger.info(f"Current working directory: {os.getcwd()}")


async def main():
    mfg_url = "www.accufab.com"
    mfg_path = f"./data_etl_app/src/data_etl_app/knowledge/tmp/{mfg_url}.txt"
    with open(f"{mfg_path}", "r") as f:
        mfg_text = f.read()

    logger.info(mfg_text[36620:54147])
    timestamp = get_current_time()

    industries = await extract_industries(timestamp, mfg_url, mfg_text)
    logger.info(industries)


if __name__ == "__main__":
    asyncio.run(main())
