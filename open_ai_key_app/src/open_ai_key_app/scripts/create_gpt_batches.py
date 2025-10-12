import argparse
import asyncio
import logging
from datetime import datetime
from typing import Callable, Awaitable

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.dependencies.aws_clients import (
    initialize_core_aws_clients,
    cleanup_core_aws_clients,
)
from data_etl_app.dependencies.aws_clients import (
    initialize_data_etl_aws_clients,
    cleanup_data_etl_aws_clients,
)

from core.utils.mongo_client import init_db

logger = logging.getLogger(__name__)

from core.utils.time_util import get_current_time
from core.services.manufacturer_service import find_manufacturer_by_etld1
from open_ai_key_app.services.deferred_manufacturer_service import (
    upsert_deferred_manufacturer,
)


async def create_and_store_gpt_batches(
    deferred_at: datetime,
):
    # mfg = await find_manufacturer_by_etld1(mfg_etld1="limitedproductions.net")
    mfg = await find_manufacturer_by_etld1(mfg_etld1="autogate.com")
    logger.info(f"Found manufacturer: {mfg}")
    assert mfg is not None

    deferred_manufacturer, updated = await upsert_deferred_manufacturer(
        timestamp=deferred_at,
        manufacturer=mfg,
    )

    logger.info(f"Upserted {updated} deferred_manufacturer: \n{deferred_manufacturer}")


async def async_main():
    await init_db()

    # Initialize AWS clients
    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    try:
        await create_and_store_gpt_batches(
            deferred_at=get_current_time(),
        )
    finally:
        # Clean up AWS clients
        await cleanup_data_etl_aws_clients()
        await cleanup_core_aws_clients()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
