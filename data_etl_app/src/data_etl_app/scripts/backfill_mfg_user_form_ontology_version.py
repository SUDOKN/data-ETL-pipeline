#!/usr/bin/env python3
"""
Backfill script to add ontology_version_id to existing ManufacturerUserForm documents.

This script sets ontology_version_id to the current latest version for all
ManufacturerUserForm documents that are missing this field.

Usage:
    python backfill_mfg_user_form_ontology_version.py [--dry-run]
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from core.dependencies.load_core_env import load_core_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_data_etl_env()

from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from core.models.db.manufacturer_user_form import ManufacturerUserForm
from data_etl_app.services.knowledge.ontology_service import get_ontology_service
from core.utils.env_util import get_env_variable

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def backfill_ontology_version_id(dry_run: bool = False) -> None:
    """
    Backfill ontology_version_id for all ManufacturerUserForm documents.

    Args:
        dry_run: If True, only count documents without showing changes.
    """
    # Initialize MongoDB connection
    mongo_uri = get_env_variable("MONGO_URI")
    db_name = get_env_variable("MONGO_DB_NAME")

    client = AsyncIOMotorClient(mongo_uri)
    database = client[db_name]

    # Initialize Beanie with the ManufacturerUserForm model
    await init_beanie(database=database, document_models=[ManufacturerUserForm])

    # Get the latest ontology version
    ontology_service = await get_ontology_service()
    latest_ontology = await ontology_service.get_latest_ontology()
    latest_version_id = latest_ontology.version_id

    logger.info(f"Latest ontology version: {latest_version_id}")

    # Find all documents without ontology_version_id
    # In MongoDB, missing fields can be queried with $exists: false
    docs_to_update = await ManufacturerUserForm.find(
        {"ontology_version_id": {"$exists": False}}
    ).to_list()

    total_count = len(docs_to_update)
    logger.info(
        f"Found {total_count} ManufacturerUserForm documents without ontology_version_id"
    )

    if dry_run:
        logger.info("DRY RUN: No changes will be made")
        return

    if total_count == 0:
        logger.info("No documents to update")
        return

    # Update each document
    updated_count = 0
    for doc in docs_to_update:
        try:
            doc.ontology_version_id = latest_version_id
            await doc.save()
            updated_count += 1

            if updated_count % 100 == 0:
                logger.info(f"Updated {updated_count}/{total_count} documents...")
        except Exception as e:
            logger.error(f"Failed to update document with etld1={doc.etld1}: {e}")

    logger.info(f"Successfully updated {updated_count} documents")

    # Close MongoDB connection
    client.close()


async def main():
    parser = argparse.ArgumentParser(
        description="Backfill ontology_version_id for ManufacturerUserForm documents"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only count documents without making changes",
    )

    args = parser.parse_args()

    await backfill_ontology_version_id(dry_run=args.dry_run)


if __name__ == "__main__":
    asyncio.run(main())
