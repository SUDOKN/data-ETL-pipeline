#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
from pathlib import Path
from pymongo import AsyncMongoClient

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.utils.mongo_client import get_mongo_database
from core.dependencies.aws_clients import (
    cleanup_core_aws_clients,
    initialize_core_aws_clients,
)
from data_etl_app.dependencies.aws_clients import (
    cleanup_data_etl_aws_clients,
    initialize_data_etl_aws_clients,
)

from core.models.db.manufacturer import Address, BusinessDescriptionResult
from core.services.ttl_generator_service import generate_triples
from core.utils.url_util import get_etld1_from_host
from data_etl_app.models.ontology import Ontology
from core.models.db.manufacturer_user_form import (
    ManufacturerUserForm,
)

logger = logging.getLogger(__name__)


async def main():
    try:
        db = await get_mongo_database()

        # Initialize AWS clients
        await initialize_core_aws_clients()
        await initialize_data_etl_aws_clients()
        from data_etl_app.services.knowledge.ontology_service import (
            get_ontology_service,
        )

        ontology_file_path = (
            Path(__file__).resolve().parents[4] / "ontology/SUDOKN1.1/SUDOKN1_1.rdf"
        )

        print(f"Ontology file path: {ontology_file_path}")
        with open(ontology_file_path, "r", encoding="utf-8") as file:
            ontology_data = file.read()

        ont_inst = await get_ontology_service(
            Ontology(rdf=ontology_data, s3_version_id="local-test-version-id")
        )

        mfg_user_forms: list[ManufacturerUserForm] = []
        batch_size = 2

        collection = db["manufacturer_old"]
        # async for mfg in collection.find():
        #     muf = ManufacturerUserForm(
        #         author_email="me",
        #         mfg_etld1=get_etld1_from_host(mfg["url"]),
        #         name=mfg["is_manufacturer"]["name"] if mfg["is_manufacturer"] else None,
        #         founded_in=None,
        #         email_addresses=None,
        #         num_employees=None,
        #         business_statuses=None,
        #         primary_naics=None,
        #         secondary_naics=None,
        #         addresses=[],
        #         primary_naics=None,
        #         primary_naics=None,
        #         primary_naics=None,
        #         primary_naics=None,
        #         primary_naics=None,
        #         primary_naics=None,
        #     )
        #     manufacturers.append(muf)
        #     if len(manufacturers) >= batch_size:
        #         break

        # print(manufacturers)
        # if manufacturers:
        #     batch.append(manufacturers)

        # print(f"Loaded {len(manufacturers)} manufacturers for RDF generation.")
        # ttl = generate_triples(ont_inst, manufacturers)
        # print(f"Generated RDF with {len(ttl.splitlines())} lines.")
        # with open("output.ttl", "w", encoding="utf-8") as f:
        #     f.write(ttl)
    finally:
        # Cleanup AWS clients
        await cleanup_core_aws_clients()
        await cleanup_data_etl_aws_clients()


if __name__ == "__main__":
    try:
        print("Starting RDF validation script...")
        asyncio.run(main())

    except Exception as e:
        print(f"Error during validation: {e}")
        sys.exit(1)
    print("RDF validation completed successfully.")
    sys.exit(0)
