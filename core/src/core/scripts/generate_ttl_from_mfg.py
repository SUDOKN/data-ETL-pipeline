#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
from pathlib import Path

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.services.ttl_generator_service import generate_triples
from core.models.db.manufacturer import Manufacturer
from data_etl_app.models.ontology import Ontology
from core.models.db.manufacturer_user_form import (
    ManufacturerUserForm,
)

logger = logging.getLogger(__name__)


async def main():
    try:
        # Initialize AWS clients
        from core.dependencies.aws_clients import (
            cleanup_core_aws_clients,
            initialize_core_aws_clients,
        )
        from data_etl_app.dependencies.aws_clients import (
            cleanup_data_etl_aws_clients,
            initialize_data_etl_aws_clients,
        )

        await initialize_core_aws_clients()
        await initialize_data_etl_aws_clients()
        from data_etl_app.services.knowledge.ontology_service import (
            get_ontology_service,
        )

        from core.utils.mongo_client import init_db

        await init_db(
            max_pool_size=200,
            min_pool_size=50,
            socket_timeout_ms=300000,  # 5 minutes for bulk operations
            server_selection_timeout_ms=60000,  # 30 seconds
            connect_timeout_ms=60000,  # 30 seconds
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
        # skip = 40_000
        skip = 0
        batch_size = 5_000

        collection = Manufacturer.get_pymongo_collection()
        cursor = collection.find(
            {
                "$nor": [
                    {"$and": [{"name": None}, {"business_desc.name": None}]},
                    {"addresses": None},
                    {"business_desc": None},
                    # {"is_contract_manufacturer": None},
                    {"is_manufacturer": None},
                    # {"is_product_manufacturer": None},
                    {"products": None},
                    {"certificates": None},
                    {"industries": None},
                    {"material_caps": None},
                    {"process_caps": None},
                ]
            }
        ).skip(skip)
        async for mfg_doc in cursor:
            logger.info(f"Processing manufacturer document: {mfg_doc.get('etld1')}")
            mfg = Manufacturer(**mfg_doc)
            muf = ManufacturerUserForm(
                author_email="me",
                mfg_etld1=mfg.etld1,
                name=mfg.name or mfg.business_desc.name if mfg.business_desc else None,
                founded_in=mfg.founded_in,
                email_addresses=mfg.email_addresses,
                num_employees=mfg.num_employees,
                business_statuses=mfg.business_statuses,
                primary_naics=mfg.primary_naics,
                secondary_naics=mfg.secondary_naics,
                addresses=mfg.addresses or [],
                business_desc=mfg.business_desc,
                products=set(mfg.products.results if mfg.products else []),
                certificates=mfg.certificates.results if mfg.certificates else [],
                industries=mfg.industries.results if mfg.industries else [],
                process_caps=mfg.process_caps.results if mfg.process_caps else [],
                material_caps=mfg.material_caps.results if mfg.material_caps else [],
                notes="Imported from Manufacturer data",
            )
            mfg_user_forms.append(muf)
            if len(mfg_user_forms) % 100 == 0:
                print(f"Loaded {len(mfg_user_forms)} manufacturers so far...")
            if len(mfg_user_forms) >= batch_size:
                break

        # print(manufacturers)
        # if manufacturers:
        #     batch.append(manufacturers)

        print(f"Loaded {len(mfg_user_forms)} manufacturers for RDF generation.")
        ttl = generate_triples(ont_inst, mfg_user_forms)
        print(f"Generated RDF with {len(ttl.splitlines())} lines.")
        with open("output.ttl", "w", encoding="utf-8") as f:
            f.write(ttl)
    except Exception as e:
        logger.error(f"Error during RDF generation: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup AWS clients
        await cleanup_core_aws_clients()
        await cleanup_data_etl_aws_clients()


if __name__ == "__main__":
    asyncio.run(main())
    print("RDF validation completed successfully.")
    sys.exit(0)
