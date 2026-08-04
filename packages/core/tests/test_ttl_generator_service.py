#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
from pathlib import Path

from packages.core.src.core.dependencies.load_core_env import load_core_env
from apps.data_etl_app.src.data_etl_app.dependencies.load_scraper_env import (
    load_scraper_env,
)
from apps.data_etl_app.src.data_etl_app.dependencies.load_open_ai_app_env import (
    load_open_ai_app_env,
)
from apps.data_etl_app.src.data_etl_app.dependencies.load_data_etl_env import (
    load_data_etl_env,
)

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.utils.mongo_client import init_db
from apps.data_etl_app.src.data_etl_app.dependencies.aws_clients import (
    cleanup_data_etl_aws_clients,
    initialize_data_etl_aws_clients,
)

from packages.core.src.core.models.ontology import Ontology
from apps.data_etl_app.src.data_etl_app.models.extraction_results.address_extraction_result import (
    Address,
)
from apps.data_etl_app.src.data_etl_app.models.extraction_results.business_description_extraction_result import (
    BusinessDescription,
)
from apps.data_etl_app.src.data_etl_app.services.ttl_generator_service import (
    generate_triples,
)
from apps.data_etl_app.src.data_etl_app.models.db.manufacturer_user_form import (
    ManufacturerUserForm,
)

logger = logging.getLogger(__name__)


async def main():
    try:
        await init_db()

        # Initialize AWS clients
        await initialize_data_etl_aws_clients()

        ontology_file_path = (
            Path(__file__).resolve().parents[2] / "ontology/SUDOKN1.1/SUDOKN1_1.rdf"
        )

        print(f"Ontology file path: {ontology_file_path}")
        with open(ontology_file_path, "r", encoding="utf-8") as file:
            ontology_data = file.read()

        ontology = Ontology(rdf=ontology_data, s3_version_id="local-test-version-id")
        print("Ontology initialized.")
        manufacturers = [
            ManufacturerUserForm(
                author_email="info@acmemfg.com",
                etld1="mfg-001",
                name="Acme Manufacturing",
                founded_in=1990,
                email_addresses=["info@acmemfg.com"],
                num_employees=150,
                business_statuses=["Disabled Veteran Owned"],
                primary_naics="332710",
                secondary_naics=["332312", "332313"],
                addresses=[
                    Address(
                        name="Main Plant",
                        address_lines=["123 Industrial Way"],
                        city="Metropolis",
                        state="CA",
                        county="Los Angeles",
                        postal_code="90001",
                        country="USA",
                        latitude=34.0522,
                        longitude=-118.2437,
                        phone_numbers=["+1-555-1234"],
                        fax_numbers=["+1-555-5678"],
                    )
                ],
                business_desc=BusinessDescription(
                    name="Acme Manufacturing",
                    description="Leading manufacturer of industrial components.",
                ),
                products=["Gears", "Sprockets"],
                equipment=["Haas VF-2 CNC Milling Machine"],
                certificates=["ISO 9001", "AS 9100"],
                industries=["Aerospace", "Automotive"],
                process_caps=["CNC Machining", "Gas Welding"],
                material_caps=["Aluminum", "Steel"],
                notes="Top-tier manufacturer with a focus on quality.",
            ),
            ManufacturerUserForm(
                author_email="info@globex.com",
                etld1="globex.com",
                name="Globex Corporation",
                founded_in=1985,
                email_addresses=["info@globex.com"],
                num_employees=500,
                business_statuses=["Women Owned"],
                primary_naics="334111",
                secondary_naics=["334112", "334118"],
                addresses=[
                    Address(
                        name="Headquarters",
                        address_lines=["456 Corporate Blvd"],
                        city="Springfield",
                        state="IL",
                        county="Sangamon",
                        postal_code="62701",
                        country="USA",
                        latitude=39.7817,
                        longitude=-89.6501,
                        phone_numbers=["+1-555-8765"],
                        fax_numbers=["+1-555-4321"],
                    )
                ],
                business_desc=BusinessDescription(
                    name="Globex Corporation",
                    description="Innovative solutions in electronics manufacturing.",
                ),
                products=["Circuit Boards", "Semiconductors"],
                equipment=["Surface Mount Technology Pick-and-Place Machine"],
                certificates=["ISO 14001"],
                industries=["Electronic Product", "Telecommunications"],
                process_caps=["Surface Mount Technology", "Wave Soldering"],
                material_caps=["Copper", "Silicon"],
                notes="Pioneer in green manufacturing practices.",
            ),
        ]
        print(f"Loaded {len(manufacturers)} manufacturers for RDF generation.")
        ttl = generate_triples(ontology, manufacturers)
        print(f"Generated RDF with {len(ttl.splitlines())} lines.")
        with open("output.ttl", "w", encoding="utf-8") as f:
            f.write(ttl)

    finally:
        # Cleanup AWS clients
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
