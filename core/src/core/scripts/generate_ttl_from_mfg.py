#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
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

MFG_FILTER = {
    "$nor": [
        {"$and": [{"name": None}, {"business_desc.name": None}]},
        {"addresses": None},
        {"business_desc": None},
        {"is_manufacturer.answer": False},
        {"process_caps.results.0": {"$exists": False}},
        {"products": None},
        {"certificates": None},
        {"industries": None},
        {"material_caps": None},
        {"process_caps": None},
    ]
}


def _fmt(n: int) -> str:
    """Format an integer with underscore thousands-separators."""
    return f"{n:_}"


def _batch_filename(start_label: int, end_label: int) -> str:
    return f"output_[{_fmt(start_label)}-{_fmt(end_label)}].ttl"


def _merged_filename(total: int) -> str:
    return f"output_[0:{_fmt(total)}].ttl"


def _find_existing_batch_file(start_label: int) -> str | None:
    """Return the filename of an existing batch file for start_label, or None.

    Uses Path.iterdir() + plain string matching to avoid glob treating '[' and ']'
    as character-class metacharacters.
    """
    prefix = f"output_[{_fmt(start_label)}-"
    for p in Path(".").iterdir():
        if p.name.startswith(prefix) and p.name.endswith("].ttl"):
            return p.name
    return None


def _merge_batch_files(
    batch_files: list[str],
    merged_path: str,
    onto_mahir_path: Path,
) -> None:
    """Merge all batch TTL files into a single file, prepending the base ontology."""
    with open(merged_path, "w", encoding="utf-8") as out:
        # 1. Write the base ontology first
        with open(onto_mahir_path, "r", encoding="utf-8") as base:
            out.write(base.read())
        out.write("\n")

        # 2. Append each batch file, stripping @prefix / @base declarations
        for batch_path in batch_files:
            with open(batch_path, "r", encoding="utf-8") as bf:
                for line in bf:
                    stripped = line.lstrip()
                    if stripped.startswith("@prefix") or stripped.startswith("@base"):
                        continue
                    out.write(line)
            out.write("\n")

    print(f"Merged {len(batch_files)} batch file(s) → {merged_path}")


async def main():
    parser = argparse.ArgumentParser(description="Generate TTL from manufacturer data")
    parser.add_argument(
        "--num-batches",
        type=int,
        default=None,
        help="Number of batches to process (default: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Number of manufacturers per batch (default: 10000)",
    )
    parser.add_argument(
        "--single-file",
        action="store_true",
        help="After processing, merge all batch files into a single output file",
    )
    args = parser.parse_args()

    batch_size: int = args.batch_size
    num_batches: int | None = args.num_batches
    single_file: bool = args.single_file

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

        # ontology_file_path = (
        #     Path(__file__).resolve().parents[4] / "ontology/SUDOKN1.1/SUDOKN1_1.rdf"
        # )
        onto_mahir_path = (
            Path(__file__).resolve().parents[4]
            / "ontology/SUDOKN1.1/sudokn_onto_mahir.ttl"
        )

        print(f"Ontology file path: {onto_mahir_path}")
        with open(onto_mahir_path, "r", encoding="utf-8") as file:
            ontology_data = file.read()

        ontology = Ontology(rdf=ontology_data, s3_version_id="local-test-version-id")

        collection = Manufacturer.get_pymongo_collection()
        batch_idx = 0
        total_processed = 0
        written_files: list[str] = []

        while True:
            if num_batches is not None and batch_idx >= num_batches:
                print(f"Reached requested batch limit ({num_batches}). Stopping.")
                break

            skip = batch_idx * batch_size
            start_label = skip + 1

            # Resumability: skip if this batch's output file already exists
            existing = _find_existing_batch_file(start_label)
            if existing is not None:
                print(
                    f"Batch {batch_idx} (start={_fmt(start_label)}): "
                    f"file '{existing}' already exists, skipping."
                )
                # Still need to track this file for merging
                written_files.append(existing)
                # Infer total_processed from end label in filename
                end_str = existing.split("-")[-1].rstrip("].ttl").replace("_", "")
                try:
                    total_processed = int(end_str)
                except ValueError:
                    total_processed = skip + batch_size
                batch_idx += 1
                continue

            # Fetch batch
            mfg_user_forms: list[ManufacturerUserForm] = []
            cursor = collection.find(MFG_FILTER).skip(skip).limit(batch_size)
            async for mfg_doc in cursor:
                logger.info(f"Processing manufacturer document: {mfg_doc.get('etld1')}")
                mfg = Manufacturer(**mfg_doc)
                muf = ManufacturerUserForm(
                    author_email="me",
                    etld1=mfg.etld1,
                    name=(
                        mfg.business_desc.name
                        if (mfg.business_desc and mfg.business_desc.name)
                        else mfg.name
                    ),
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
                    material_caps=(
                        mfg.material_caps.results if mfg.material_caps else []
                    ),
                    notes="Imported from Manufacturer data",
                )
                mfg_user_forms.append(muf)
                if len(mfg_user_forms) % 100 == 0:
                    print(
                        f"Batch {batch_idx}: loaded {len(mfg_user_forms)} manufacturers so far..."
                    )

            if not mfg_user_forms:
                print(f"Batch {batch_idx}: no data found. All manufacturers processed.")
                break

            end_label = skip + len(mfg_user_forms)
            total_processed = end_label

            print(
                f"Batch {batch_idx}: loaded {len(mfg_user_forms)} manufacturers "
                f"(indices {_fmt(start_label)}–{_fmt(end_label)})."
            )

            ttl = generate_triples(ontology, mfg_user_forms)
            out_filename = _batch_filename(start_label, end_label)
            with open(out_filename, "w", encoding="utf-8") as f:
                f.write(ttl)

            print(
                f"Batch {batch_idx}: wrote {len(ttl.splitlines())} lines → {out_filename}"
            )
            written_files.append(out_filename)
            batch_idx += 1

            # If this batch was smaller than batch_size, we've reached the end
            if len(mfg_user_forms) < batch_size:
                print("Last batch was smaller than batch size. All data processed.")
                break

        print(
            f"\nDone. {len(written_files)} batch file(s), "
            f"{_fmt(total_processed)} total manufacturers processed."
        )

        if single_file and written_files:
            merged_path = _merged_filename(total_processed)
            _merge_batch_files(written_files, merged_path, onto_mahir_path)

    except Exception as e:
        logger.error(f"Error during RDF generation: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Cleanup AWS clients
        await cleanup_core_aws_clients()
        await cleanup_data_etl_aws_clients()


if __name__ == "__main__":
    asyncio.run(main())
    print("RDF generation completed successfully.")
    sys.exit(0)
