"""Backfill ``extraction_runs`` from the manufacturers' cached results (2026-09-14).

Why this exists: the per-run history writer (``core.services.extraction_run_service``)
never produced a document between 2026-08-27 and 2026-09-14 — ``model_dump`` was not a
BSON encoder, then the document id alias slipped through as null and the collection's
schema validator refused it — while the results themselves reached the subject. A run
that completed under the broken writer therefore has a full cache and no record. The
cache carries its own ``run_provenance.run_timestamp``, which is exactly the record's
key, so the record can be rebuilt from it with no model spend.

Dry run by default: prints what it would write and whether the live validator accepts
each document (against a throwaway collection carrying the same validator). ``--write``
performs the upserts through the same ``save_extraction_run`` the pipeline uses.

Usage (from the repo root, with the app's .env):
    .venv/bin/python apps/data_etl_app/src/data_etl_app/scripts/backfill_extraction_runs.py [--subjects a.com b.com] [--write]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Optional

logging.basicConfig(level=logging.WARNING)

CONCEPT_FIELDS = ("industries", "material_caps", "process_caps", "conformity_attestations")
KEYWORD_FIELDS = ("products", "contract_products", "equipments")


async def main(subjects: Optional[list[str]], write: bool) -> None:
    from pure_utils.env_util import load_env
    from data_etl_app.dependencies.env import EXTRACT_BOT_ENV

    load_env(EXTRACT_BOT_ENV)
    from data_etl_app.dependencies.db import init_app_db

    await init_app_db()
    from core.db_models.extraction_run import ExtractionRun
    from core.services.extraction_run_service import encode_extraction_run, save_extraction_run
    from data_etl_app.db_models.manufacturer import Manufacturer

    collection = ExtractionRun.get_pymongo_collection()
    db = collection.database
    infos = await (await db.list_collections(filter={"name": "extraction_runs"})).to_list(length=1)
    validator = infos[0]["options"].get("validator") if infos else None

    probe = None
    if validator is not None and not write:
        name = "extraction_runs__backfill_probe"
        if name in await db.list_collection_names():
            await db.drop_collection(name)
        await db.create_collection(name, validator=validator, validationLevel="strict", validationAction="error")
        probe = db[name]

    query = {"etld1": {"$in": subjects}} if subjects else {}
    accepted = rejected = skipped = written = 0
    try:
        async for manufacturer in Manufacturer.find(query):
            for family, fields in (("concept", CONCEPT_FIELDS), ("keyword", KEYWORD_FIELDS)):
                for field in fields:
                    results = getattr(manufacturer, field, None)
                    if results is None or getattr(results, "run_provenance", None) is None:
                        skipped += 1
                        continue
                    provenance = results.run_provenance
                    if write:
                        await save_extraction_run(
                            subject_unique_id=manufacturer.etld1,
                            field_name=field,
                            field_family=family,  # type: ignore[arg-type]
                            run_timestamp=provenance.run_timestamp,
                            run_provenance=provenance,
                            results=results,
                        )
                        written += 1
                        continue
                    run = ExtractionRun(
                        subject_unique_id=manufacturer.etld1,
                        field_name=field,
                        field_family=family,  # type: ignore[arg-type]
                        run_timestamp=provenance.run_timestamp,
                        run_provenance=provenance,
                        results=results,
                    )
                    document = encode_extraction_run(run)
                    if probe is None:
                        accepted += 1
                        continue
                    try:
                        await probe.insert_one(document)
                        accepted += 1
                        print(f"{manufacturer.etld1}.{field}: run {provenance.run_timestamp:%Y%m%dT%H%M%S} — accepted by the live validator")
                    except Exception as error:  # noqa: BLE001 — a probe reports, it does not raise
                        rejected += 1
                        details = getattr(error, "details", None) or {}
                        print(f"{manufacturer.etld1}.{field}: REJECTED")
                        print(json.dumps(details.get("errInfo", details), indent=1, default=str)[:3000])
    finally:
        if probe is not None:
            await db.drop_collection(probe.name)
    if write:
        print(f"written {written}; skipped (no cached result) {skipped}; extraction_runs now holds {await collection.count_documents({})} documents")
    else:
        print(f"dry run: accepted {accepted}, rejected {rejected}, skipped (no cached result) {skipped}; pass --write to upsert")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--subjects", nargs="*", default=None)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.subjects, args.write))
