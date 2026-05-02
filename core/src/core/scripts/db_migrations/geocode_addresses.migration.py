#!/usr/bin/env python3
"""
Migration script to geocode addresses for all Manufacturer documents.

For every address in every manufacturer that has an addresses list, this script:
  - Calls the Google Maps Geocoding API via get_lat_lng_from_address
  - Writes latitude, longitude, and place_id back into the address subdocument

Run with --dry-run to preview changes without writing to MongoDB.
"""

import argparse
import asyncio
import logging
from datetime import datetime, timezone
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables before importing app modules that depend on them
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.models.db.manufacturer import Address, Manufacturer
from core.models.db.place import Place
from core.utils.mongo_client import init_db
from data_etl_app.utils.lat_lng_util import get_geocode_result_from_address

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def iterate(dry_run: bool) -> None:
    print("=" * 80)
    print(f"Geocode Addresses Migration{'  [DRY RUN]' if dry_run else ''}")
    print("=" * 80)

    collection = Manufacturer.get_pymongo_collection()
    total_mfg = await collection.count_documents({})
    print(f"Total manufacturer documents: {total_mfg}")

    query = {
        "addresses": {"$exists": True, "$ne": None, "$not": {"$size": 0}},
        "addresses.place_id": {"$exists": False},
    }
    matched_count = await collection.count_documents(query)
    print(
        f"Manufacturers matching query (at least one address without place_id): {matched_count}"
    )
    input("Press Enter to proceed...")

    # Fetch in chunks to avoid holding the cursor open during slow geocoding API calls.
    # Each iteration: (1) load chunk into memory, (2) geocode with no active cursor,
    # (3) bulk-write results with a fresh connection.
    fetch_chunk_size = 50

    total_updated = 0
    total_failed = 0
    docs_processed = 0
    docs_with_updates = 0  # manufacturers that had at least one address geocoded
    addresses_geocoded = 0  # addresses that received lat/lng for the first time
    addresses_overwritten = 0  # addresses whose existing lat/lng was overwritten
    skip = 0

    while True:
        # ── Phase 1: fetch chunk into memory (cursor opens and closes here) ──────
        projection = {"_id": 1, "etld1": 1, "addresses": 1}
        docs = (
            await collection.find(query, projection)
            .skip(skip)
            .limit(fetch_chunk_size)
            .to_list(length=fetch_chunk_size)
        )
        if not docs:
            break

        print(f"\n--- Chunk starting at offset {skip} ({len(docs)} docs) ---")

        bulk_operations: list[UpdateOne] = []
        places_ops: list[UpdateOne] = []

        # ── Phase 2: geocode in memory — no MongoDB connection held open ─────────
        for doc in docs:
            docs_processed += 1
            etld1 = doc.get("etld1", "<unknown>")
            addresses = doc.get("addresses", [])
            updated = False

            print(f"\n[{docs_processed}] {etld1} — {len(addresses)} address(es)")

            for i, addr_dict in enumerate(addresses):
                try:
                    addr = Address(**addr_dict)
                except Exception as e:
                    print(f"  Address {i}: skipped — could not parse: {e}")
                    continue

                coords = get_geocode_result_from_address(addr)
                if coords is None:
                    print(f"  Address {i}: could not geocode, skipping")
                    continue
                raw_result, geocode_query = coords
                lat = raw_result["geometry"]["location"]["lat"]
                lng = raw_result["geometry"]["location"]["lng"]
                place_id = raw_result.get("place_id")
                old_lat = addr_dict.get("latitude")
                old_lng = addr_dict.get("longitude")

                addresses_geocoded += 1
                print(
                    f"  Address {i}: geocoded lat={lat}, lng={lng}, place_id={place_id}"
                )

                value_changed = (old_lat != lat) or (old_lng != lng)
                addr_dict["latitude"] = lat
                addr_dict["longitude"] = lng
                addr_dict["place_id"] = place_id
                updated = True

                if place_id is not None:
                    places_ops.append(
                        UpdateOne(
                            {"place_id": place_id},
                            {
                                "$set": {
                                    "geocoded_at": datetime.now(tz=timezone.utc),
                                    "geocode_query": geocode_query,
                                    "raw_result": raw_result,
                                }
                            },
                            upsert=True,
                        )
                    )

                if value_changed:
                    addresses_overwritten += 1
                    print(
                        f"  Address {i}: overwritten ({old_lat}, {old_lng}) -> lat={lat}, lng={lng}, place_id={place_id}"
                    )

            if updated:
                docs_with_updates += 1
                if dry_run:
                    print(f"  [DRY RUN] Would update addresses for {etld1}")
                else:
                    # Normalize lat/lng to float across ALL addresses (not just newly geocoded ones)
                    # to satisfy the BSON schema (bsonType: double). Existing int values like -80
                    # would otherwise fail validation on write.
                    for a in addresses:
                        if a.get("latitude") is not None:
                            a["latitude"] = float(a["latitude"])
                        if a.get("longitude") is not None:
                            a["longitude"] = float(a["longitude"])
                    bulk_operations.append(
                        UpdateOne(
                            {"_id": doc["_id"]}, {"$set": {"addresses": addresses}}
                        )
                    )

        if dry_run:
            if places_ops:
                print(
                    f"\n  [DRY RUN] Would upsert {len(places_ops)} place doc(s) for this chunk"
                )
            skip += fetch_chunk_size
            continue

        # ── Phase 3: bulk-write with a fresh connection (no cursor is open) ──────
        if bulk_operations:
            print(f"\nFlushing {len(bulk_operations)} manufacturer update(s)...")
            try:
                result = await collection.bulk_write(bulk_operations)
                total_updated += result.modified_count
                total_failed += len(bulk_operations) - result.modified_count
                print(
                    f"  Chunk complete: {result.modified_count} updated, "
                    f"{len(bulk_operations) - result.modified_count} failed"
                )
                print(
                    f"  Running totals — manufacturers: {docs_with_updates}, "
                    f"geocoded (new/unchanged): {addresses_geocoded}, "
                    f"overwritten (changed): {addresses_overwritten}"
                )
            except BulkWriteError as bwe:
                logger.error(f"Bulk write error: {bwe.details}")
                for err in bwe.details.get("writeErrors", [])[:5]:
                    print(f"  Error index={err['index']}: {err['errmsg']}")
                total_failed += len(bulk_operations)
            except Exception as e:
                logger.error(f"Bulk write error: {e}")
                total_failed += len(bulk_operations)

        if places_ops:
            print(f"  Flushing {len(places_ops)} place upsert(s)...")
            try:
                places_collection = Place.get_pymongo_collection()
                await places_collection.bulk_write(places_ops, ordered=False)
                print(f"  Places upserted: {len(places_ops)}")
            except Exception as e:
                logger.error(f"Places bulk write error: {e}")

        skip += fetch_chunk_size

    print("\n" + "=" * 80)
    if dry_run:
        print(
            f"[DRY RUN] Migration complete: {docs_processed} documents scanned, no writes performed."
        )
    else:
        print(
            f"Migration complete: {total_updated} documents updated, {total_failed} failed."
        )
    print(f"  Manufacturers with updates:             {docs_with_updates}")
    print(f"  Addresses geocoded (no change / new):  {addresses_geocoded}")
    print(f"  Addresses overwritten (values changed): {addresses_overwritten}")
    print("=" * 80)


async def main(dry_run: bool) -> None:
    await init_db()
    print("Database initialized.")
    await iterate(dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Geocode manufacturer addresses and persist lat/lng/place_id."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview which documents would be updated without writing to MongoDB.",
    )
    args = parser.parse_args()
    asyncio.run(main(dry_run=args.dry_run))
