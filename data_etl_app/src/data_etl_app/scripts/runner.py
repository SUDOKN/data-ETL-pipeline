# %%
import aiofiles
import asyncio
import os
import datetime
import time
import shutil

from data_etl_app.models.db.manufacturer import Manufacturer
from data_etl_app.models.db.extraction_error import ExtractionError
from data_etl_app.models.db.binary_classifier_result import (
    BinaryClassifierResult_DBModel,
)
from data_etl_app.models.db.extraction_results import ExtractionResults_DBModel
from data_etl_app.services.is_manufacturer_service import is_company_a_manufacturer
from data_etl_app.services.extract_concept_service import (
    extract_industries,
    extract_certificates,
    extract_materials,
    extract_processes,
)

from data_etl_app.utils.mongo_client import init_db

from open_ai_key_app.services.openai_keypool_service import keypool
from open_ai_key_app.utils.ask_gpt_util import num_tokens_from_string


async def process_mfg_text(mfg_txt: str, manufacturer: Manufacturer):
    print(f"Processing `{manufacturer.name}:{manufacturer.url}`")
    updated = False
    updated_at = datetime.datetime.now(datetime.timezone.utc)
    if (
        manufacturer.is_manufacturer is None
        or manufacturer.is_manufacturer.name is None
    ):
        try:
            mfg_bundle = await is_company_a_manufacturer(
                manufacturer.url,
                mfg_txt,
            )
            manufacturer.is_manufacturer = BinaryClassifierResult_DBModel.from_dict(
                mfg_bundle
            )
            updated = True
            if (
                manufacturer.is_manufacturer
                and manufacturer.is_manufacturer.answer is False
            ):
                manufacturer.updated_at = updated_at
                await manufacturer.save()  # crucial to save before proceeding
                return

        except Exception as e:
            print(f"{manufacturer.name}.is_manufacturer errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    error=str(e),
                    field="is_manufacturer",
                    url=manufacturer.url,
                    name=manufacturer.name,
                )
            )
            return

    if manufacturer.is_manufacturer and manufacturer.is_manufacturer.answer is False:
        print(
            f"{manufacturer.name} is not a manufacturer, because\n{manufacturer.is_manufacturer.explanation}"
        )
        return

        # extract industries if missing
    if (
        not manufacturer.industries
        or not manufacturer.industries.results
        or (
            manufacturer.industries.results
            and any(result.islower() for result in manufacturer.industries.results)
        )
    ):
        try:
            industries = await extract_industries(manufacturer.url, mfg_txt)
            manufacturer.industries = ExtractionResults_DBModel.from_dict(industries)
            updated = True
        except Exception as e:
            print(f"{manufacturer.name}.industries errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    error=str(e),
                    field="industries",
                    url=manufacturer.url,
                    name=manufacturer.name,
                )
            )

    if (
        not manufacturer.certificates
        or not manufacturer.certificates.results
        or (
            manufacturer.certificates.results
            and any(result.islower() for result in manufacturer.certificates.results)
        )
    ):
        try:
            certificates = await extract_certificates(
                manufacturer.url,
                mfg_txt,
            )
            manufacturer.certificates = ExtractionResults_DBModel.from_dict(
                certificates
            )
            updated = True
        except Exception as e:
            print(f"{manufacturer.name}.certificates errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    error=str(e),
                    field="certificates",
                    url=manufacturer.url,
                    name=manufacturer.name,
                )
            )

    # extract material capabilities if missing
    if (
        not manufacturer.material_caps
        or not manufacturer.material_caps.results
        or (
            manufacturer.material_caps.results
            and any(result.islower() for result in manufacturer.material_caps.results)
        )
    ):
        try:
            material_caps = await extract_materials(
                manufacturer.url,
                mfg_txt,
            )
            manufacturer.material_caps = ExtractionResults_DBModel.from_dict(
                material_caps
            )
            updated = True
        except Exception as e:
            print(f"{manufacturer.name}.material_caps errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    error=str(e),
                    field="material_caps",
                    url=manufacturer.url,
                    name=manufacturer.name,
                )
            )

    # extract process capabilities if missing
    if (
        not manufacturer.process_caps
        or not manufacturer.process_caps.results
        or (
            manufacturer.process_caps.results
            and any(result.islower() for result in manufacturer.process_caps.results)
        )
    ):
        try:
            process_caps = await extract_processes(
                manufacturer.url,
                mfg_txt,
            )
            manufacturer.process_caps = ExtractionResults_DBModel.from_dict(
                process_caps
            )
            updated = True
        except Exception as e:
            print(f"{manufacturer.name}.process_caps errored:{e}")
            await ExtractionError.insert_one(
                ExtractionError(
                    error=str(e),
                    field="process_caps",
                    url=manufacturer.url,
                    name=manufacturer.name,
                )
            )

    # get manufacturer from mongo
    if updated:
        # update the manufacturer in mongo
        manufacturer.updated_at = updated_at
        await manufacturer.save()
        print(f"Updated `{manufacturer.name}:{manufacturer.url}` in MongoDB")

    print(f"Processed {manufacturer.name}:{manufacturer.url}")


async def safe_process_mfg_text(
    mfg_txt, manufacturer, filename, read_directory, done_directory
):
    try:
        await process_mfg_text(mfg_txt, manufacturer)
        # Move the file only if processing succeeds
        src = os.path.join(read_directory, filename)
        dst = os.path.join(done_directory, filename)
        os.rename(src, dst)
        return True
    except Exception as e:
        print(f"Error processing {manufacturer.name}:{manufacturer.url} — {e}")
        return False


async def iterate_txt_files(
    read_directory, done_directory, too_small_directory, too_large_directory
):
    all_filenames = [f for f in os.listdir(read_directory) if f.endswith(".txt")]
    remaining = len(all_filenames)
    batch_size = 100

    total_processed = 0
    total_time = 0
    for i in range(0, len(all_filenames), batch_size):
        batch = all_filenames[i : i + batch_size]
        try:
            urls = [filename.split(".txt")[0].lower() for filename in batch]
            manufacturers = await Manufacturer.find({"url": {"$in": urls}}).to_list()
            url_to_manufacturer = {m.url: m for m in manufacturers}

            tasks = []
            for filename in batch:
                url = filename.split(".txt")[0].lower()
                manufacturer = url_to_manufacturer.get(url)
                if not manufacturer:
                    print(f"No manufacturer found for URL: {url}")
                    continue

                filepath = os.path.join(read_directory, filename)
                try:
                    async with aiofiles.open(filepath, "r", encoding="utf-8") as f:
                        mfg_txt = await f.read()
                    num_tokens = num_tokens_from_string(mfg_txt)
                    if num_tokens < 30:
                        # should never happen, but just in case
                        too_small_path = os.path.join(too_small_directory, filename)
                        shutil.move(filepath, too_small_path)
                        print(f"Skipped {filename} — too small (<30 tokens)")
                        continue
                    elif num_tokens > 120000:
                        # should never happen, but just in case
                        too_large_path = os.path.join(too_large_directory, filename)
                        shutil.move(filepath, too_large_path)
                        print(f"Skipped {filename} — too large (>120k tokens)")
                        continue

                    task = asyncio.create_task(
                        safe_process_mfg_text(
                            mfg_txt,
                            manufacturer,
                            filename,
                            read_directory,
                            done_directory,
                        )
                    )

                    tasks.append(task)
                except Exception as e:
                    print(f"Error reading file {filepath}: {e}")
                    continue
            remaining -= len(batch)
            print(f"remaining: {remaining}")

            success_count = 0
            batch_start_time = time.time()

            for task in asyncio.as_completed(tasks):
                try:
                    result = await task
                    if result:
                        success_count += 1
                except Exception as e:
                    print(f"Task failed with error: {e}")

            batch_time = time.time() - batch_start_time
            total_processed += success_count
            total_time += batch_time

            if total_processed:
                avg_time_per_task = total_time / total_processed
                est_remaining_time = (
                    avg_time_per_task * remaining
                )  # assuming each remaining task succeeds

                print(f"Batch {i}-{i+len(batch)} took {batch_time:.2f} seconds.")
                print(f"Successful tasks: {success_count}")
                print(f"Avg time per successful task: {avg_time_per_task:.2f} seconds.")
                print(f"Estimated time remaining: {est_remaining_time/60:.2f} minutes.")
            else:
                print("No successful tasks yet; skipping timing estimate.")

            # print number of active keys in the pool
            print(f"Active keys in pool: {len(keypool.slots)}\n\n")

        except Exception as batch_err:
            print(f"Batch {i}-{i + batch_size} failed: {batch_err}")


async def main():
    await init_db()
    await iterate_txt_files(
        read_directory="./scraped_manufacturers/final",
        done_directory="./scraped_manufacturers/done",
        too_small_directory="./scraped_manufacturers/too_small",
        too_large_directory="./scraped_manufacturers/too_large",
    )


if __name__ == "__main__":
    # print(f'os.getenv("PM2_APP_NAME"):{os.getenv("PM2_APP_NAME")}')
    asyncio.run(main())
