import asyncio
import pandas as pd
import json

from shared.models.db.manufacturer import Address, Manufacturer, NAICSEntry

# ------------ CSV Parsing & Insertion ------------


def extract_address_group(row, i):
    try:
        return Address(
            line_1=row[f"Address_{i}"],
            city=row[f"City_{i}"] if row[f"City_{i}"] else None,
            county=row[f"County_{i}"] if row[f"County_{i}"] else None,
            state=row[f"State_{i}"] if row[f"State_{i}"] else None,
            zip=str(row[f"Zip_{i}"]) if row[f"Zip_{i}"] else None,
            latitude=(float(row[f"Latitude_{i}"]) if row[f"Latitude_{i}"] else None),
            longitude=(float(row[f"Longitude_{i}"]) if row[f"Longitude_{i}"] else None),
            phone_num=str(row[f"Phone_{i}"]) if row[f"Phone_{i}"] else None,
            fax_num=str(row[f"Fax_{i}"]) if row[f"Fax_{i}"] else None,
        )
    except KeyError:
        return None


def extract_naics_group(row, i):
    try:
        return NAICSEntry(
            code=str(row[f"NAICS_Code_{i}"]) if row[f"NAICS_Code_{i}"] else "",
            desc=row[f"NAICS_Description_{i}"] or "",
        )
    except KeyError:
        return None


async def find_missing_manufacturers(path: str):
    df = pd.read_csv(path).fillna("")

    # read urls from the CSV file and find the corresponding manufacturers in the database
    urls = df["Web"].str.lower().tolist()
    print(
        f"Trying to find {len(urls)} manufacturers in the database, like {urls[0]}..."
    )
    manufacturers = await Manufacturer.find(
        {"url": {"$in": urls}}, {"_id": 1, "url": 1, "name": 1}
    ).to_list()
    print(f"Found {len(manufacturers)} manufacturers in the database.")

    # find the urls that are not in the database
    missing_urls = set(urls) - {m.url for m in manufacturers if m.name}
    print(f"Missing manufacturers: {len(missing_urls)}")
    # missing_manufacturers = [
    #     {"url": url, "name": df.loc[df["Web"].str.lower() == url, "Name"].values[0]}
    #     for url in missing_urls
    # ]
    if missing_urls:
        # write the missing urls to a JSON file
        with open("missing_manufacturers.json", "w") as f:
            json.dump(manufacturers, f, indent=4)


"""
async def insert_manufacturers_from_csv(path: str):
    df = pd.read_csv(path).fillna("")

    manufacturers = []
    for _, row in df.iterrows():
        addresses = [extract_address_group(row, i) for i in range(1, 11)]
        naics = [extract_naics_group(row, i) for i in range(1, 11)]
        addresses = [addr for addr in addresses if addr and addr.line_1]
        naics = [entry for entry in naics if entry and entry.code]

        m = Manufacturer(
            url=row["Web"].lower(),
            global_id=str(row["Global_Id"]),
            name=str(row["Name"]),
            num_employees=(
                int(str(row["Employees"])) if str(row["Employees"]).isdigit() else None
            ),
            business_desc=(
                str(row["Business_Description"])
                if str(row["Business_Description"])
                else None
            ),
            data_src=str(row["Data_Source"]),
            is_manufacturer=None,
            is_contract_manufacturer=None,
            is_product_manufacturer=None,
            business_statuses=[
                str(row[f"Business_Status_{i}"])
                for i in range(1, 4)
                if str(row[f"Business_Status_{i}"])
            ],
            products_old=[
                str(row[f"Product_{i}"])
                for i in range(1, 151)
                if str(row[f"Product_{i}"])
            ],
            addresses=addresses,
            naics=naics if naics else None,
            certificates=None,
            industries=None,
            process_caps=None,
            material_caps=None,
        )

        # doc_dict = m.model_dump(exclude_none=True)  # Beanie & Pydantic compatible
        # manufacturers_dicts.append(doc_dict)
        manufacturers.append(m)

    if manufacturers:
        await Manufacturer.insert_many(manufacturers)
        # await Manufacturer.get_motor_collection().insert_many(manufacturers)
        print(f"Inserted {len(manufacturers)} manufacturers.")
        
"""

# ------------ Main ------------


async def main():
    await init_db()
    # await insert_manufacturers_from_csv(
    #     "./mfg_master_csv/Cleaned_Master_CSV.csv"
    # )  # replace with your path
    await find_missing_manufacturers(
        "./knowledge/mfg_master_csv/Cleaned_Master_CSV.csv"
    )  # replace with your path


if __name__ == "__main__":
    asyncio.run(main())
