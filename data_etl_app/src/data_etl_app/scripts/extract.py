import os
import asyncio
import os
from dotenv import load_dotenv

DOT_ENV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"
)
print(f"Loading environment variables from: {DOT_ENV_PATH}")
load_dotenv(dotenv_path=DOT_ENV_PATH)

from data_etl_app.services.extract_concept_service import extract_industries

print(os.getcwd())


async def main():
    mfg_url = "www.accufab.com"
    mfg_path = f"./data_etl_app/src/data_etl_app/knowledge/tmp/{mfg_url}.txt"
    with open(f"{mfg_path}", "r") as f:
        mfg_text = f.read()

    print(mfg_text[36620:54147])

    industries = await extract_industries(mfg_url, mfg_text)
    print(industries)


if __name__ == "__main__":
    asyncio.run(main())
