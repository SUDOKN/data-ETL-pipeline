import json
import os

from open_ai_key_app.utils.redis_key_manager import add_openai_key

"""
# Load environment variables from .env at startup
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
"""

api_keys = json.loads(os.environ.get("API_KEYS", "[]"))
if not api_keys:
    raise ValueError(
        "API_KEYS environment variable is not set or is empty. Please set it in your .env file."
    )


async def seed_openai_keys():
    """
    Seed OpenAI API keys into the Redis database.
    This function reads API keys from the environment variable API_KEYS,
    which should be a JSON array of objects with 'name' and 'api_key' fields.
    """

    for name, api_key in api_keys:
        if not isinstance(api_key, str) or not isinstance(name, str):
            raise ValueError(
                "Each API key must be a list with 0:'name' and 1:'api_key' elements."
            )

        if not name or not api_key:
            raise ValueError("API key name and api_key cannot be empty.")

        print(f"Seeding OpenAI API key: {name}")
        await add_openai_key(api_key, name)
    print("All OpenAI API keys seeded successfully.")
