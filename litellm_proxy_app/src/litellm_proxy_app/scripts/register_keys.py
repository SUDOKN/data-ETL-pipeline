"""
One-time migration script: reads OpenAI API keys from Redis keypool and
registers them as provider entries in LiteLLM, then generates a virtual key
for use by the extraction bots.

Prerequisites:
  1. Postgres is running:  docker compose -f postgres_infra/docker-compose.yml up -d
  2. LiteLLM proxy is running:  pm2 start ecosystem.normal.config.json --only litellm-proxy
     (or:  litellm --config ./litellm_proxy_app/src/litellm_proxy_app/config/litellm.yaml --port 4000)
  3. LITELLM_MASTER_KEY env var matches the master_key in litellm.yaml / PM2 env.
  4. KEYPOOL_PREFIX, REDIS_HOST, REDIS_PORT env vars are set (loaded via load_open_ai_app_env).

Usage:
  python -m litellm_proxy_app.scripts.register_keys
  python -m litellm_proxy_app.scripts.register_keys --litellm-url http://localhost:4000

After running, copy the printed LITELLM_VIRTUAL_KEY value into:
  litellm_proxy_app/src/.env    ← loaded by the extraction bots
  envs/litellm_proxy_app.env    ← reference copy
"""

import argparse
import logging
import os
import sys

import httpx

from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from open_ai_key_app.utils.redis_key_manager_util import get_all_openai_keys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def register_keys(litellm_url: str) -> None:
    load_open_ai_app_env()

    master_key = os.environ.get("LITELLM_MASTER_KEY")
    if not master_key:
        logger.error(
            "LITELLM_MASTER_KEY is not set. "
            "Export it or add it to the PM2 env block before running this script."
        )
        sys.exit(1)

    headers = {
        "Authorization": f"Bearer {master_key}",
        "Content-Type": "application/json",
    }

    # Fetch all OpenAI keys currently stored in Redis.
    key_map: dict[str, str] = get_all_openai_keys()  # {key_name: api_key}

    if not key_map:
        logger.warning("No OpenAI keys found in Redis keypool. Nothing to register.")
        return

    logger.info(f"Found {len(key_map)} key(s) in Redis: {list(key_map.keys())}")

    # Register each key as a separate OpenAI provider model in LiteLLM.
    # Using a per-key model name lets LiteLLM's least-busy router distribute
    # load across keys automatically.
    registered_models: list[str] = []
    for key_name, api_key in key_map.items():
        model_name = f"gpt-4o-mini"  # public model alias exposed to callers
        payload = {
            "model_name": model_name,
            "litellm_params": {
                "model": "openai/gpt-4o-mini",
                "api_key": api_key,
            },
            "model_info": {
                "custom_llm_provider": "openai",
                "description": f"OpenAI key: {key_name}",
            },
        }
        resp = httpx.post(
            f"{litellm_url}/model/new",
            json=payload,
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        registered_models.append(model_name)
        logger.info(f"Registered key '{key_name}' as LiteLLM model '{model_name}'")

    # Generate a virtual key scoped to all registered model aliases.
    vkey_payload = {
        "key_alias": "extraction-bots-key",
        "models": list(set(registered_models)),  # deduplicate aliases
        "duration": None,  # no expiry
    }
    vkey_resp = httpx.post(
        f"{litellm_url}/key/generate",
        json=vkey_payload,
        headers=headers,
        timeout=30,
    )
    vkey_resp.raise_for_status()
    virtual_key = vkey_resp.json()["key"]

    print("\n" + "=" * 60)
    print("Virtual key generated successfully.")
    print(f"\n  LITELLM_VIRTUAL_KEY={virtual_key}\n")
    print("Add the line above to:")
    print("  litellm_proxy_app/src/.env")
    print("  envs/litellm_proxy_app.env")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Migrate OpenAI keys from Redis keypool to LiteLLM proxy."
    )
    parser.add_argument(
        "--litellm-url",
        default="http://localhost:4000",
        help="Base URL of the running LiteLLM proxy (default: http://localhost:4000)",
    )
    args = parser.parse_args()
    register_keys(args.litellm_url)
