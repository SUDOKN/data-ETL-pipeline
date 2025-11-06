from pathlib import Path
import logging
from typing import Optional

from core.models.db.gpt_batch import GPTBatch
from core.models.db.api_key_bundle import APIKeyBundle
from openai import OpenAI, OpenAIError
from openai.types import Batch


logger = logging.getLogger(__name__)


def download_openai_file(
    client: OpenAI, output_type: str, output_path: Path, openai_file_id: str
) -> bool:
    try:
        if output_path.exists():
            file_size = output_path.stat().st_size
            logger.info(
                f"⏭️  Skipping download - file already exists: {output_path} "
                f"({file_size:,} bytes)"
            )
            return True
        else:
            # Create parent directory if it doesn't exist
            # output_path.parent.mkdir(parents=True, exist_ok=True)
            # Download file content
            file_response = client.files.content(openai_file_id)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(file_response.text)
            logger.info(
                f"✅ Downloaded output file to {output_path} "
                f"({len(file_response.text):,} bytes)"
            )
            return True
    except OpenAIError as e:
        logger.error(f"Error downloading {output_type} file {openai_file_id}: {e}")
        return False
    except Exception as e:
        logger.error(
            f"Unexpected error downloading {output_type} file {openai_file_id}: {e}",
            exc_info=True,
        )
        return False


def delete_uploaded_batch_file_from_openai(client: OpenAI, input_file_id: str) -> bool:
    if not input_file_id:
        logger.warning(f"No input_file_id passed")
        return False

    try:
        logger.info(f"Deleting uploaded input file {input_file_id}")
        resp = client.files.delete(input_file_id)
        # resp is expected to be a dict-like with 'deleted': True
        deleted = bool(
            getattr(
                resp,
                "deleted",
                resp.get("deleted") if isinstance(resp, dict) else False,
            )
        )
        if deleted:
            logger.info(f"Deleted input file {input_file_id}")
            return True
        else:
            logger.warning(f"Deletion response for {input_file_id}: {resp}")
            return False
    except OpenAIError as e:
        logger.error(f"OpenAI error deleting file {input_file_id}: {e}")
        return False
    except Exception as e:
        logger.error(
            f"Unexpected error deleting file {input_file_id}: {e}", exc_info=True
        )
        return False


def find_latest_batch_of_api_key_bundle(
    client: OpenAI,
    api_key_bundle: APIKeyBundle,
) -> Optional[Batch]:
    """
    Query OpenAI API to find the latest batch ID for this API key.
    Returns the most recent batch's external_batch_id or None if no batches exist.
    """
    try:
        # client = OpenAI(api_key=api_key_bundle.key)

        # List batches, sorted by created_at descending (most recent first)
        batches_response = client.batches.list(limit=1)  # Only need the most recent one

        # Get the first (most recent) batch if any exist
        if batches_response.data:
            latest_batch: Batch = batches_response.data[0]
            return latest_batch

        return None

    except Exception as e:
        logger.error(
            f"Error fetching latest batch for key {api_key_bundle.label}: {e}",
            exc_info=True,
        )
