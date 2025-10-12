import logging
import re
import asyncio
from email_validator import validate_email, EmailNotValidError
from core.models.field_types import MfgETLDType

logger = logging.getLogger(__name__)


def get_validated_emails_from_text(mfg_etld1: MfgETLDType, text: str) -> list[str]:
    """
    Extracts all emails from text using regex,
    validates them with email_validator,
    and returns a list of unique, valid emails.
    """
    # Regex to extract candidate emails
    pattern = r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,63}\b"
    candidates = re.findall(pattern, text)

    valid_emails = set()  # use set to avoid duplicates

    for email in candidates:
        try:
            valid = validate_email(email)
            valid_emails.add(valid.email)  # normalized email
        except EmailNotValidError:
            logger.warning(f"Invalid email found: {email} while processing {mfg_etld1}")
            continue  # skip invalid ones

    logger.info(f"Found {len(valid_emails)} unique valid emails for {mfg_etld1}")
    return list(valid_emails)


async def _validate_single_email(email: str, mfg_etld1: str) -> str | None:
    """Validate a single email using DNS lookup in thread pool."""
    try:
        # Run the blocking DNS validation in a thread
        valid = await asyncio.to_thread(validate_email, email)
        return valid.email  # normalized email
    except EmailNotValidError:
        logger.warning(f"Invalid email found: {email} while processing {mfg_etld1}")
        return None


async def get_validated_emails_from_text_async(
    mfg_etld1: MfgETLDType, text: str
) -> list[str]:
    """
    Async version that:
    1. Runs regex extraction in thread pool (CPU-intensive)
    2. Validates emails concurrently (Network I/O)
    """
    # Step 1: Extract email candidates in thread pool (CPU-intensive regex)
    pattern = r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,63}\b"
    candidates = await asyncio.to_thread(re.findall, pattern, text)

    if not candidates:
        logger.info(f"No email candidates found for {mfg_etld1}")
        return []

    logger.debug(f"Found {len(candidates)} email candidates for {mfg_etld1}")

    # Step 2: Validate all emails concurrently (Network I/O)
    validation_tasks = [
        _validate_single_email(email, mfg_etld1)
        for email in set(candidates)  # dedupe candidates first
    ]

    # Run all validations concurrently
    validation_results = await asyncio.gather(*validation_tasks, return_exceptions=True)

    # Collect valid emails, filtering out None and exceptions
    valid_emails = {
        result
        for result in validation_results
        if isinstance(result, str) and result is not None
    }

    logger.info(f"Found {len(valid_emails)} unique valid emails for {mfg_etld1}")
    return list(valid_emails)
