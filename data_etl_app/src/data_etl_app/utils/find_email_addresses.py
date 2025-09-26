import logging
import re
from email_validator import validate_email, EmailNotValidError
from shared.models.field_types import MfgETLDType

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

    return list(valid_emails)
