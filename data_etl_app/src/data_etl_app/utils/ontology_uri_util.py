import os

SUDOKN_PROCESS_CAP_BASE_URI = os.getenv("SUDOKN_PROCESS_CAP_BASE_URI")
if not SUDOKN_PROCESS_CAP_BASE_URI:
    raise ValueError(
        "SUDOKN_PROCESS_CAP_BASE_URI environment variable is not set. "
        "Please set it in your .env file."
    )

SUDOKN_MATERIAL_CAP_BASE_URI = os.getenv("SUDOKN_MATERIAL_CAP_BASE_URI")
if not SUDOKN_MATERIAL_CAP_BASE_URI:
    raise ValueError(
        "SUDOKN_MATERIAL_CAP_BASE_URI environment variable is not set. "
        "Please set it in your .env file."
    )

SUDOKN_INDUSTRY_BASE_URI = os.getenv("SUDOKN_INDUSTRY_BASE_URI")
if not SUDOKN_INDUSTRY_BASE_URI:
    raise ValueError(
        "SUDOKN_INDUSTRY_BASE_URI environment variable is not set. "
        "Please set it in your .env file."
    )

SUDOKN_CERTIFICATE_BASE_URI = os.getenv("SUDOKN_CERTIFICATE_BASE_URI")
if not SUDOKN_CERTIFICATE_BASE_URI:
    raise ValueError(
        "SUDOKN_CERTIFICATE_BASE_URI environment variable is not set. "
        "Please set it in your .env file."
    )

SUDOKN_PRODUCT_BASE_URI = os.getenv("SUDOKN_PRODUCT_BASE_URI")
if not SUDOKN_PRODUCT_BASE_URI:
    raise ValueError(
        "SUDOKN_PRODUCT_BASE_URI environment variable is not set. "
        "Please set it in your .env file."
    )

def process_cap_uri() -> str | None:
    return SUDOKN_PROCESS_CAP_BASE_URI


def material_cap_uri() -> str | None:
    return SUDOKN_MATERIAL_CAP_BASE_URI


def industry_uri() -> str | None:
    return SUDOKN_INDUSTRY_BASE_URI


def certificate_uri() -> str | None:
    return SUDOKN_CERTIFICATE_BASE_URI

def product_uri() -> str | None:
    return SUDOKN_PRODUCT_BASE_URI