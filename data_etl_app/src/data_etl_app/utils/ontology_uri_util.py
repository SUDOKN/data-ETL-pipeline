import os
from urllib.parse import quote

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

SUDOKN_OWNERSHIP_STATUS_BASE_URI = os.getenv("SUDOKN_OWNERSHIP_STATUS_BASE_URI")
if not SUDOKN_OWNERSHIP_STATUS_BASE_URI:
    raise ValueError(
        "SUDOKN_OWNERSHIP_STATUS_BASE_URI environment variable is not set. "
        "Please set it in your .env file."
    )

SUDOKN_NAICS_BASE_URI = os.getenv("SUDOKN_NAICS_BASE_URI")
if not SUDOKN_NAICS_BASE_URI:
    raise ValueError(
        "SUDOKN_NAICS_BASE_URI environment variable is not set. "
        "Please set it in your .env file."
    )


def process_cap_base_uri() -> str:
    assert SUDOKN_PROCESS_CAP_BASE_URI is not None
    return SUDOKN_PROCESS_CAP_BASE_URI


def material_cap_base_uri() -> str:
    assert SUDOKN_MATERIAL_CAP_BASE_URI is not None
    return SUDOKN_MATERIAL_CAP_BASE_URI


def industry_base_uri() -> str:
    assert SUDOKN_INDUSTRY_BASE_URI is not None
    return SUDOKN_INDUSTRY_BASE_URI


def certificate_base_uri() -> str:
    assert SUDOKN_CERTIFICATE_BASE_URI is not None
    return SUDOKN_CERTIFICATE_BASE_URI


def ownership_status_base_uri() -> str:
    assert SUDOKN_OWNERSHIP_STATUS_BASE_URI is not None
    return SUDOKN_OWNERSHIP_STATUS_BASE_URI


def naics_base_uri() -> str:
    assert SUDOKN_NAICS_BASE_URI is not None
    return SUDOKN_NAICS_BASE_URI
