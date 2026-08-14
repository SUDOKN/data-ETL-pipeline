from urllib.parse import quote

from pure_utils.env_util import require_env


def process_cap_base_uri() -> str:
    return require_env("SUDOKN_PROCESS_CAP_BASE_URI")


def material_cap_base_uri() -> str:
    return require_env("SUDOKN_MATERIAL_CAP_BASE_URI")


def industry_base_uri() -> str:
    return require_env("SUDOKN_INDUSTRY_BASE_URI")


def conformity_attestation_base_uri() -> str:
    return require_env("SUDOKN_CONFORMITY_ATTESTATION_BASE_URI")


def ownership_status_base_uri() -> str:
    return require_env("SUDOKN_OWNERSHIP_STATUS_BASE_URI")


def naics_base_uri() -> str:
    return require_env("SUDOKN_NAICS_BASE_URI")
