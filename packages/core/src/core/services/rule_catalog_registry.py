"""Lets ``core`` reach rule catalogs that the app owns.

Catalog JSON lives in the app (``data_etl_app/knowledge/rule_catalog``), but the
parse functions that validate against it live here, and ``core`` must not import
from the app. The app registers a lookup at bootstrap; parse functions call
``get_rule_catalog``.

Keyed by ``(stage, field_type)``, which is one-to-one with a prompt — that is why
the product freehand prompt was split into pure-product and contract variants.
"""

from __future__ import annotations

import logging
from typing import Callable, Optional

from core.models.rule_catalog import RuleCatalog

logger = logging.getLogger(__name__)

RuleCatalogLookup = Callable[[str, str], Optional[RuleCatalog]]

_lookup: Optional[RuleCatalogLookup] = None


class RuleCatalogNotRegisteredError(RuntimeError):
    """No catalog is available for a stage that reports applied rules.

    Raised rather than skipping validation: a silent skip would let rule reports
    that no catalog ever checked reach the database, which is the exact failure
    this check exists to prevent.
    """


def set_rule_catalog_lookup(lookup: Optional[RuleCatalogLookup]) -> None:
    """Register the app's catalog lookup. Pass ``None`` to clear it (tests)."""
    global _lookup
    _lookup = lookup


def get_rule_catalog(stage: str, field_type: str) -> RuleCatalog:
    if _lookup is None:
        raise RuleCatalogNotRegisteredError(
            f"no rule catalog lookup registered, so applied rules for "
            f"{stage}:{field_type} cannot be validated. The app must call "
            f"set_rule_catalog_lookup() during bootstrap."
        )

    catalog = _lookup(stage, field_type)
    if catalog is None:
        raise RuleCatalogNotRegisteredError(
            f"no rule catalog registered for stage {stage!r} and field type "
            f"{field_type!r}"
        )
    return catalog
