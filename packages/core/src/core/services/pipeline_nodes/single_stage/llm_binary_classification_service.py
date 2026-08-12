import logging
from typing import Optional

from pydantic import ValidationError

from core.models.extraction_results.binary_classification_result import (
    LLMBinaryClassification,
)
from core.models.extraction_schemas.applied_rule import AppliedRule
from core.models.extraction_schemas.catalog_wire_schema import (
    binary_classification_response_model,
    flatten_rule_slots,
    response_format_for,
)
from core.models.field_types import ExtractionFieldType
from core.models.rule_catalog import STAGE_BINARY_CLASSIFICATION
from core.services.applied_rule_validation import (
    passed_implied_by,
    validate_applied_rules,
)
from core.services.rule_catalog_registry import get_rule_catalog

logger = logging.getLogger(__name__)


def get_binary_classification_response_schema(field_type: ExtractionFieldType) -> dict:
    """This stage's strict ``response_format``, for one classification.

    Per catalog rather than a module constant — the schema names that catalog's own
    rule ids as required properties, which is what stops a report omitting one.
    Built from the WIRE shape, not the stored one: the model reports its rules and
    the parser derives answer/reason from them. See ``catalog_wire_schema``.
    """
    return response_format_for(
        get_rule_catalog(STAGE_BINARY_CLASSIFICATION, field_type.name)
    )


def _reason_from_applied_rules(applied_rules: list[AppliedRule]) -> str:
    """The stored ``reason``, synthesized from the report.

    The full trail rather than a summary: reason's consumers are humans (the
    ground-truth survey, the keyword API's rejection message), and picking a
    "decisive" rule would re-introduce editorial judgment in code that the whole
    catalog design moved into the rules.
    """
    return "\n".join(
        f"[{rule.rule_id} {rule.outcome}] {rule.explanation}"
        for rule in applied_rules
    )


def parse_binary_classification_result_from_gpt_response(
    gpt_response: Optional[str],
    field_type: ExtractionFieldType,
) -> LLMBinaryClassification:
    if not gpt_response:
        logger.error(f"Invalid gpt_response:{gpt_response}")
        raise ValueError(
            "parse_binary_classification_result_from_gpt_response: Empty or invalid response from GPT"
        )

    catalog = get_rule_catalog(STAGE_BINARY_CLASSIFICATION, field_type.name)

    try:
        report = binary_classification_response_model(catalog).model_validate_json(
            gpt_response
        )
    except ValidationError as e:
        raise ValueError(
            f"parse_binary_classification_result_from_gpt_response: Invalid response from GPT:{gpt_response}"
        ) from e

    # No null-entity shortcut, unlike screening: a screening request carries many
    # phrases and mostly-rejecting batches earn the token saving, but this is one
    # unit per request and the report is the only diagnostic there is. A text
    # that offered no candidate reports the first condition failed and the rest
    # not_triggered, which validates like any other rejection.
    applied_rules = flatten_rule_slots(catalog, report)
    validate_applied_rules(
        catalog=catalog,
        applied_rules=applied_rules,
        where=f"{field_type.name} classification",
    )

    # Derived, never reported — the record and the verdict cannot disagree.
    answer = passed_implied_by(catalog, applied_rules)

    # A positive answer is a claim about a named entity, so the name has to be
    # there. A negative one still names the candidate it got furthest with, and
    # null only when the text offered none.
    if answer and report.identified_entity is None:
        raise ValueError(
            f"parse_binary_classification_result_from_gpt_response: "
            f"{field_type.name} reported rules that imply a positive answer but "
            f"named no identified_entity"
        )

    return LLMBinaryClassification(
        answer=answer,
        reason=_reason_from_applied_rules(applied_rules),
        confidence=report.confidence,
        identified_entity=report.identified_entity,
        applied_rules=applied_rules,
    )
