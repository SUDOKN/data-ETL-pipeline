#!/usr/bin/env python3
"""
Generate MongoDB $jsonSchema validation files from Beanie Document models.

Reads each Beanie Document subclass, calls Pydantic's model_json_schema(),
converts the output from JSON Schema format to MongoDB $jsonSchema (bsonType)
format, and writes one .schema.json file per model to core/db_schemas/.

Usage:
    python generate_db_schemas.py

The script overwrites existing schema files. Models are the source of truth.
"""

import json
import logging
import re
from pathlib import Path

from beanie import Document

from core.models.db.api_key_bundle import APIKeyBundle
from core.models.db.binary_ground_truth import BinaryGroundTruth
from core.models.db.concept_ground_truth import ConceptGroundTruth
from core.models.db.deferred_manufacturer import DeferredManufacturer
from core.models.db.extraction_error import ExtractionError
from core.models.db.gpt_batch import GPTBatch
from core.models.db.gpt_batch_request import GPTBatchRequest
from core.models.db.keyword_ground_truth import KeywordGroundTruth
from core.models.db.manufacturer import Manufacturer
from core.models.db.manufacturer_user_form import ManufacturerUserForm
from core.models.db.mep_request import MEPRequest
from core.models.db.place import Place
from core.models.db.scraping_error import ScrapingError
from core.models.db.user import User

MODELS: list[type[Document]] = [
    APIKeyBundle,
    BinaryGroundTruth,
    ConceptGroundTruth,
    DeferredManufacturer,
    ExtractionError,
    GPTBatch,
    GPTBatchRequest,
    KeywordGroundTruth,
    Manufacturer,
    ManufacturerUserForm,
    MEPRequest,
    Place,
    ScrapingError,
    User,
]

SCHEMAS_DIR = Path(__file__).parent.parent / "db_schemas"

BSON_TYPE_MAP = {
    "string": "string",
    "integer": "int",
    "number": "double",
    "boolean": "bool",
    "null": "null",
    "object": "object",
    "array": "array",
}

# Beanie Document fields that must not appear in generated schemas.
# Beanie exposes the _id field under both "id" and "_id" (alias) in the JSON schema;
# we strip both and re-inject _id with the correct bsonType: objectId.
_BEANIE_INTERNAL = {"id", "_id", "revision_id"}

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def _to_snake_case(name: str) -> str:
    """Convert CamelCase / acronym class names to snake_case.

    Examples: APIKeyBundle → api_key_bundle, GPTBatch → gpt_batch
    """
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _resolve_refs(node: dict, defs: dict) -> dict:
    """Recursively inline all $ref pointers using the top-level $defs map."""
    if "$ref" in node:
        ref_name = node["$ref"].split("/")[-1]
        if ref_name not in defs:
            logger.warning(f"$ref '{ref_name}' not found in $defs — using empty object")
            return {}
        resolved = _resolve_refs(defs[ref_name], defs)
        extra = {k: v for k, v in node.items() if k != "$ref"}
        return {**resolved, **extra} if extra else resolved

    result: dict = {}
    for key, value in node.items():
        if key == "$defs":
            continue
        elif isinstance(value, dict):
            result[key] = _resolve_refs(value, defs)
        elif isinstance(value, list):
            result[key] = [
                _resolve_refs(item, defs) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            result[key] = value
    return result


def _is_nullable(converted_node: dict) -> bool:
    """Return True if a converted MongoDB schema represents a nullable / optional field.

    Nullable means the field may be absent or null in MongoDB, so it should not appear
    in the 'required' list. This matches Optional[T] / T | None semantics in Python.
    """
    bson_t = converted_node.get("bsonType")
    if isinstance(bson_t, list):
        return True
    if "anyOf" in converted_node:
        return any(b.get("bsonType") == "null" for b in converted_node["anyOf"])
    return False


def _convert_node(node: dict) -> dict:
    """Convert a resolved JSON Schema node to MongoDB $jsonSchema (bsonType) format."""

    # --- anyOf: nullable / union types ---
    if "anyOf" in node:
        branches = node["anyOf"]
        null_branch = {"type": "null"}
        non_null = [b for b in branches if b != null_branch]
        has_null = null_branch in branches

        if has_null and len(non_null) == 1:
            inner = _convert_node(non_null[0])
            bson_t = inner.get("bsonType")
            if isinstance(bson_t, str):
                # Merge all object properties at the same level (MongoDB style)
                merged = dict(inner)
                merged["bsonType"] = [bson_t, "null"]
                return merged
        # Multi-branch union: keep as anyOf
        return {"anyOf": [_convert_node(b) for b in branches]}

    # --- allOf with single branch (Pydantic wraps some refs this way) ---
    if "allOf" in node and len(node["allOf"]) == 1:
        return _convert_node(node["allOf"][0])

    # --- enum (str Enum, Literal, etc.) ---
    if "enum" in node:
        t = node.get("type")
        result: dict = {"enum": node["enum"]}
        if t and t != "null":
            result["bsonType"] = BSON_TYPE_MAP.get(t, t)
        return result

    t = node.get("type")
    fmt = node.get("format")

    # --- datetime ---
    if t == "string" and fmt == "date-time":
        return {"bsonType": "date"}

    # --- object ---
    if t == "object":
        result = {"bsonType": "object"}
        props = node.get("properties")
        additional = node.get("additionalProperties")

        if props is not None:
            converted_props = {k: _convert_node(v) for k, v in props.items()}
            # Pydantic v2 marks Optional[T] (no default) as required; strip those
            # so MongoDB allows the field to be absent (matches hand-written schema style)
            required_raw = node.get("required", [])
            required = [f for f in required_raw if not _is_nullable(converted_props.get(f, {}))]
            if required:
                result["required"] = required
            result["additionalProperties"] = False
            result["properties"] = converted_props
            # Typed additionalProperties (dict[str, T]) overrides the False above
            if isinstance(additional, dict):
                result["additionalProperties"] = _convert_node(additional)
        elif isinstance(additional, dict):
            # dict[str, T] with no named properties
            result["additionalProperties"] = _convert_node(additional)
        # else: plain untyped dict — just {"bsonType": "object"}

        return result

    # --- array ---
    if t == "array":
        result = {"bsonType": "array"}
        if "items" in node:
            result["items"] = _convert_node(node["items"])
        if node.get("uniqueItems"):
            result["uniqueItems"] = True
        return result

    # --- primitives ---
    if t in BSON_TYPE_MAP:
        return {"bsonType": BSON_TYPE_MAP[t]}

    # --- fallback: untyped dict or unknown ---
    return {"bsonType": "object"}


def generate_schema_for_model(model: type[Document]) -> dict:
    """Return a MongoDB $jsonSchema dict for the given Beanie Document model."""
    raw = model.model_json_schema()
    defs = raw.get("$defs", {})
    resolved = _resolve_refs(raw, defs)
    mongo = _convert_node(resolved)

    assert mongo.get("bsonType") == "object", (
        f"Top-level schema for {model.__name__} must be 'object', got {mongo.get('bsonType')}"
    )

    # Fields to strip from the generated schema
    computed = set(model.model_computed_fields.keys())
    to_remove = computed | _BEANIE_INTERNAL

    props = mongo.get("properties", {})
    for name in to_remove:
        props.pop(name, None)

    required = mongo.get("required", [])
    if required:
        cleaned = [f for f in required if f not in to_remove]
        if cleaned:
            mongo["required"] = cleaned
        else:
            del mongo["required"]

    # Inject _id as the first property (auto-assigned by MongoDB, not required)
    mongo["properties"] = {"_id": {"bsonType": "objectId"}, **props}

    return {"$jsonSchema": mongo}


def main() -> None:
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)

    ok = 0
    fail = 0
    for model in MODELS:
        filename = f"{_to_snake_case(model.__name__)}.schema.json"
        output_path = SCHEMAS_DIR / filename
        try:
            schema = generate_schema_for_model(model)
            output_path.write_text(json.dumps(schema, indent=2) + "\n")
            logger.info(f"✓ {model.__name__} → {filename}")
            ok += 1
        except Exception as e:
            logger.error(f"✗ {model.__name__}: {e}", exc_info=True)
            fail += 1

    logger.info(f"\n{ok} generated, {fail} failed.")
    if fail:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
