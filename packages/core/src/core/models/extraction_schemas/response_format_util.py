from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel

# OpenAI's published limits on a strict `json_schema`. Asserted by
# `assert_strict_schema_supported` rather than discovered on the first live call,
# because the schemas are generated from rule catalogs now: a catalog that grows
# past a limit would otherwise fail every request for that stage at run time.
MAX_NESTING_DEPTH = 5
MAX_TOTAL_PROPERTIES = 100


class StrictSchemaError(ValueError):
    """A generated schema is not one OpenAI strict mode will accept."""


def _normalize_strict_keywords(node: Any) -> Any:
    """Rewrite `const` as a single-value `enum`, everywhere in the schema.

    Pydantic emits `const` for a one-value `Literal`, which is how the screening
    union tags its branches. `enum` is in the strict-mode keyword subset and
    `const` is not, and the two say the same thing, so the tag survives the
    translation intact.
    """
    if isinstance(node, dict):
        rewritten = {
            key: _normalize_strict_keywords(value)
            for key, value in node.items()
            if key != "const"
        }
        if "const" in node:
            rewritten["enum"] = [node["const"]]
        return rewritten
    if isinstance(node, list):
        return [_normalize_strict_keywords(item) for item in node]
    return node


def build_gpt_response_format(model: type[BaseModel], name: str) -> dict:
    """Wrap a Pydantic model's JSON schema into an OpenAI strict `json_schema`
    response_format dict. The model (and any nested models) must set
    `model_config = ConfigDict(extra="forbid")` so `additionalProperties: false`
    holds at every level, and required-but-nullable fields must be declared
    `Optional[...]` with no default so they still appear in `required`."""
    return {
        "type": "json_schema",
        "json_schema": {
            "name": name,
            "strict": True,
            "schema": _normalize_strict_keywords(model.model_json_schema()),
        },
    }


def _resolve(node: dict, root: dict) -> dict:
    ref = node.get("$ref")
    if not ref:
        return node
    # Pydantic only ever emits local "#/$defs/Name" refs.
    target: Any = root
    for part in ref.lstrip("#/").split("/"):
        target = target[part]
    return target


def assert_strict_schema_supported(
    response_format: dict, *, where: Optional[str] = None
) -> None:
    """Raise unless ``response_format`` satisfies strict mode's structural rules.

    Checks what a generated schema can plausibly get wrong: an object that allows
    extra properties, an object whose `required` omits one of its properties, a
    `const` that survived normalization, and the depth and property-count limits.
    """
    label = f"{where}: " if where else ""
    schema = response_format["json_schema"]["schema"]
    total_properties = 0
    max_depth = 0
    seen: set[int] = set()

    def visit(node: dict, depth: int) -> None:
        nonlocal total_properties, max_depth
        node = _resolve(node, schema)
        if id(node) in seen:
            return

        if "const" in node:
            raise StrictSchemaError(
                f"{label}`const` is not in the strict-mode keyword subset; it "
                f"should have been normalized to a single-value `enum`"
            )

        for branch_key in ("anyOf", "allOf"):
            for branch in node.get(branch_key, []):
                visit(branch, depth)

        if node.get("type") == "array":
            items = node.get("items")
            if isinstance(items, dict):
                visit(items, depth)
            return

        if node.get("type") != "object":
            return

        seen.add(id(node))
        depth += 1
        max_depth = max(max_depth, depth)

        properties = node.get("properties", {})
        total_properties += len(properties)

        if node.get("additionalProperties") is not False:
            raise StrictSchemaError(
                f"{label}object {node.get('title', '<anonymous>')!r} must set "
                f"additionalProperties: false — give its model "
                f'ConfigDict(extra="forbid")'
            )

        required = set(node.get("required", []))
        missing = set(properties) - required
        if missing:
            raise StrictSchemaError(
                f"{label}object {node.get('title', '<anonymous>')!r} leaves "
                f"{sorted(missing)} out of `required`. Strict mode requires every "
                f"property; a nullable field must be Optional[...] with no default."
            )

        for child in properties.values():
            visit(child, depth)

    visit(schema, 0)

    if max_depth > MAX_NESTING_DEPTH:
        raise StrictSchemaError(
            f"{label}schema nests {max_depth} levels of objects, over the strict-mode "
            f"limit of {MAX_NESTING_DEPTH}"
        )
    if total_properties > MAX_TOTAL_PROPERTIES:
        raise StrictSchemaError(
            f"{label}schema declares {total_properties} properties, over the "
            f"strict-mode limit of {MAX_TOTAL_PROPERTIES}"
        )
