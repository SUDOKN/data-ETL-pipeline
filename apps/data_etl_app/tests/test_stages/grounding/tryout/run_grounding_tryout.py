"""Step 2 tryout: the redesigned prompts against today's, on REAL records of a
finished run, in the production call shape, N repeats per arm read as a mode.

Why this exists (grounding-gap survey Phase 4, 2026-09-14; Step 2 design draft
§8 step 1, 2026-09-21): the rebuilt stages cannot be measured on the pipeline
before they are built, but their own gap classes — coverage violations,
membership drops, multi-record inference, proposal flood, quote completeness,
per-option stability, fabricated machines, own-line sectors, screening's wrong
accepts — are properties of the model's answer to a prompt, and a prompt can
be tried on the records of a finished synthesis run. Every arm runs N repeats
(default 5) and is read as a mode, never as one sample (design doc §23).

Requests are rebuilt from the run's dumps with no Mongo: a chunk's synthesis
block carries every group's focal form and paragraph, and a grounding request
is exactly ``render_record_blocks(group payloads)`` plus the options outline,
behind the per-request nonce line, packed 25 records per request as production
does. The vocabulary comes from the repo's ontology JSON, rendered by the same
``render_concept_outline`` production uses (the dash-line shape for arm ``b``).

Arms (``--arms``):
    a    today's two grounding calls (initial, then out-of-vocabulary chained on
         its answer), concept fields; published prompts + their catalog schemas.
    b    Step 2's one grounding call: the ``phrase_grounding`` catalog rendered
         under ``final_texts/assembled/``, the dash-line outline, the three-list
         schema generated from the catalog.
    fa   today's freehand grounding (equipments, products, contract_products):
         the published text snapshotted under ``arms/today/`` with its catalog.
    fb   Step 2's reworded freehand catalog (same rule ids, same schema).
    sa   today's screening prompt, on the MODE candidates of a grounding arm
         (``--screen-from``, default ``b`` for concept fields, ``fb`` for
         freehand), so the two screening arms judge the same candidate set.
    sb   Step 2's unit screening (``phrase_unit_screening`` catalog): the
         records once, then a UNITS block (option, meaning, record ids), on the
         same mode candidates.

Outputs land under ``out/<tag>/`` as raw response text plus a ``.usage.json``
per call (prompt/completion tokens, seconds); ``check_tryout.py`` reads them.

Usage (from the repo root):
    .venv/bin/python apps/data_etl_app/tests/test_stages/grounding/tryout/run_grounding_tryout.py --list
    ... --targets tanfel.com:process_caps:0:81109:0 --write
    ... --targets ... --arms a b --n 5
    ... --targets ... --arms sa sb --n 5          # after the grounding arms
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import time
import uuid
from collections import Counter, defaultdict
from typing import Any, Optional

ROOT = next(p for p in pathlib.Path(__file__).resolve().parents if (p / ".git").exists())
for _line in (ROOT / ".env").read_text().splitlines() if (ROOT / ".env").exists() else []:
    if "=" in _line and not _line.startswith("#"):
        _k, _v = _line.split("=", 1)
        os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

HERE = pathlib.Path(__file__).resolve().parent
ARMS = HERE / "arms"
TODAY = ARMS / "today"
OUT = HERE / "out"
DUMPS = ROOT / "packages" / "logs" / "extraction_dumps"
PROMPTS = ROOT / "apps" / "data_etl_app" / "src" / "data_etl_app" / "knowledge" / "prompts"
ASSEMBLED = PROMPTS / "final_texts" / "assembled"
ONTOLOGY = ROOT / "apps" / "data_etl_app" / "src" / "data_etl_app" / "knowledge" / "ontology"
SUBJECT_NAMES = HERE / "subject_names.json"

DEFAULT_RUN = "20260915T024255"  # the pre-Step-2 baseline (18 subjects, C16 paragraphs)
NONCE_LABEL = "request nonce (ignore): "
MANUFACTURER_LABEL = "the name of the manufacturer in question: "
MAX_RECORDS_PER_REQUEST = 25  # ExtractionPipelineFactory.DEFAULT_*_MAX_PAIRS_PER_REQUEST

UNITS_OPEN = "<<<UNITS"
UNITS_CLOSE = "UNITS>>>"

CONCEPT_FIELDS = {
    "industries": "industry",
    "material_caps": "material_cap",
    "process_caps": "process_cap",
    "conformity_attestations": "conformity_attestation",
}
FREEHAND_FIELDS = {
    "equipments": "equipment",
    "products": "product",
    "contract_products": "product",
}
FIELD_PREFIX = {**CONCEPT_FIELDS, **FREEHAND_FIELDS}
FIELD_ONTOLOGY = {
    "industries": "industries.json",
    "material_caps": "material_caps.json",
    "process_caps": "process_caps.json",
    "conformity_attestations": "certificates.json",
}

GROUNDING_ARMS = ("a", "b", "fa", "fb")
SCREENING_ARMS = ("sa", "sb")


# --- records and vocabulary ---------------------------------------------------


def load_concepts(field: str) -> set[Any]:
    from core.utils.rdf_to_graph_util import tree_list_to_flat  # noqa: E402

    doc = json.loads((ONTOLOGY / FIELD_ONTOLOGY[field]).read_text(encoding="utf-8"))
    body = next(v for v in doc.values() if isinstance(v, list))
    return tree_list_to_flat(body)


def concept_by_label(field: str) -> dict[str, Any]:
    """casefold label (name or altLabel) -> Concept."""
    out: dict[str, Any] = {}
    for concept in load_concepts(field):
        for label in [concept.name, *concept.altLabels]:
            out[label.casefold()] = concept
    return out


def options_section(field: str, *, arm: str, oov: bool = False) -> str:
    from core.utils.rdf_to_graph_util import render_concept_outline  # noqa: E402

    if arm == "b":
        return (
            "the vocabulary to match against:\n"
            f"{render_concept_outline(load_concepts(field), with_definitions=True)}"
        )
    heading = (
        "the vocabulary that earlier identification matched against:"
        if oov
        else "options to match against:"
    )
    return f"{heading}\n{render_concept_outline(load_concepts(field))}"


def chunk_records(run: str, subject: str, field: str, chunk: str) -> dict[str, dict[str, Any]]:
    """group_id -> {focal_form, synthesis} for every synthesized group of the chunk."""
    slug = subject.replace(".", "_")
    path = DUMPS / run / f"{slug}__{field}__partial.json"
    if not path.exists():
        path = DUMPS / run / f"{slug}__{field}.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    block = doc["chunks"][chunk].get("synthesis") or {}
    payloads: dict[str, dict[str, Any]] = {}
    for rec in block.get("records") or []:
        if rec.get("status") != "synthesized" or not rec.get("synthesis"):
            continue
        payloads[rec["group_id"]] = {"focal_form": rec["focal_form"], "synthesis": rec["synthesis"]}
    return payloads


def request_groups(run: str, subject: str, field: str, chunk: str) -> list[dict[str, dict[str, Any]]]:
    from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (  # noqa: E402
        grouped_record_payloads,
    )

    return grouped_record_payloads(chunk_records(run, subject, field, chunk), MAX_RECORDS_PER_REQUEST)


def render_context(
    payloads: dict[str, dict[str, Any]],
    field: str,
    *,
    arm: str,
    oov: bool = False,
    nonce: Optional[str] = None,
) -> str:
    from core.services.phrase_blocks_contract import render_record_blocks  # noqa: E402

    body = render_record_blocks(payloads)
    if field in CONCEPT_FIELDS and arm in ("a", "b"):
        body = f"{body}\n\n{options_section(field, arm=arm, oov=oov)}"
    return f"{NONCE_LABEL}{nonce or uuid.uuid4().hex}\n\n{body}"


def subject_name(subject: str) -> str:
    names = json.loads(SUBJECT_NAMES.read_text(encoding="utf-8"))
    name = names.get(subject)
    if not name:
        raise SystemExit(f"no display name for {subject!r} in {SUBJECT_NAMES}")
    return name


# --- targets ------------------------------------------------------------------


class Target:
    def __init__(self, spec: str) -> None:
        parts = spec.split(":")
        if len(parts) != 5:
            raise SystemExit(f"target {spec!r}: want subject:field:start:end:group_index")
        self.subject, self.field = parts[0], parts[1]
        if self.field not in FIELD_PREFIX:
            raise SystemExit(f"target {spec!r}: unknown field {self.field!r}")
        self.chunk = f"{parts[2]}:{parts[3]}"
        self.group_index = int(parts[4])
        self.tag = f"{self.subject.split('.')[0]}_{self.field}_{parts[2]}_g{self.group_index}"

    @property
    def concept(self) -> bool:
        return self.field in CONCEPT_FIELDS

    def payloads(self, run: str) -> dict[str, dict[str, Any]]:
        groups = request_groups(run, self.subject, self.field, self.chunk)
        if self.group_index >= len(groups):
            raise SystemExit(f"{self.tag}: group {self.group_index} of {len(groups)} does not exist")
        return groups[self.group_index]


def list_targets(run: str) -> None:
    for path in sorted((DUMPS / run).glob("*__*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        field = doc.get("field_type")
        if field not in FIELD_PREFIX:
            continue
        for chunk, ch in doc["chunks"].items():
            n = sum(1 for r in (ch.get("synthesis") or {}).get("records") or [] if r.get("status") == "synthesized")
            groups = -(-n // MAX_RECORDS_PER_REQUEST) if n else 0
            print(f"{doc['subject_unique_id']}:{field}:{chunk}  records={n} groups={groups}")


# --- prompts and schemas per arm ------------------------------------------------


def _catalog_registry() -> None:
    from core.services.rule_catalog_registry import set_rule_catalog_lookup  # noqa: E402
    from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup  # noqa: E402

    set_rule_catalog_lookup(build_rule_catalog_lookup())


def _catalog(stage: str, field: str) -> Any:
    from core.services.rule_catalog_registry import get_rule_catalog  # noqa: E402

    return get_rule_catalog(stage, field)


def _assembled_text(catalog: Any) -> str:
    from data_etl_app.services.prompt_assembly_service import prompt_s3_key  # noqa: E402

    return (ASSEMBLED / prompt_s3_key(catalog)).read_text(encoding="utf-8")


def _schema(catalog: Any) -> dict:
    from core.models.extraction_schemas.catalog_wire_schema import response_format_for  # noqa: E402

    return response_format_for(catalog)


def arm_system_and_schema(arm: str, field: str) -> tuple[str, dict, Any]:
    """(system prompt, response_format, catalog) for a grounding or screening arm."""
    from core.models.rule_catalog import (  # noqa: E402
        STAGE_FREEHAND_GROUNDING,
        STAGE_GROUNDING,
        STAGE_INITIAL_GROUNDING,
        STAGE_OOV_GROUNDING,
        STAGE_RELATIONSHIP_SCREENING,
        STAGE_UNIT_SCREENING,
        RuleCatalog,
    )

    if arm == "a":
        cat = _catalog(STAGE_INITIAL_GROUNDING, field)
    elif arm == "a_oov":
        cat = _catalog(STAGE_OOV_GROUNDING, field)
    elif arm == "b":
        cat = _catalog(STAGE_GROUNDING, field)
    elif arm == "fb":
        cat = _catalog(STAGE_FREEHAND_GROUNDING, field)
    elif arm == "fa":
        name = f"{FIELD_PREFIX[field]}_phrase_freehand_grounding"
        cat = RuleCatalog.model_validate_json((TODAY / f"{name}.json").read_text(encoding="utf-8"))
        return (TODAY / f"{name}.txt").read_text(encoding="utf-8"), _schema(cat), cat
    elif arm == "sa":
        cat = _catalog(STAGE_RELATIONSHIP_SCREENING, field)
    elif arm == "sb":
        cat = _catalog(STAGE_UNIT_SCREENING, field)
    else:
        raise SystemExit(f"unknown arm {arm!r}")
    return _assembled_text(cat), _schema(cat), cat


def oov_payloads(payloads: dict[str, dict[str, Any]], iv_answer_text: str, field: str) -> dict[str, dict[str, Any]]:
    """Arm a's second call: the same records, each carrying what the first call
    identified (production's ``already_identified``)."""
    from core.models.extraction_schemas.synthesis import GroupRecord  # noqa: E402
    from core.models.rule_catalog import STAGE_INITIAL_GROUNDING  # noqa: E402
    from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (  # noqa: E402
        build_group_record_payloads,
        parse_record_grounding_result,
    )
    from core.utils.rdf_to_graph_util import get_match_label_to_concept_map  # noqa: E402

    labels = list(get_match_label_to_concept_map(load_concepts(field)).keys())
    parsed = parse_record_grounding_result(
        iv_answer_text, catalog=_catalog(STAGE_INITIAL_GROUNDING, field), allowed_labels=labels
    )
    records = {gid: GroupRecord(**p) for gid, p in payloads.items()}
    return build_group_record_payloads(
        records, already_identified={gid: sorted(entry.tags) for gid, entry in parsed.items()}
    )


# --- mode candidates (what the screening arms are handed) -----------------------


def _canonical(option: str, labels: dict[str, Any]) -> Optional[str]:
    from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (  # noqa: E402
        _strip_trailing_parenthetical,
    )

    concept = labels.get(option.strip().casefold())
    if concept is None:
        concept = labels.get(_strip_trailing_parenthetical(option).casefold())
    return concept.name if concept is not None else None


def labels_per_record_a(iv_text: str, oov_text: str, labels: dict[str, Any]) -> dict[str, set[str]]:
    per_record: dict[str, set[str]] = defaultdict(set)
    for text, minted in ((iv_text, False), (oov_text, True)):
        for entry in json.loads(text).get("groundings") or []:
            rid = entry["record_id"]
            per_record.setdefault(rid, set())
            for unit in entry.get("options") or entry.get("candidates") or []:
                label = unit.get("option") or unit.get("candidate")
                canon = _canonical(label, labels)
                if canon:
                    per_record[rid].add(canon)
                elif minted:
                    per_record[rid].add(label)
    return per_record


def labels_per_record_b(text: str, labels: dict[str, Any]) -> dict[str, set[str]]:
    per_record: dict[str, set[str]] = defaultdict(set)
    doc = json.loads(text)
    for entry in doc.get("matched") or []:
        canon = _canonical(entry["option"], labels)
        label = canon or entry["option"]  # a non-label in matched is rerouted to a proposal (V3)
        for rec in entry.get("records") or []:
            per_record[rec["record_id"]].add(label)
    for entry in doc.get("proposed") or []:
        canon = _canonical(entry["label"], labels)
        for rec in entry.get("records") or []:
            per_record[rec["record_id"]].add(canon or entry["label"])
    for entry in doc.get("unmatched") or []:
        per_record.setdefault(entry["record_id"], set())
    return per_record


def labels_per_record_freehand(text: str) -> dict[str, set[str]]:
    per_record: dict[str, set[str]] = defaultdict(set)
    for entry in json.loads(text).get("groundings") or []:
        rid = entry["record_id"]
        per_record.setdefault(rid, set())
        for unit in entry.get("candidates") or []:
            per_record[rid].add(unit["candidate"])
    return per_record


def mode_candidates(t: Target, source_arm: str) -> dict[str, list[str]]:
    """Per record, the labels present in a majority of the source arm's repeats,
    casefold-deduped (the shape ``candidates_for_screening`` produces)."""
    d = OUT / t.tag
    labels = concept_by_label(t.field) if t.concept else {}
    runs: list[dict[str, set[str]]] = []
    k = 0
    while True:
        if source_arm == "a":
            iv, oov = d / f"a_iv_{k}.json", d / f"a_oov_{k}.json"
            if not (iv.exists() and oov.exists()):
                break
            runs.append(labels_per_record_a(iv.read_text(), oov.read_text(), labels))
        elif source_arm == "b":
            p = d / f"b_{k}.json"
            if not p.exists():
                break
            runs.append(labels_per_record_b(p.read_text(), labels))
        elif source_arm in ("fa", "fb"):
            p = d / f"{source_arm}_{k}.json"
            if not p.exists():
                break
            runs.append(labels_per_record_freehand(p.read_text()))
        else:
            raise SystemExit(f"cannot screen from arm {source_arm!r}")
        k += 1
    if not runs:
        raise SystemExit(f"{t.tag}: no outputs of arm {source_arm} to screen from; run it first")
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for r in runs:
        for rid, ls in r.items():
            for label in ls:
                counts[rid][label] += 1
    need = len(runs) / 2
    out: dict[str, list[str]] = {}
    for rid, c in counts.items():
        seen: set[str] = set()
        kept: list[str] = []
        for label, n in sorted(c.items()):
            if n > need and label.casefold() not in seen:
                seen.add(label.casefold())
                kept.append(label)
        if kept:
            out[rid] = kept
    return out


# --- screening requests -----------------------------------------------------------


def render_units_block(units: list[dict[str, Any]]) -> str:
    """The UNITS block of a unit-screening request: one JSON object per line
    inside fences, mirroring the records block so the parser can read back
    exactly what was asked (the candidate axis of the hold)."""
    lines = ",\n".join(json.dumps(u, ensure_ascii=False) for u in units)
    return f"{UNITS_OPEN}\n[\n{lines}\n]\n{UNITS_CLOSE}"


def build_units(t: Target, candidates: dict[str, list[str]]) -> list[dict[str, Any]]:
    labels = concept_by_label(t.field) if t.concept else {}
    by_option: dict[str, set[str]] = defaultdict(set)
    for rid, ls in candidates.items():
        for label in ls:
            by_option[label].add(rid)
    units: list[dict[str, Any]] = []
    for option in sorted(by_option):
        unit: dict[str, Any] = {"option": option}
        concept = labels.get(option.casefold())
        if concept is not None and (concept.definition or "").strip():
            unit["meaning"] = concept.definition.strip()
        unit["records"] = sorted(by_option[option])
        units.append(unit)
    return units


def render_screening_context(
    t: Target,
    payloads: dict[str, dict[str, Any]],
    candidates: dict[str, list[str]],
    *,
    arm: str,
    nonce: Optional[str] = None,
) -> str:
    from core.models.extraction_schemas.synthesis import GroupRecord  # noqa: E402
    from core.services.phrase_blocks_contract import render_record_blocks  # noqa: E402
    from core.services.pipeline_nodes.multi_stage.llm_relationship_screening_node_service import (  # noqa: E402
        build_screening_payloads,
    )

    head = f"{NONCE_LABEL}{nonce or uuid.uuid4().hex}\n\n{MANUFACTURER_LABEL}{subject_name(t.subject)}\n\n"
    if arm == "sa":
        records = {gid: GroupRecord(**p) for gid, p in payloads.items()}
        return head + render_record_blocks(build_screening_payloads(records, candidates))
    if arm == "sb":
        cited = {rid: payloads[rid] for rid in sorted(candidates)}
        return head + render_record_blocks(cited) + "\n\n" + render_units_block(build_units(t, candidates))
    raise SystemExit(f"unknown screening arm {arm!r}")


# --- run ----------------------------------------------------------------------


def write_requests(run: str, targets: list[Target], screen_from: Optional[str]) -> None:
    for t in targets:
        d = OUT / t.tag
        d.mkdir(parents=True, exist_ok=True)
        payloads = t.payloads(run)
        arms = ("a", "b") if t.concept else ("fa",)
        for arm in arms:
            (d / f"request_{arm}_user.txt").write_text(
                render_context(payloads, t.field, arm=arm, nonce="0" * 32), encoding="utf-8"
            )
        (d / "request_meta.json").write_text(
            json.dumps(
                {"run": run, "subject": t.subject, "field": t.field, "chunk": t.chunk, "group_index": t.group_index, "record_ids": sorted(payloads)},
                indent=1,
            )
        )
        if screen_from:
            candidates = mode_candidates(t, screen_from)
            for arm in SCREENING_ARMS:
                (d / f"request_{arm}_user.txt").write_text(
                    render_screening_context(t, payloads, candidates, arm=arm, nonce="0" * 32), encoding="utf-8"
                )
            (d / f"candidates_from_{screen_from}.json").write_text(json.dumps(candidates, indent=1, ensure_ascii=False))
        print(f"{t.tag}: {len(payloads)} records written", file=sys.stderr)


async def run(run_id: str, targets: list[Target], arms: list[str], n: int, concurrency: int, screen_from: Optional[str]) -> None:
    from llm_providers.models.llm_model import GPT_4_1  # noqa: E402
    from llm_providers.models.open_ai.gpt_model_params import GPTModelParams  # noqa: E402
    from llm_providers.utils.ask_llm_util import fetch_llm_chat_completion_result  # noqa: E402

    _catalog_registry()
    base = GPTModelParams(
        temperature=0, top_p=1, presence_penalty=0, frequency_penalty=0, seed=12345,
        max_completion_tokens=10_000, response_format={"type": "json_object"},
    )
    gate = asyncio.Semaphore(concurrency)

    async def call(out: pathlib.Path, context: str, system: str, schema: dict) -> None:
        if out.exists() and out.stat().st_size:
            return
        async with gate:
            started = time.monotonic()
            response, _timing = await fetch_llm_chat_completion_result(
                context=context, prompt=system, gpt_model=GPT_4_1, model_params=base.with_response_format(schema)
            )
            seconds = time.monotonic() - started
        text = response.choices[0].message.content or ""
        usage = getattr(response, "usage", None)
        out.write_text(text, encoding="utf-8")
        out.with_suffix(".usage.json").write_text(
            json.dumps(
                {
                    "prompt_tokens": getattr(usage, "prompt_tokens", None),
                    "completion_tokens": getattr(usage, "completion_tokens", None),
                    "seconds": round(seconds, 1),
                    "finish_reason": response.choices[0].finish_reason,
                }
            )
        )
        print(f"{out.parent.name} {out.stem}: {len(text):,} B in {seconds:.0f}s", file=sys.stderr)

    async def one(t: Target, arm: str, k: int) -> None:
        d = OUT / t.tag
        d.mkdir(parents=True, exist_ok=True)
        payloads = t.payloads(run_id)
        if arm == "a":
            if not t.concept:
                return
            iv, oov = d / f"a_iv_{k}.json", d / f"a_oov_{k}.json"
            system, schema, _ = arm_system_and_schema("a", t.field)
            await call(iv, render_context(payloads, t.field, arm="a"), system, schema)
            system, schema, _ = arm_system_and_schema("a_oov", t.field)
            second = oov_payloads(payloads, iv.read_text(encoding="utf-8"), t.field)
            await call(oov, render_context(second, t.field, arm="a", oov=True), system, schema)
        elif arm == "b":
            if not t.concept:
                return
            system, schema, _ = arm_system_and_schema("b", t.field)
            await call(d / f"b_{k}.json", render_context(payloads, t.field, arm="b"), system, schema)
        elif arm in ("fa", "fb"):
            if t.concept:
                return
            system, schema, _ = arm_system_and_schema(arm, t.field)
            await call(d / f"{arm}_{k}.json", render_context(payloads, t.field, arm=arm), system, schema)
        elif arm in SCREENING_ARMS:
            source = screen_from or ("b" if t.concept else "fb")
            candidates = mode_candidates(t, source)
            (d / f"candidates_from_{source}.json").write_text(json.dumps(candidates, indent=1, ensure_ascii=False))
            if not candidates:
                print(f"{t.tag}: no mode candidates from {source}; nothing to screen", file=sys.stderr)
                return
            system, schema, _ = arm_system_and_schema(arm, t.field)
            await call(d / f"{arm}_{k}.json", render_screening_context(t, payloads, candidates, arm=arm), system, schema)
        else:
            raise SystemExit(f"unknown arm {arm!r}")

    await asyncio.gather(*[one(t, a, k) for t in targets for a in arms for k in range(n)])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", default=DEFAULT_RUN)
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--targets", nargs="*", default=[])
    ap.add_argument("--arms", nargs="+", default=["a", "b", "fa", "fb"])
    ap.add_argument("--screen-from", default=None, help="grounding arm whose MODE candidates the screening arms judge (default b / fb)")
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--concurrency", type=int, default=6)
    args = ap.parse_args()
    if args.list:
        list_targets(args.run)
        return
    targets = [Target(s) for s in args.targets]
    if not targets:
        raise SystemExit("no targets; use --list to see chunks and --targets subject:field:start:end:group_index ...")
    if args.write:
        _catalog_registry()
        write_requests(args.run, targets, args.screen_from)
        return
    asyncio.run(run(args.run, targets, args.arms, args.n, args.concurrency, args.screen_from))


if __name__ == "__main__":
    main()
