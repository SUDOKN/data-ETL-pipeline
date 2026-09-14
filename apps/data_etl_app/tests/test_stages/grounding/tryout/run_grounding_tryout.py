"""Grounding-shape tryout: today's two calls (arm ``a``) against the drafted
three-list call (arm ``b``) on REAL records, in the production call shape.

Why this exists (grounding-gap survey, Phase 4, 2026-09-14): the option-major
grounding rebuild cannot be measured on the pipeline before it is built, but
its own gap classes — coverage violations, membership drops, multi-record
inference, proposal flood, evidence-citation completeness, per-option
stability — are properties of the model's answer to a prompt, and a prompt can
be tried on the records of a finished synthesis run. Every arm runs N repeats
(default 5) and is read as a mode, never as one sample (the per-request coin,
design doc §23).

Requests are rebuilt from the run's partial dumps with no Mongo: a chunk's
synthesis block carries every group's focal form and paragraph, and a grounding
request is exactly ``render_record_blocks(group payloads)`` plus the options
outline, behind the per-request nonce line (``llm_grounding_node_service
.create_deferred_record_grounding_gpt_request`` / ``create_base_gpt_batch_request``).
The vocabulary comes from the repo's ontology JSON, rendered by the same
``render_concept_outline`` production uses.

Arm ``a`` = the published initial-grounding prompt with its catalog-generated
schema, then the published OOV prompt chained on that answer (as production
does). Arm ``b`` = ``arms/<field>_three_list_system.txt`` (the draft the user
approved in chat) with the three-list schema below. Outputs land under
``out/<tag>/`` as raw response text; ``check_tryout.py`` reads them.

Usage (from the repo root):
    .venv/bin/python apps/data_etl_app/tests/test_stages/grounding/tryout/run_grounding_tryout.py --list --run 20260912T191548
    ... --targets tanfel.com:process_caps:0:81109:0 mathewsco.com:process_caps:0:43039:0 --write
    ... --targets ... --arms a b --n 5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import uuid
from typing import Any, Literal, Optional, cast

from pydantic import BaseModel, ConfigDict, Field

ROOT = next(p for p in pathlib.Path(__file__).resolve().parents if (p / ".git").exists())
for _line in (ROOT / ".env").read_text().splitlines() if (ROOT / ".env").exists() else []:
    if "=" in _line and not _line.startswith("#"):
        _k, _v = _line.split("=", 1)
        os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

HERE = pathlib.Path(__file__).resolve().parent
ARMS = HERE / "arms"
OUT = HERE / "out"
DUMPS = ROOT / "packages" / "logs" / "extraction_dumps"
PROMPTS = ROOT / "apps" / "data_etl_app" / "src" / "data_etl_app" / "knowledge" / "prompts"
ASSEMBLED = PROMPTS / "final_texts" / "assembled" / "multi_stage"
ONTOLOGY = ROOT / "apps" / "data_etl_app" / "src" / "data_etl_app" / "knowledge" / "ontology"

NONCE_LABEL = "request nonce (ignore): "
MAX_RECORDS_PER_REQUEST = 25  # ExtractionPipelineFactory.DEFAULT_INITIAL_GROUNDING_MAX_PAIRS_PER_REQUEST

FIELD_PREFIX = {
    "industries": "industry",
    "material_caps": "material_cap",
    "process_caps": "process_cap",
    "conformity_attestations": "conformity_attestation",
}
FIELD_ONTOLOGY = {
    "industries": "industries.json",
    "material_caps": "material_caps.json",
    "process_caps": "process_caps.json",
    "conformity_attestations": "certificates.json",
}


# --- the three-list wire (arm b), per NEW_SHAPE_SPEC.md ----------------------


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class EvidenceReport(_Strict):
    outcome: Literal["satisfied"]
    explanation: str


class ChosenBranch(_Strict):
    rule_id: Literal["IGR-M1", "IGR-M2"]
    explanation: str


class MatchedEntry(_Strict):
    option: str
    supporting_records: list[str]
    IGR_E1: EvidenceReport = Field(alias="IGR-E1")
    chosen: ChosenBranch


class ProposedEntry(_Strict):
    label: str
    supporting_records: list[str]
    explanation: str


class UnmatchedEntry(_Strict):
    record_id: str
    explanation: str


class ThreeListResponse(_Strict):
    matched: list[MatchedEntry]
    proposed: list[ProposedEntry]
    unmatched: list[UnmatchedEntry]


# --- records and vocabulary ---------------------------------------------------


def load_concepts(field: str) -> set[Any]:
    from core.utils.rdf_to_graph_util import tree_list_to_flat  # noqa: E402

    doc = json.loads((ONTOLOGY / FIELD_ONTOLOGY[field]).read_text(encoding="utf-8"))
    body = next(v for v in doc.values() if isinstance(v, list))
    return tree_list_to_flat(cast(Any, body))


def options_section(field: str, *, oov: bool = False) -> str:
    from core.utils.rdf_to_graph_util import render_concept_outline  # noqa: E402

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
    payloads: dict[str, dict[str, Any]], field: str, *, oov: bool = False, nonce: Optional[str] = None
) -> str:
    from core.services.phrase_blocks_contract import render_record_blocks  # noqa: E402

    body = f"{render_record_blocks(payloads)}\n\n{options_section(field, oov=oov)}"
    return f"{NONCE_LABEL}{nonce or uuid.uuid4().hex}\n\n{body}"


# --- targets ------------------------------------------------------------------


class Target:
    def __init__(self, spec: str) -> None:
        parts = spec.split(":")
        if len(parts) != 5:
            raise SystemExit(f"target {spec!r}: want subject:field:start:end:group_index")
        self.subject, self.field = parts[0], parts[1]
        self.chunk = f"{parts[2]}:{parts[3]}"
        self.group_index = int(parts[4])
        self.tag = f"{self.subject.split('.')[0]}_{FIELD_PREFIX.get(self.field, self.field)}_{parts[2]}_g{self.group_index}"

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


# --- arms ---------------------------------------------------------------------


def _catalog_registry() -> None:
    from core.services.rule_catalog_registry import set_rule_catalog_lookup  # noqa: E402
    from data_etl_app.services.prompt_assembly_service import build_rule_catalog_lookup  # noqa: E402

    set_rule_catalog_lookup(build_rule_catalog_lookup())


def arm_a_system(field: str, *, oov: bool) -> str:
    stage = "5_oov_grounding" if oov else "5_initial_grounding"
    kind = "oov" if oov else "initial"
    return (ASSEMBLED / stage / f"{FIELD_PREFIX[field]}_phrase_{kind}_grounding.txt").read_text(encoding="utf-8")


def arm_a_schema(field: str, *, oov: bool) -> dict:
    from core.models.extraction_schemas.catalog_wire_schema import response_format_for  # noqa: E402
    from core.models.rule_catalog import STAGE_INITIAL_GROUNDING, STAGE_OOV_GROUNDING  # noqa: E402
    from core.services.rule_catalog_registry import get_rule_catalog  # noqa: E402

    return response_format_for(get_rule_catalog(STAGE_OOV_GROUNDING if oov else STAGE_INITIAL_GROUNDING, field))


def arm_b_system(field: str) -> str:
    path = ARMS / f"{field}_three_list_system.txt"
    if not path.exists():
        raise SystemExit(f"arm b needs the approved draft at {path}")
    return path.read_text(encoding="utf-8")


def arm_b_schema() -> dict:
    from core.models.extraction_schemas.response_format_util import build_gpt_response_format  # noqa: E402

    return build_gpt_response_format(ThreeListResponse, name="grounding_three_list_result")


def oov_payloads(payloads: dict[str, dict[str, Any]], iv_answer_text: str, field: str) -> dict[str, dict[str, Any]]:
    """Arm a's second call: the same records, each carrying what the first call
    identified (production's ``already_identified``)."""
    from core.models.rule_catalog import STAGE_INITIAL_GROUNDING  # noqa: E402
    from core.services.pipeline_nodes.multi_stage.llm_grounding_node_service import (  # noqa: E402
        build_group_record_payloads,
        parse_record_grounding_result,
    )
    from core.services.rule_catalog_registry import get_rule_catalog  # noqa: E402
    from core.utils.rdf_to_graph_util import get_match_label_to_concept_map  # noqa: E402
    from core.models.extraction_schemas.synthesis import GroupRecord  # noqa: E402

    labels = list(get_match_label_to_concept_map(load_concepts(field)).keys())
    parsed = parse_record_grounding_result(
        iv_answer_text, catalog=get_rule_catalog(STAGE_INITIAL_GROUNDING, field), allowed_labels=labels
    )
    records = {gid: GroupRecord(**p) for gid, p in payloads.items()}
    return build_group_record_payloads(
        records, already_identified={gid: sorted(entry.tags) for gid, entry in parsed.items()}
    )


# --- run ----------------------------------------------------------------------


def write_requests(run: str, targets: list[Target]) -> None:
    for t in targets:
        d = OUT / t.tag
        d.mkdir(parents=True, exist_ok=True)
        payloads = t.payloads(run)
        (d / "request_a_user.txt").write_text(render_context(payloads, t.field, nonce="0" * 32), encoding="utf-8")
        (d / "request_meta.json").write_text(
            json.dumps(
                {"run": run, "subject": t.subject, "field": t.field, "chunk": t.chunk, "group_index": t.group_index, "record_ids": sorted(payloads)},
                indent=1,
            )
        )
        print(f"{t.tag}: {len(payloads)} records; request_a_user.txt {len((d / 'request_a_user.txt').read_text()):,} chars", file=sys.stderr)


async def run(run_id: str, targets: list[Target], arms: list[str], n: int, concurrency: int) -> None:
    from llm_providers.models.llm_model import GPT_4_1  # noqa: E402
    from llm_providers.models.open_ai.gpt_model_params import GPTModelParams  # noqa: E402
    from llm_providers.utils.ask_llm_util import ask_gpt  # noqa: E402

    _catalog_registry()
    base = GPTModelParams(
        temperature=0, top_p=1, presence_penalty=0, frequency_penalty=0, seed=12345,
        max_completion_tokens=10_000, response_format={"type": "json_object"},
    )
    gate = asyncio.Semaphore(concurrency)

    async def call(context: str, system: str, schema: dict) -> str:
        async with gate:
            text = await ask_gpt(context=context, prompt=system, gpt_model=GPT_4_1, model_params=base.with_response_format(schema))
        return text or ""

    async def one(t: Target, arm: str, k: int) -> None:
        d = OUT / t.tag
        d.mkdir(parents=True, exist_ok=True)
        payloads = t.payloads(run_id)
        if arm == "a":
            iv = d / f"a_iv_{k}.json"
            oov = d / f"a_oov_{k}.json"
            if not (iv.exists() and iv.stat().st_size):
                iv.write_text(await call(render_context(payloads, t.field), arm_a_system(t.field, oov=False), arm_a_schema(t.field, oov=False)), encoding="utf-8")
            if not (oov.exists() and oov.stat().st_size):
                second = oov_payloads(payloads, iv.read_text(encoding="utf-8"), t.field)
                oov.write_text(await call(render_context(second, t.field, oov=True), arm_a_system(t.field, oov=True), arm_a_schema(t.field, oov=True)), encoding="utf-8")
            print(f"{t.tag} a #{k}: iv {iv.stat().st_size:,} B, oov {oov.stat().st_size:,} B", file=sys.stderr)
        elif arm == "b":
            out = d / f"b_{k}.json"
            if out.exists() and out.stat().st_size:
                return
            out.write_text(await call(render_context(payloads, t.field), arm_b_system(t.field), arm_b_schema()), encoding="utf-8")
            print(f"{t.tag} b #{k}: {out.stat().st_size:,} B", file=sys.stderr)
        else:
            raise SystemExit(f"unknown arm {arm!r}")

    await asyncio.gather(*[one(t, a, k) for t in targets for a in arms for k in range(n)])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", default="20260912T191548")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--targets", nargs="*", default=[])
    ap.add_argument("--arms", nargs="+", default=["a", "b"])
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
        write_requests(args.run, targets)
        return
    asyncio.run(run(args.run, targets, args.arms, args.n, args.concurrency))


if __name__ == "__main__":
    main()
