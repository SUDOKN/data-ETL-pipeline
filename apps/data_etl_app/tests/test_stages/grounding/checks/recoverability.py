"""Recoverability readout: which grounding defects screening could, could not, or
did not catch, plus the enumeration that lets judges answer that per item.

The grounding stages ENUMERATE candidates; screening can only REMOVE them, and
it tests two things (substance, party) plus one guard (aspirational or
discontinued involvement). So every grounding defect falls into one of three
bins that this readout makes countable:

- structurally unrecoverable: no screening rule tests it (wrong axis, vague,
  bridge, plausible fabrication, wrong granularity, every descent hop);
- recoverable in principle: a screening rule does test it (another party's
  dealing, nothing substantive) — and then either CAUGHT (screening failed the
  tag) or SHIPPED (screening passed it anyway);
- omitted: the label was never emitted at all (a false decline, a dropped
  option, a broader match, a coinage split) — nothing downstream can add it.

This module is the CODE half of the 2026-09-14 grounding-gap survey and stays in
the harness so any later run can recompute it. It judges nothing semantic:
`enumerate_items` lists every tag instance, decline and descent hop with the
record context and the MECHANICAL screening outcome; `mechanical_matrix` counts
them; `write_packets` / `write_omission_sample` hand them to judge agents;
`merge_verdicts` joins the judges' codes back onto the mechanical outcomes and
writes the recoverability matrix. Codes are the harness TAXONOMY's (D/N/X/V/B/P/F,
DS/DF/DR, H-*), extended by the survey's fields (`recoverable_by`,
`narrower_evidenced`, `lost_label`) documented in the survey's addendum.

Usage (from the instrument directory):

    .venv/bin/python checks/recoverability.py --run <run_id> --out <dir>
        [--packet-size 170] [--omission-per-dump 43] [--seed 20260914]
    .venv/bin/python checks/recoverability.py --run <run_id> --out <dir> --merge

``--merge`` overlays ``verify/corrections_*.jsonl`` on the census verdicts and
``omission_verify/corrections_*.jsonl`` on the oracle verdicts (full replacement
rows written by the Opus verifiers) before joining.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterator, Optional

HERE = Path(__file__).resolve().parent
INSTRUMENT = HERE.parent
REPO_ROOT = HERE.parents[5]
DEFAULT_DUMPS_ROOT = REPO_ROOT / "packages" / "logs" / "extraction_dumps"
ONTOLOGY_DIR = (
    REPO_ROOT / "apps" / "data_etl_app" / "src" / "data_etl_app" / "knowledge" / "ontology"
)

CONCEPT_FIELDS = {"industries", "material_caps", "process_caps", "conformity_attestations"}
FREEHAND_FIELDS = {"products", "contract_products", "equipments"}
PHRASE_FIELDS = CONCEPT_FIELDS | FREEHAND_FIELDS

FIELD_TO_ONTOLOGY_FILE = {
    "industries": "industries.json",
    "material_caps": "material_caps.json",
    "process_caps": "process_caps.json",
    "conformity_attestations": "certificates.json",
}

# Dump block -> the stage name the survey and the census use.
GROUNDING_BLOCKS = {
    "in_vocab_grounding": "in_vocab",
    "oov_grounding": "oov",
    "freehand_grounding": "freehand",
}

# Rows that never reached grounding carry no judgeable item and no paragraph.
UNSYNTHESIZED_STATUSES = {"no_mentions", "not_synthesized"}

TAG_CODES = ("D", "N", "X", "V", "B", "P", "F")
CLEAN_CODES = {"D", "N"}
DECLINE_CODES = ("DS", "DF", "DR")
HOP_CODES = ("H-OK", "H-BRIDGE", "H-FORCED", "H-STOP")
# The survey's answer to "which screening rule, applied faithfully to this
# record, would reject this tag": a party test, a substance test, the guard, or
# none of them (the defect is outside what screening tests). `n/a` on clean
# items.
RECOVERABLE_BY = ("SCR-1", "SCR-2", "guard", "none", "n/a")


# ---------------------------------------------------------------------------
# Enumeration
# ---------------------------------------------------------------------------


def _short_id(*parts: str) -> str:
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()[:10]


def _subject_slug(subject: str) -> str:
    return subject.replace(".", "_")


def load_dumps(
    run_dir: Path, subject: Optional[str] = None, field: Optional[str] = None
) -> list[dict[str, Any]]:
    """Every phrase-field dump of the run, as loaded JSON (twin-safe: rows keep
    their chunk bounds)."""
    docs: list[dict[str, Any]] = []
    for path in sorted(run_dir.glob("*.json")):
        doc = json.loads(path.read_text())
        if doc.get("field_type") not in PHRASE_FIELDS:
            continue
        if subject and doc["subject_unique_id"] not in (subject, subject.replace("_", ".")):
            continue
        if field and doc["field_type"] != field:
            continue
        docs.append(doc)
    return docs


def _iter_rows(doc: dict[str, Any]) -> Iterator[tuple[str, dict[str, Any]]]:
    for bounds, chunk in (doc.get("chunks") or {}).items():
        for row in chunk.get("rows") or []:
            yield bounds, row


def _row_tag_set(row: dict[str, Any]) -> frozenset[str]:
    tags: set[str] = set()
    for block in GROUNDING_BLOCKS:
        tags.update(((row.get(block) or {}).get("tags") or {}).keys())
    return frozenset(tags)


def _twin_index(doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """group_id -> {"twin": bool, "divergent": bool} over the dump's chunks.
    A twin is a group synthesized in more than one chunk; divergent when the
    status or the emitted tag set differs between them (the census's 51%)."""
    signatures: dict[str, set[tuple[Optional[str], frozenset[str]]]] = defaultdict(set)
    for _bounds, row in _iter_rows(doc):
        signatures[row["group_id"]].add((row.get("status"), _row_tag_set(row)))
    counts: Counter[str] = Counter(row["group_id"] for _b, row in _iter_rows(doc))
    return {
        gid: {"twin": counts[gid] > 1, "divergent": counts[gid] > 1 and len(sigs) > 1}
        for gid, sigs in signatures.items()
    }


def _screening_of(row: dict[str, Any], tag: str) -> dict[str, Any]:
    verdicts = row.get("screening")
    if not isinstance(verdicts, dict) or tag not in verdicts:
        return {"verdict": "absent", "rules": []}
    verdict = verdicts[tag] or {}
    return {
        "verdict": "passed" if verdict.get("passed") else "failed",
        "rules": list(verdict.get("applied_rules") or []),
    }


def _base_item(
    doc: dict[str, Any], bounds: str, row: dict[str, Any], twins: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    record = row.get("record") or {}
    gid = row["group_id"]
    return {
        "run_id": doc.get("timestamp"),
        "subject": doc["subject_unique_id"],
        "subject_slug": _subject_slug(doc["subject_unique_id"]),
        "field": doc["field_type"],
        "chunk": bounds,
        "group_id": gid,
        "focal_form": row.get("focal_form") or record.get("focal_form"),
        "forms": list(row.get("forms") or []),
        "synthesis": record.get("synthesis"),
        "row_status": row.get("status"),
        "twin": twins.get(gid, {}).get("twin", False),
        "twin_divergent": twins.get(gid, {}).get("divergent", False),
    }


def enumerate_items(
    run_dir: Path, subject: Optional[str] = None, field: Optional[str] = None
) -> list[dict[str, Any]]:
    """Every judgeable item of the run: one per tag instance (per grounding
    block), one per decline, one per descent hop. Each carries the record it
    was judged on and the mechanical screening outcome, so a judge reads the
    record and never has to open the dump."""
    items: list[dict[str, Any]] = []
    for doc in load_dumps(run_dir, subject, field):
        twins = _twin_index(doc)
        for bounds, row in _iter_rows(doc):
            base = _base_item(doc, bounds, row, twins)
            key_prefix = (base["subject"], base["field"], bounds, base["group_id"])
            # The labels descent reached on this row, so a judge's "a narrower
            # label was evidenced" can be checked against what descent did.
            descent_reached = sorted(
                {
                    str(node.get("group_id"))
                    for nodes in (row.get("lvl_by_lvl_itps") or {}).values()
                    for node in nodes
                    if node.get("origin") in ("recursive_grounding", "both")
                }
            )
            for block, stage in GROUNDING_BLOCKS.items():
                pass_dump = row.get(block)
                if not isinstance(pass_dump, dict):
                    continue
                dropped = list(pass_dump.get("dropped_options") or [])
                for tag, rules in (pass_dump.get("tags") or {}).items():
                    screening = _screening_of(row, tag)
                    items.append(
                        {
                            **base,
                            "item_id": "t_" + _short_id(*key_prefix, stage, tag),
                            "kind": "tag",
                            "stage": stage,
                            "tag": tag,
                            "rules": list(rules or []),
                            "screening": screening,
                            "shipped": base["row_status"] == "grounded"
                            and screening["verdict"] == "passed",
                            "dropped_options": dropped,
                            "descent_reached": descent_reached,
                        }
                    )
                declined = pass_dump.get("declined")
                if isinstance(declined, str):
                    items.append(
                        {
                            **base,
                            "item_id": "d_" + _short_id(*key_prefix, stage),
                            "kind": "decline",
                            "stage": stage,
                            "explanation": declined,
                            "dropped_options": dropped,
                        }
                    )
            for level, nodes in (row.get("lvl_by_lvl_itps") or {}).items():
                for node in nodes:
                    if node.get("origin") not in ("recursive_grounding", "both"):
                        continue
                    child = node.get("group_id")
                    parent = node.get("parent_group_id")
                    iterative = node.get("iterative_og_tag_w_rules") or {}
                    rules = [rule for rules in iterative.values() for rule in rules]
                    items.append(
                        {
                            **base,
                            "item_id": "h_"
                            + _short_id(*key_prefix, str(level), str(parent), str(child)),
                            "kind": "hop",
                            "stage": "descent",
                            "level": int(level),
                            "parent": parent,
                            "child": child,
                            "rules": rules,
                            "stop_reason": node.get("stop_reason"),
                            "in_vocab": bool(node.get("in_vocab")),
                            "screening": _screening_of(row, str(child)),
                            "shipped": base["row_status"] == "grounded",
                        }
                    )
    return items


# ---------------------------------------------------------------------------
# The mechanical matrix (no judgment)
# ---------------------------------------------------------------------------


def _decoration_stripped(option: str) -> str:
    return option.split(" (also:")[0].strip()


def load_label_set(field: str) -> set[str]:
    fname = FIELD_TO_ONTOLOGY_FILE.get(field)
    if fname is None:
        return set()
    doc = json.loads((ONTOLOGY_DIR / fname).read_text())
    body = next(v for v in doc.values() if isinstance(v, list))
    labels: set[str] = set()

    def walk(nodes: list[dict[str, Any]]) -> None:
        for n in nodes:
            labels.add(n["name"])
            labels.update(n.get("altLabels") or [])
            walk(n.get("children") or [])

    walk(body)
    return labels


def mechanical_matrix(items: list[dict[str, Any]]) -> dict[str, Any]:
    """Counts a reader can check against the dumps: tags by stage x screening
    verdict, shipped tags, declines by stage, drops (false and recaptured),
    hops, twins. Per (subject, field) and overall."""
    per: dict[str, dict[str, Any]] = {}
    labels_by_field: dict[str, set[str]] = {}

    def bucket(key: str) -> dict[str, Any]:
        return per.setdefault(
            key,
            {
                "tags": Counter(),
                "tags_shipped": Counter(),
                "screening": Counter(),
                "declines": Counter(),
                "drops": {"total": 0, "false_decoration": 0, "recaptured_by_oov": 0},
                "hops": 0,
                "hops_screened": 0,
                "twin_items": 0,
                "twin_divergent_items": 0,
            },
        )

    oov_tags_by_row: dict[tuple[str, str, str, str], set[str]] = defaultdict(set)
    for item in items:
        if item["kind"] == "tag" and item["stage"] == "oov":
            oov_tags_by_row[
                (item["subject"], item["field"], item["chunk"], item["group_id"])
            ].add(item["tag"].casefold())

    seen_drop_rows: set[tuple[str, str, str, str, str]] = set()
    for item in items:
        key = f"{item['subject_slug']}__{item['field']}"
        b = bucket(key)
        if item["twin"]:
            b["twin_items"] += 1
        if item["twin_divergent"]:
            b["twin_divergent_items"] += 1
        if item["kind"] == "tag":
            b["tags"][item["stage"]] += 1
            b["screening"][f"{item['stage']}:{item['screening']['verdict']}"] += 1
            if item["shipped"]:
                b["tags_shipped"][item["stage"]] += 1
        elif item["kind"] == "decline":
            b["declines"][item["stage"]] += 1
        elif item["kind"] == "hop":
            b["hops"] += 1
            if item["screening"]["verdict"] != "absent":
                b["hops_screened"] += 1
        # Drops ride on the block; count each (row, stage) once.
        drop_key = (item["subject"], item["field"], item["chunk"], item["group_id"], item.get("stage", ""))
        if item.get("dropped_options") and drop_key not in seen_drop_rows:
            seen_drop_rows.add(drop_key)
            field = item["field"]
            labels = labels_by_field.setdefault(field, load_label_set(field))
            row_key = (item["subject"], item["field"], item["chunk"], item["group_id"])
            for option in item["dropped_options"]:
                b["drops"]["total"] += 1
                stripped = _decoration_stripped(option)
                if labels and option not in labels and stripped in labels:
                    b["drops"]["false_decoration"] += 1
                elif stripped.casefold() in oov_tags_by_row.get(row_key, set()):
                    b["drops"]["recaptured_by_oov"] += 1

    total = bucket("ALL")
    for key, b in list(per.items()):
        if key == "ALL":
            continue
        total["tags"].update(b["tags"])
        total["tags_shipped"].update(b["tags_shipped"])
        total["screening"].update(b["screening"])
        total["declines"].update(b["declines"])
        for k in total["drops"]:
            total["drops"][k] += b["drops"][k]
        total["hops"] += b["hops"]
        total["hops_screened"] += b["hops_screened"]
        total["twin_items"] += b["twin_items"]
        total["twin_divergent_items"] += b["twin_divergent_items"]

    return {
        key: {
            **b,
            "tags": dict(b["tags"]),
            "tags_shipped": dict(b["tags_shipped"]),
            "screening": dict(b["screening"]),
            "declines": dict(b["declines"]),
        }
        for key, b in per.items()
    }


def write_mechanical_report(out_dir: Path, matrix: dict[str, Any], n_items: int) -> None:
    lines = [
        "# Mechanical matrix (no judgment)",
        "",
        f"Items enumerated: {n_items}. Screening verdict per tag is read off the dump; "
        "`shipped` = the row is `grounded` and screening passed the tag.",
        "",
        "| dump | tags in_vocab/oov/freehand | passed / failed / absent | shipped | declines | drops (false, oov-recaptured) | hops (screened) | twin items (divergent) |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for key in sorted(k for k in matrix if k != "ALL") + ["ALL"]:
        b = matrix[key]
        tags = b["tags"]
        scr = b["screening"]
        passed = sum(v for k, v in scr.items() if k.endswith(":passed"))
        failed = sum(v for k, v in scr.items() if k.endswith(":failed"))
        absent = sum(v for k, v in scr.items() if k.endswith(":absent"))
        lines.append(
            f"| {key} | {tags.get('in_vocab', 0)}/{tags.get('oov', 0)}/{tags.get('freehand', 0)} "
            f"| {passed} / {failed} / {absent} | {sum(b['tags_shipped'].values())} "
            f"| {sum(b['declines'].values())} "
            f"| {b['drops']['total']} ({b['drops']['false_decoration']}, {b['drops']['recaptured_by_oov']}) "
            f"| {b['hops']} ({b['hops_screened']}) | {b['twin_items']} ({b['twin_divergent_items']}) |"
        )
    (out_dir / "MECHANICAL.md").write_text("\n".join(lines) + "\n")
    (out_dir / "mechanical.json").write_text(json.dumps(matrix, indent=1, sort_keys=True))


# ---------------------------------------------------------------------------
# Judge packets (Phase 2) and the omission sample (Phase 3)
# ---------------------------------------------------------------------------


def write_packets(
    items: list[dict[str, Any]], out_dir: Path, packet_size: int = 170
) -> list[dict[str, Any]]:
    """One JSONL packet per (subject, field), split at `packet_size` items in
    row order so a judge sees one row's tags and declines together."""
    packets_dir = out_dir / "packets"
    packets_dir.mkdir(parents=True, exist_ok=True)
    by_dump: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        by_dump[f"{item['subject_slug']}__{item['field']}"].append(item)
    index: list[dict[str, Any]] = []
    for key in sorted(by_dump):
        rows = by_dump[key]
        rows.sort(key=lambda it: (it["chunk"], it["group_id"], it["kind"], it.get("stage", ""), it.get("tag", "")))
        n_parts = max(1, -(-len(rows) // packet_size))
        per_part = -(-len(rows) // n_parts)
        for part in range(n_parts):
            chunk = rows[part * per_part : (part + 1) * per_part]
            if not chunk:
                continue
            name = f"{key}__p{part + 1}.jsonl"
            with (packets_dir / name).open("w") as fh:
                for item in chunk:
                    fh.write(json.dumps(item, ensure_ascii=False) + "\n")
            index.append(
                {
                    "packet": name,
                    "dump": key,
                    "items": len(chunk),
                    "kinds": dict(Counter(it["kind"] for it in chunk)),
                }
            )
    (packets_dir / "INDEX.json").write_text(json.dumps(index, indent=1))
    return index


def _record_units(run_dir: Path) -> list[dict[str, Any]]:
    """One unit per synthesized row: the record plus everything grounding
    emitted or declined for it — the population the omission probe samples."""
    units: list[dict[str, Any]] = []
    for doc in load_dumps(run_dir):
        twins = _twin_index(doc)
        for bounds, row in _iter_rows(doc):
            if row.get("status") in UNSYNTHESIZED_STATUSES or not (row.get("record") or {}).get("synthesis"):
                continue
            base = _base_item(doc, bounds, row, twins)
            emitted: dict[str, list[str]] = {}
            declined: dict[str, str] = {}
            dropped: list[str] = []
            for block, stage in GROUNDING_BLOCKS.items():
                pass_dump = row.get(block)
                if not isinstance(pass_dump, dict):
                    continue
                if pass_dump.get("tags"):
                    emitted[stage] = sorted(pass_dump["tags"])
                if isinstance(pass_dump.get("declined"), str):
                    declined[stage] = pass_dump["declined"]
                dropped.extend(pass_dump.get("dropped_options") or [])
            descent: list[str] = []
            for _level, nodes in (row.get("lvl_by_lvl_itps") or {}).items():
                for node in nodes:
                    if node.get("origin") in ("recursive_grounding", "both"):
                        descent.append(f"{node.get('parent_group_id')} > {node.get('group_id')}")
            units.append(
                {
                    **base,
                    "item_id": "r_" + _short_id(base["subject"], base["field"], bounds, base["group_id"]),
                    "kind": "record",
                    "emitted": emitted,
                    "declined": declined,
                    "dropped_options": dropped,
                    "descent": sorted(descent),
                    "screening": {
                        tag: ("passed" if (verdict or {}).get("passed") else "failed")
                        for tag, verdict in (row.get("screening") or {}).items()
                    },
                }
            )
    return units


def write_omission_sample(
    run_dir: Path, out_dir: Path, per_dump: int = 43, seed: int = 20260914
) -> list[dict[str, Any]]:
    """A seeded, per-dump sample of synthesized records for the oracle recall
    probe: the judge lists every label the paragraph evidences and the merge
    diffs that against what grounding emitted."""
    omission_dir = out_dir / "omission"
    omission_dir.mkdir(parents=True, exist_ok=True)
    units = _record_units(run_dir)
    by_dump: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for unit in units:
        by_dump[f"{unit['subject_slug']}__{unit['field']}"].append(unit)
    rng = random.Random(seed)
    index: list[dict[str, Any]] = []
    for key in sorted(by_dump):
        rows = sorted(by_dump[key], key=lambda u: (u["chunk"], u["group_id"]))
        sample = rows if len(rows) <= per_dump else rng.sample(rows, per_dump)
        sample.sort(key=lambda u: (u["chunk"], u["group_id"]))
        name = f"{key}.jsonl"
        with (omission_dir / name).open("w") as fh:
            for unit in sample:
                fh.write(json.dumps(unit, ensure_ascii=False) + "\n")
        index.append({"packet": name, "dump": key, "sampled": len(sample), "population": len(rows)})
    (omission_dir / "INDEX.json").write_text(json.dumps(index, indent=1))
    return index


def render_outlines(out_dir: Path) -> None:
    """The vocabulary outline per concept field, twice: as the initial-grounding
    prompt shows it (name + altLabels), and with each label's definition for
    the judges' reference."""
    outlines_dir = out_dir / "outlines"
    outlines_dir.mkdir(parents=True, exist_ok=True)
    for field, fname in FIELD_TO_ONTOLOGY_FILE.items():
        doc = json.loads((ONTOLOGY_DIR / fname).read_text())
        body = next(v for v in doc.values() if isinstance(v, list))
        plain: list[str] = []
        defined: list[str] = []

        def walk(nodes: list[dict[str, Any]], depth: int) -> None:
            for n in sorted(nodes, key=lambda n: n["name"]):
                text = "  " * depth + n["name"]
                if n.get("altLabels"):
                    text += f" (also: {', '.join(sorted(n['altLabels']))})"
                plain.append(text)
                definition = " ".join((n.get("definition") or "").split())
                defined.append(text + (f"  —  {definition}" if definition else ""))
                walk(n.get("children") or [], depth + 1)

        walk(body, 0)
        header = f"# {field} vocabulary, ontology {doc.get('ontology_version_id')}\n"
        (outlines_dir / f"{field}.txt").write_text(header + "\n".join(plain) + "\n")
        (outlines_dir / f"{field}_with_definitions.txt").write_text(
            header + "\n".join(defined) + "\n"
        )


# ---------------------------------------------------------------------------
# Merge (Phase 2 + Phase 3 verdicts -> the recoverability matrix)
# ---------------------------------------------------------------------------


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # a killed agent's trailing line; counted by the caller
    return rows


def load_verdicts(
    verdicts_dir: Path, corrections_dir: Optional[Path] = None
) -> dict[str, dict[str, Any]]:
    """item_id -> verdict, last one wins (append-only files), then the verifiers'
    full replacement rows from ``corrections_dir`` (``corrections_*.jsonl``)
    overlaid — a correction is a verdict row a verifier re-wrote after reading
    the record, and it replaces the judge's row whole."""
    verdicts: dict[str, dict[str, Any]] = {}
    for path in sorted(verdicts_dir.glob("*.jsonl")):
        for row in _read_jsonl(path):
            item_id = row.get("item_id")
            if isinstance(item_id, str):
                verdicts[item_id] = row
    if corrections_dir is not None and corrections_dir.exists():
        for path in sorted(corrections_dir.glob("corrections_*.jsonl")):
            for row in _read_jsonl(path):
                item_id = row.get("item_id")
                if isinstance(item_id, str) and item_id in verdicts:
                    verdicts[item_id] = {**row, "verified": True}
    return verdicts


# What a verifier re-reads: the rare, consequential codes (where a mis-calibrated
# judge does damage), every "screening was wrong" call, and a random slice of
# clean rows per packet as the control.
VERIFY_ALWAYS_CODES = {"F", "P", "B", "X", "DF", "DR", "H-BRIDGE", "H-FORCED", "H-STOP"}


def write_verify_packets(
    items: list[dict[str, Any]],
    verdicts: dict[str, dict[str, Any]],
    out_dir: Path,
    *,
    packet_size: int = 120,
    random_clean_per_file: int = 5,
    seed: int = 20260914,
) -> list[dict[str, Any]]:
    """Verification packets: each row = the packet item plus the judge's verdict.
    A verifier writes full replacement rows for the ones it changes into
    ``verify/corrections_<k>.jsonl`` and leaves the rest alone."""
    verify_dir = out_dir / "verify"
    verify_dir.mkdir(parents=True, exist_ok=True)
    by_item = {item["item_id"]: item for item in items}
    rng = random.Random(seed)
    chosen: list[dict[str, Any]] = []
    by_file: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item_id, verdict in verdicts.items():
        item = by_item.get(item_id)
        if item is None:
            continue
        code = str(verdict.get("code", ""))
        flagged = (
            code in VERIFY_ALWAYS_CODES
            or verdict.get("screening_sound") == "no"
            or bool(verdict.get("narrower_evidenced"))
        )
        row = {"item": item, "verdict": verdict}
        if flagged:
            chosen.append(row)
        elif code in CLEAN_CODES or code == "DS" or code == "H-OK":
            by_file[f"{item['subject_slug']}__{item['field']}"].append(row)
    for key in sorted(by_file):
        pool = by_file[key]
        chosen.extend(pool if len(pool) <= random_clean_per_file else rng.sample(pool, random_clean_per_file))
    chosen.sort(key=lambda r: (r["item"]["subject_slug"], r["item"]["field"], r["item"]["chunk"], r["item"]["group_id"]))
    index: list[dict[str, Any]] = []
    for k, start in enumerate(range(0, len(chosen), packet_size), start=1):
        part = chosen[start : start + packet_size]
        name = f"packet_{k}.jsonl"
        with (verify_dir / name).open("w") as fh:
            for row in part:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        index.append({"packet": name, "rows": len(part), "codes": dict(Counter(str(r["verdict"].get("code")) for r in part))})
    (verify_dir / "INDEX.json").write_text(json.dumps(index, indent=1))
    return index


def recoverability_matrix(
    items: list[dict[str, Any]], verdicts: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Join judged codes onto the mechanical outcomes.

    Tags: code x recoverable_by x screening verdict x shipped, per field and
    overall; the three bins in the module docstring fall out of it.
    Declines: DS/DF/DR per stage with the lost labels. Hops: H-* codes and
    their recoverable_by. Coverage says how much of the enumeration is judged.
    """
    tag_cells: Counter[tuple[str, str, str, str, str]] = Counter()
    decline_cells: Counter[tuple[str, str, str]] = Counter()
    lost_labels: list[dict[str, Any]] = []
    hop_cells: Counter[tuple[str, str, str]] = Counter()
    narrower: list[dict[str, Any]] = []
    judged = Counter()
    unjudged = Counter()
    bad = Counter()
    for item in items:
        kind = item["kind"]
        verdict = verdicts.get(item["item_id"])
        if verdict is None:
            unjudged[kind] += 1
            continue
        code = str(verdict.get("code", ""))
        field = item["field"]
        if kind == "tag":
            if code not in TAG_CODES:
                bad[kind] += 1
                continue
            judged[kind] += 1
            recoverable = str(verdict.get("recoverable_by") or ("n/a" if code in CLEAN_CODES else "none"))
            tag_cells[(field, code, recoverable, item["screening"]["verdict"], "shipped" if item["shipped"] else "not_shipped")] += 1
            if verdict.get("narrower_evidenced"):
                wanted = str(verdict["narrower_evidenced"]).casefold()
                reached = {str(label).casefold() for label in item.get("descent_reached") or []}
                narrower.append(
                    {
                        "item_id": item["item_id"],
                        "field": field,
                        "tag": item["tag"],
                        "narrower": verdict["narrower_evidenced"],
                        "reached_by_descent": wanted in reached,
                    }
                )
        elif kind == "decline":
            if code not in DECLINE_CODES:
                bad[kind] += 1
                continue
            judged[kind] += 1
            decline_cells[(field, item["stage"], code)] += 1
            if code == "DF" and verdict.get("lost_label"):
                lost_labels.append({"item_id": item["item_id"], "field": field, "stage": item["stage"], "focal_form": item["focal_form"], "lost_label": verdict["lost_label"]})
        elif kind == "hop":
            if code not in HOP_CODES:
                bad[kind] += 1
                continue
            judged[kind] += 1
            hop_cells[(field, code, str(verdict.get("recoverable_by") or "n/a"))] += 1

    def bins(field_filter: Optional[str]) -> dict[str, Any]:
        out = {
            "clean": 0,
            "defective": 0,
            "structurally_unrecoverable": 0,
            "recoverable_caught": 0,
            "recoverable_shipped": 0,
            "recoverable_unscreened": 0,
            "by_code": Counter(),
            "shipped_by_code": Counter(),
        }
        for (field, code, recoverable, screening, shipped), n in tag_cells.items():
            if field_filter and field != field_filter:
                continue
            out["by_code"][code] += n
            if code in CLEAN_CODES:
                out["clean"] += n
                continue
            out["defective"] += n
            if shipped == "shipped":
                out["shipped_by_code"][code] += n
            if recoverable == "none":
                out["structurally_unrecoverable"] += n
            elif screening == "failed":
                out["recoverable_caught"] += n
            elif screening == "passed":
                out["recoverable_shipped"] += n
            else:
                out["recoverable_unscreened"] += n
        out["by_code"] = dict(out["by_code"])
        out["shipped_by_code"] = dict(out["shipped_by_code"])
        return out

    fields = sorted({item["field"] for item in items})
    return {
        "coverage": {"judged": dict(judged), "unjudged": dict(unjudged), "bad_code": dict(bad)},
        "tags": {"ALL": bins(None), **{f: bins(f) for f in fields}},
        "tag_cells": [
            {"field": f, "code": c, "recoverable_by": r, "screening": s, "shipped": sh, "n": n}
            for (f, c, r, s, sh), n in sorted(tag_cells.items())
        ],
        "declines": [
            {"field": f, "stage": s, "code": c, "n": n} for (f, s, c), n in sorted(decline_cells.items())
        ],
        "lost_labels": lost_labels,
        "hops": [
            {"field": f, "code": c, "recoverable_by": r, "n": n} for (f, c, r), n in sorted(hop_cells.items())
        ],
        "narrower_evidenced": narrower,
    }


def omission_matrix(
    omission_dir: Path, verdicts_dir: Path, corrections_dir: Optional[Path] = None
) -> dict[str, Any]:
    """Phase 3: per field, the oracle labels by status (emitted, generalized,
    declined, dropped, missed). A judge's verdict row carries
    `oracle_labels: [{label, status, in_vocabulary, party, note}]` for one
    record; an Opus verifier's full replacement rows under ``corrections_dir``
    (``corrections_*.jsonl``) overlay the oracle's, as in Phase 2."""
    units = {}
    for path in omission_dir.glob("*.jsonl"):
        for unit in _read_jsonl(path):
            units[unit["item_id"]] = unit
    verdicts = load_verdicts(verdicts_dir, corrections_dir)
    cells: Counter[tuple[str, str]] = Counter()
    by_party: Counter[tuple[str, str, str]] = Counter()
    by_vocab: Counter[tuple[str, str, str]] = Counter()
    records_with_a_miss: Counter[str] = Counter()
    records_with_an_own_miss: Counter[str] = Counter()
    records_judged: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for item_id, unit in units.items():
        verdict = verdicts.get(item_id)
        if verdict is None:
            continue
        field = unit["field"]
        records_judged[field] += 1
        missed_here = own_missed_here = False
        for entry in verdict.get("oracle_labels") or []:
            status = str(entry.get("status", "?"))
            party = str(entry.get("party", "unstated"))
            cells[(field, status)] += 1
            by_party[(field, status, party)] += 1
            by_vocab[(field, status, str(entry.get("in_vocabulary", "?")))] += 1
            if status in ("missed", "generalized", "declined", "dropped"):
                missed_here = True
                own_missed_here = own_missed_here or party == "own"
                if status == "missed" and len(examples) < 80:
                    examples.append({"field": field, "focal_form": unit["focal_form"], "label": entry.get("label"), "party": party, "note": entry.get("note")})
        if missed_here:
            records_with_a_miss[field] += 1
        if own_missed_here:
            records_with_an_own_miss[field] += 1
    return {
        "records_judged": dict(records_judged),
        "records_with_a_miss": dict(records_with_a_miss),
        "records_with_an_own_dealing_miss": dict(records_with_an_own_miss),
        "labels_by_status": [
            {"field": f, "status": s, "n": n} for (f, s), n in sorted(cells.items())
        ],
        "labels_by_status_and_party": [
            {"field": f, "status": s, "party": p, "n": n} for (f, s, p), n in sorted(by_party.items())
        ],
        "labels_by_status_and_vocabulary": [
            {"field": f, "status": s, "in_vocabulary": v, "n": n} for (f, s, v), n in sorted(by_vocab.items())
        ],
        "examples": examples,
    }


def packet_coverage(out_dir: Path) -> list[dict[str, Any]]:
    """Per judge packet (Phase 2 under ``packets/``, Phase 3 under
    ``omission/``): items sent, verdict rows present, missing item ids,
    duplicate rows, malformed lines — what a cut-off judge left behind and what
    a RESUME-mode relaunch must append. Nothing is deleted or rewritten."""
    report: list[dict[str, Any]] = []
    for packets_dir, verdicts_dir in ((out_dir / "packets", out_dir / "verdicts"), (out_dir / "omission", out_dir / "omission_verdicts")):
        if not packets_dir.exists():
            continue
        for packet in sorted(packets_dir.glob("*.jsonl")):
            sent = [row["item_id"] for row in _read_jsonl(packet)]
            verdict_path = verdicts_dir / packet.name
            lines = verdict_path.read_text().splitlines() if verdict_path.exists() else []
            rows = _read_jsonl(verdict_path) if verdict_path.exists() else []
            ids = [row.get("item_id") for row in rows]
            counts = Counter(ids)
            missing = [item_id for item_id in sent if item_id not in counts]
            report.append(
                {
                    "packet": packet.name,
                    "phase": "omission" if packets_dir.name == "omission" else "census",
                    "sent": len(sent),
                    "judged": len(counts),
                    "missing": len(missing),
                    "missing_ids": missing,
                    "duplicates": sum(n - 1 for n in counts.values() if n > 1),
                    "unknown_ids": sorted(str(k) for k in set(counts) - set(sent) if k is not None),
                    "malformed_lines": sum(1 for line in lines if line.strip()) - len(rows),
                    "status": "complete" if not missing and verdict_path.exists() else ("not_started" if not verdict_path.exists() else "partial"),
                }
            )
    return report


def write_recoverability_report(out_dir: Path, matrix: dict[str, Any], omission: Optional[dict[str, Any]]) -> None:
    lines = ["# Recoverability matrix (judged codes x mechanical screening outcome)", ""]
    cov = matrix["coverage"]
    lines.append(f"Coverage: judged {cov['judged']}, unjudged {cov['unjudged']}, bad codes {cov['bad_code']}.")
    lines.append("")
    lines.append("| field | clean | defective | structurally unrecoverable | recoverable, caught | recoverable, shipped | recoverable, unscreened | shipped by code |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for field, b in matrix["tags"].items():
        lines.append(
            f"| {field} | {b['clean']} | {b['defective']} | {b['structurally_unrecoverable']} | {b['recoverable_caught']} | {b['recoverable_shipped']} | {b['recoverable_unscreened']} | {b['shipped_by_code']} |"
        )
    lines.append("")
    lines.append("## Declines")
    lines.append("")
    lines.append("| field | stage | code | n |")
    lines.append("|---|---|---|---|")
    for row in matrix["declines"]:
        lines.append(f"| {row['field']} | {row['stage']} | {row['code']} | {row['n']} |")
    lines.append("")
    lines.append(f"Lost labels (DF): {len(matrix['lost_labels'])}; narrower labels evidenced but not reached: {len(matrix['narrower_evidenced'])}.")
    lines.append("")
    lines.append("## Descent hops")
    lines.append("")
    lines.append("| field | code | recoverable_by | n |")
    lines.append("|---|---|---|---|")
    for row in matrix["hops"]:
        lines.append(f"| {row['field']} | {row['code']} | {row['recoverable_by']} | {row['n']} |")
    if omission:
        lines.append("")
        lines.append("## Omission probe (oracle recall)")
        lines.append("")
        lines.append(f"Records judged: {omission['records_judged']}; records with at least one missed label: {omission['records_with_a_miss']}.")
        lines.append("")
        lines.append("| field | status | n |")
        lines.append("|---|---|---|")
        for row in omission["labels_by_status"]:
            lines.append(f"| {row['field']} | {row['status']} | {row['n']} |")
    (out_dir / "RECOVERABILITY.md").write_text("\n".join(lines) + "\n")
    (out_dir / "recoverability.json").write_text(json.dumps({"tags": matrix, "omission": omission}, indent=1))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", required=True)
    parser.add_argument("--out", required=True, help="survey directory to write into")
    parser.add_argument("--dumps-root", default=str(DEFAULT_DUMPS_ROOT))
    parser.add_argument("--packet-size", type=int, default=170)
    parser.add_argument("--omission-per-dump", type=int, default=43)
    parser.add_argument("--seed", type=int, default=20260914)
    parser.add_argument("--merge", action="store_true", help="join verdicts/ (+ verify/ corrections) and omission_verdicts/ onto the enumeration")
    parser.add_argument("--verify-packets", action="store_true", help="build verify/packet_<k>.jsonl from the judged verdicts")
    parser.add_argument("--coverage", action="store_true", help="per packet: sent vs judged vs missing (the RESUME worklist)")
    args = parser.parse_args(argv)

    run_dir = Path(args.dumps_root) / args.run
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    items = enumerate_items(run_dir)

    if args.coverage:
        report = packet_coverage(out_dir)
        for row in report:
            flag = "" if row["status"] == "complete" else f"  <-- {row['status']} ({row['missing']} missing)"
            print(f"{row['phase']:8} {row['packet']:56} sent {row['sent']:4} judged {row['judged']:4} dup {row['duplicates']:2} bad {row['malformed_lines']:2}{flag}")
        (out_dir / "COVERAGE.json").write_text(json.dumps(report, indent=1))
        done = sum(1 for r in report if r["status"] == "complete")
        print(f"{done} of {len(report)} packets complete")
        return

    if args.verify_packets:
        verdicts = load_verdicts(out_dir / "verdicts")
        index = write_verify_packets(items, verdicts, out_dir)
        print(f"verify packets: {index}")
        return

    if args.merge:
        verdicts = load_verdicts(out_dir / "verdicts", out_dir / "verify")
        matrix = recoverability_matrix(items, verdicts)
        omission_dir = out_dir / "omission"
        omission_verdicts = out_dir / "omission_verdicts"
        omission = (
            omission_matrix(omission_dir, omission_verdicts, out_dir / "omission_verify")
            if omission_dir.exists() and omission_verdicts.exists()
            else None
        )
        write_recoverability_report(out_dir, matrix, omission)
        print(f"merged: {matrix['coverage']}")
        return

    (out_dir / "items.jsonl").write_text(
        "".join(json.dumps(it, ensure_ascii=False) + "\n" for it in items)
    )
    write_mechanical_report(out_dir, mechanical_matrix(items), len(items))
    packets = write_packets(items, out_dir, args.packet_size)
    sample = write_omission_sample(run_dir, out_dir, args.omission_per_dump, args.seed)
    render_outlines(out_dir)
    print(f"items {len(items)}: {dict(Counter(it['kind'] for it in items))}")
    print(f"packets {len(packets)} (max {max(p['items'] for p in packets)} items); omission sample {sum(s['sampled'] for s in sample)} records over {len(sample)} dumps")


if __name__ == "__main__":
    sys.exit(main())
