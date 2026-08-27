"""Grounding evaluation instrument — deterministic pass (Phase A) and judgment merge (Phase C).

This is the code half of the instrument described in README.md / RUNBOOK.md.
It judges NOTHING semantic: every check here is structural (statuses, rule-outcome
fields, tag strings, vocabulary membership, counters). Semantic judgment belongs to
the census agents (Phase B) whose JSONL output this script merges.

Usage (from the instrument directory, i.e. tests/test_stages/grounding/):

    .venv/bin/python checks/run_eval.py --run <run_id>
        [--subject S] [--field F] [--dumps-root PATH]
    .venv/bin/python checks/run_eval.py --run <run_id> --merge-judgments

Phase A writes  history/runs/<run_id>/metrics.json  and  DETERMINISTIC.md, and
prints the judge-agent worklist. --merge-judgments folds
history/runs/<run_id>/judgments/*.jsonl into metrics.json, computes outcome
tiers, and appends the ledger (history/ledger.json + history/LEDGER.md).
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
INSTRUMENT = HERE.parent
REPO_ROOT = HERE.parents[5]
DEFAULT_DUMPS_ROOT = REPO_ROOT / "packages" / "logs" / "extraction_dumps"
ONTOLOGY_DIR = (
    REPO_ROOT / "apps" / "data_etl_app" / "src" / "data_etl_app" / "knowledge" / "ontology"
)
RESULTS_DIR = INSTRUMENT / "history" / "runs"
CONFIG_DIR = INSTRUMENT / "config"
EXPECTATIONS_DIR = INSTRUMENT / "expectations"
LEDGER_JSON = INSTRUMENT / "history" / "ledger.json"
LEDGER_MD = INSTRUMENT / "history" / "LEDGER.md"

TAXONOMY_VERSION = 1

CONCEPT_FIELDS = {"industries", "material_caps", "process_caps", "conformity_attestations"}
FREEHAND_FIELDS = {"products", "contract_products", "equipments"}
PHRASE_FIELDS = CONCEPT_FIELDS | FREEHAND_FIELDS

FIELD_TO_ONTOLOGY_FILE = {
    "industries": "industries.json",
    "material_caps": "material_caps.json",
    "process_caps": "process_caps.json",
    "conformity_attestations": "certificates.json",
}

GROUNDING_BLOCKS = ("freehand_grounding", "in_vocab_grounding", "oov_grounding")
GROUNDING_REQUEST_STAGES = (
    "llm_phrase_freehand_grounding",
    "llm_phrase_initial_grounding",
    "llm_phrase_oov_grounding",
    "llm_phrase_recursive_tagging",
)
KNOWN_STATUSES = {"grounded", "no_candidates", "screened_out", "no_mentions", "not_synthesized"}
FAILING_OUTCOMES = {"failed", "violated"}
SENTINEL = "none of the above"
DECORATION_RE = re.compile(r"\s*\(also:.*\)\s*$")

TAG_CODES = ("D", "N", "X", "V", "B", "P", "F")
CLEAN_CODES = {"D", "N"}
DECLINE_CODES = ("DS", "DF", "DR")
HOP_CODES = ("H-OK", "H-BRIDGE", "H-FORCED", "H-STOP")
DEFECT_HOP_CODES = {"H-BRIDGE", "H-FORCED", "H-STOP"}

# Fallback per-row A/A divergence when the run has no computable A/A instrument
# (measured 12.9% / 18.7% on runs 190359 and 194457; take the pessimistic end).
FALLBACK_ROW_DIVERGENCE = 0.19


def _normalize_tag(tag: str) -> str:
    """Grouping key for churn detection. Prefers core's normalizer (the pipeline's
    own notion of 'same spelling'); falls back to a crude fold so the tool still
    runs outside the venv. The normalizer used is recorded in metrics because
    churn counts are only comparable under the same normalizer."""
    try:
        from core.utils.form_normalizer import normalize  # noqa: PLC0415

        return normalize(tag, verb_fold=False)
    except Exception:
        folded = re.sub(r"[^\w\s]", " ", tag.casefold())
        folded = re.sub(r"\s+", " ", folded).strip()
        return folded[:-1] if folded.endswith("s") and len(folded) > 3 else folded


def _normalizer_name() -> str:
    try:
        from core.utils.form_normalizer import NORMALIZER_VERSION  # noqa: PLC0415

        return f"core:{NORMALIZER_VERSION}"
    except Exception:
        return "fallback"


class FieldDump:
    """One dump file: one run x subject x field."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.doc: dict[str, Any] = json.loads(path.read_text())
        self.subject: str = self.doc["subject_unique_id"]
        self.field: str = self.doc["field_type"]
        self.key = f"{self.subject.replace('.', '_')}__{self.field}"

    def iter_rows(self):
        for bounds, chunk in self.doc.get("chunks", {}).items():
            for row in chunk.get("rows") or []:
                yield bounds, row

    def iter_requests(self):
        for bounds, chunk in self.doc.get("chunks", {}).items():
            for stage, reqs in (chunk.get("requests") or {}).items():
                if isinstance(reqs, dict):
                    for sub in reqs.values():
                        for r in sub:
                            yield bounds, stage, r
                else:
                    for r in reqs:
                        yield bounds, stage, r


def discover(run_dir: Path, subject: str | None, field: str | None) -> list[FieldDump]:
    dumps: list[FieldDump] = []
    for p in sorted(run_dir.glob("*.json")):
        fd = FieldDump(p)
        if fd.field not in PHRASE_FIELDS:
            continue
        if subject and fd.subject not in (subject, subject.replace("_", ".")):
            continue
        if field and fd.field != field:
            continue
        dumps.append(fd)
    return dumps


def load_label_set(field: str) -> tuple[str | None, set[str]]:
    """(ontology_version_id, all names + altLabels) for a concept field's vocabulary."""
    fname = FIELD_TO_ONTOLOGY_FILE.get(field)
    if fname is None:
        return None, set()
    doc = json.loads((ONTOLOGY_DIR / fname).read_text())
    body = next(v for v in doc.values() if isinstance(v, list))
    labels: set[str] = set()

    def walk(nodes: list[dict[str, Any]]) -> None:
        for n in nodes:
            labels.add(n["name"])
            labels.update(n.get("altLabels") or [])
            walk(n.get("children") or [])

    walk(body)
    return doc.get("ontology_version_id"), labels


def _grounding_tag_items(row: dict[str, Any]):
    """Yield (stage_short, tag, rules) for every tag the three grounding blocks emit."""
    for block_name in GROUNDING_BLOCKS:
        block = row.get(block_name) or {}
        for tag, rules in (block.get("tags") or {}).items():
            yield block_name.replace("_grounding", ""), tag, rules


def _descent_hops(row: dict[str, Any]):
    """Yield descent nodes minted by recursive grounding (origin recursive_grounding)."""
    for level, nodes in (row.get("lvl_by_lvl_itps") or {}).items():
        for n in nodes:
            if n.get("origin") == "recursive_grounding":
                yield level, n


def _screening_passed(row: dict[str, Any], tag: str) -> bool | None:
    scr = row.get("screening") or {}
    if tag in scr:
        return bool(scr[tag].get("passed"))
    return None


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    vs = sorted(values)
    idx = min(len(vs) - 1, max(0, math.ceil(q * len(vs)) - 1))
    return vs[idx]


def check_field_dump(
    fd: FieldDump, labels: set[str], label_version: str | None
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    findings: list[dict[str, Any]] = []

    def finding(check: str, bounds: str, group_id: str | None, detail: str) -> None:
        findings.append(
            {
                "check": check,
                "subject": fd.subject,
                "field": fd.field,
                "chunk": bounds,
                "group_id": group_id,
                "detail": detail,
            }
        )

    status_counts: Counter[str] = Counter()
    n_tags = n_declines = n_hops = 0
    tags_by_stage: Counter[str] = Counter()
    gate_violations = gate_shipped = 0
    membership_checked = membership_violations = 0
    sentinel_leaks = 0
    dropped_total = dropped_false = dropped_recaptured = 0
    duplicate_tag_rows = 0
    own_name_in_tags = 0
    descent_minted = descent_screened = 0
    surviving_tags: list[str] = []
    rows_by_gid: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)

    own_name = fd.subject.split(".")[0].split("_")[0].casefold()
    own_name_re = re.compile(rf"\b{re.escape(own_name)}\b", re.IGNORECASE)

    dump_meta = fd.doc.get("run", {}).get("extraction_metadata", {})
    dump_ontology_version = dump_meta.get("ontology_version_id")
    ontology_version_mismatch = bool(
        labels and label_version and dump_ontology_version and label_version != dump_ontology_version
    )

    for bounds, row in fd.iter_rows():
        gid = row.get("group_id", "?")
        status = row.get("status")
        status_counts[status or "MISSING"] += 1
        if status not in KNOWN_STATUSES:
            finding("status_accounting", bounds, gid, f"unknown status {status!r}")
        rows_by_gid[gid].append((bounds, row))

        row_tag_keys: list[str] = []
        for stage, tag, rules in _grounding_tag_items(row):
            n_tags += 1
            tags_by_stage[stage] += 1
            row_tag_keys.append(tag)

            outcomes = {r.get("outcome") for r in rules}
            if outcomes & FAILING_OUTCOMES:
                gate_violations += 1
                shipped = status == "grounded" and _screening_passed(row, tag) is not False
                gate_shipped += int(shipped)
                finding(
                    "failed_rule_gate",
                    bounds,
                    gid,
                    f"tag {tag!r} emitted with outcomes {sorted(o for o in outcomes if o)}"
                    f" ({'SHIPPED' if shipped else 'caught downstream'})",
                )

            if tag.strip().casefold() == SENTINEL:
                sentinel_leaks += 1
                finding("sentinel_leak", bounds, gid, f"sentinel stored as tag in {stage}")

            if stage == "in_vocab" and labels:
                membership_checked += 1
                if tag not in labels:
                    membership_violations += 1
                    finding(
                        "vocab_membership",
                        bounds,
                        gid,
                        f"in-vocab tag {tag!r} not an ontology label (name or altLabel)",
                    )

            if own_name_re.search(tag):
                own_name_in_tags += 1

            passed = _screening_passed(row, tag)
            if status == "grounded" and passed is not False:
                surviving_tags.append(tag)

        # duplicate tags within one row (same normalized key from >1 emission)
        norm_keys = Counter(_normalize_tag(t) for t in row_tag_keys)
        dups = [k for k, c in norm_keys.items() if c > 1]
        if dups:
            duplicate_tag_rows += 1
            finding("duplicate_tags", bounds, gid, f"row emits duplicate tag keys {dups}")

        # declines
        for block_name in GROUNDING_BLOCKS:
            block = row.get(block_name) or {}
            if isinstance(block.get("declined"), str):
                n_declines += 1

        # B1 dropped options + decoration echo + OOV recapture
        oov_tags_folded = {
            _normalize_tag(t) for t in ((row.get("oov_grounding") or {}).get("tags") or {})
        }
        for block_name in GROUNDING_BLOCKS:
            for opt in (row.get(block_name) or {}).get("dropped_options") or []:
                dropped_total += 1
                stripped = DECORATION_RE.sub("", opt)
                if labels and opt not in labels and stripped in labels:
                    dropped_false += 1
                    finding(
                        "decoration_echo",
                        bounds,
                        gid,
                        f"dropped option {opt!r} is the decorated form of label {stripped!r}"
                        " (false drop)",
                    )
                elif _normalize_tag(stripped) in oov_tags_folded:
                    dropped_recaptured += 1

        # descent
        for _level, node in _descent_hops(row):
            n_hops += 1
            for tag, rules in (node.get("iterative_og_tag_w_rules") or {}).items():
                descent_minted += 1
                if _screening_passed(row, tag) is not None:
                    descent_screened += 1
                if labels and node.get("in_vocab"):
                    membership_checked += 1
                    if tag not in labels:
                        membership_violations += 1
                        finding(
                            "vocab_membership",
                            bounds,
                            gid,
                            f"descent tag {tag!r} not an ontology label",
                        )
                outcomes = {r.get("outcome") for r in rules}
                if outcomes & FAILING_OUTCOMES:
                    gate_violations += 1
                    finding(
                        "failed_rule_gate",
                        bounds,
                        gid,
                        f"descent tag {tag!r} minted with outcomes"
                        f" {sorted(o for o in outcomes if o)}",
                    )

    # rows with grounding blocks despite no evidence
    for bounds, row in fd.iter_rows():
        if row.get("status") in {"no_mentions", "not_synthesized"} and any(
            (row.get(b) or {}).get("tags") for b in GROUNDING_BLOCKS
        ):
            finding(
                "status_accounting",
                bounds,
                row.get("group_id"),
                f"status {row['status']!r} but grounding tags present",
            )

    # twins: same group_id in >1 chunk
    twin_groups = divergent_twins = 0
    for gid, entries in rows_by_gid.items():
        if len(entries) < 2:
            continue
        twin_groups += 1
        signatures = {
            (
                row.get("status"),
                frozenset(t for _s, t, _r in _grounding_tag_items(row)),
            )
            for _bounds, row in entries
        }
        if len(signatures) > 1:
            divergent_twins += 1

    # churn: surviving tags whose normalized keys collide across distinct spellings
    by_key: dict[str, set[str]] = defaultdict(set)
    for t in surviving_tags:
        by_key[_normalize_tag(t)].add(t)
    churn_clusters = {k: sorted(v) for k, v in by_key.items() if len(v) > 1}

    # ops rollup over grounding requests
    ops: dict[str, dict[str, Any]] = {}
    phantom_requests = real_requests = 0
    for _bounds, stage, req in fd.iter_requests():
        base_stage = stage.removesuffix("_retry")
        if base_stage not in GROUNDING_REQUEST_STAGES:
            continue
        entry = ops.setdefault(
            base_stage,
            {"requests": 0, "input_tokens": 0, "output_tokens": 0, "retry_requests": 0, "_lat": []},
        )
        is_phantom = req.get("note") == "usage_unavailable" or req.get("input_tokens") is None
        if base_stage == "llm_phrase_recursive_tagging":
            phantom_requests += int(is_phantom)
            real_requests += int(not is_phantom)
        if is_phantom:
            continue
        entry["requests"] += 1
        entry["input_tokens"] += req.get("input_tokens") or 0
        entry["output_tokens"] += req.get("output_tokens") or 0
        if stage.endswith("_retry") or ">retry>" in (req.get("custom_id") or ""):
            entry["retry_requests"] += 1
        if req.get("client_latency_ms") is not None:
            entry["_lat"].append(float(req["client_latency_ms"]))
    for entry in ops.values():
        lat = entry.pop("_lat")
        entry["latency_ms_p50"] = _percentile(lat, 0.50)
        entry["latency_ms_p90"] = _percentile(lat, 0.90)

    metrics: dict[str, Any] = {
        "rows": sum(status_counts.values()),
        "status_counts": dict(status_counts),
        "items": {"tag_instances": n_tags, "declines": n_declines, "descent_hops": n_hops},
        "tags_by_stage": dict(tags_by_stage),
        "gate_violations": {"count": gate_violations, "shipped": gate_shipped},
        "membership": {
            "checked": membership_checked,
            "violations": membership_violations,
            "ontology_version_mismatch": ontology_version_mismatch,
        },
        "sentinel_leaks": sentinel_leaks,
        "dropped_options": {
            "total": dropped_total,
            "false_drops": dropped_false,
            "oov_recaptured": dropped_recaptured,
        },
        "duplicate_tag_rows": duplicate_tag_rows,
        "twins": {
            "twin_groups": twin_groups,
            "divergent": divergent_twins,
            "divergence_rate": (divergent_twins / twin_groups) if twin_groups else None,
        },
        "descent": {
            "minted_tags": descent_minted,
            "screened": descent_screened,
            "screened_share": (descent_screened / descent_minted) if descent_minted else None,
            "requests_listed": phantom_requests + real_requests,
            "requests_real": real_requests,
        },
        "tag_churn": {
            "surviving_tags": len(surviving_tags),
            "clusters": len(churn_clusters),
            "examples": dict(sorted(churn_clusters.items())[:8]),
        },
        "own_name_in_tags": own_name_in_tags,
        "ops": ops,
    }
    return metrics, findings


def reproducibility_aa(dumps: list[FieldDump]) -> dict[str, Any]:
    """The free A/A instrument: products vs contract_products share freehand payloads.
    Only meaningful when the two fields' freehand request digests (ud=) match."""
    out: dict[str, Any] = {}
    by_subject: dict[str, dict[str, FieldDump]] = defaultdict(dict)
    for fd in dumps:
        if fd.field in {"products", "contract_products"}:
            by_subject[fd.subject][fd.field] = fd
    for subject, pair in by_subject.items():
        if len(pair) != 2:
            continue
        digests: dict[str, set[str]] = {}
        for field, fd in pair.items():
            digests[field] = {
                req.get("custom_id", "").split("ud=")[-1]
                for _b, stage, req in fd.iter_requests()
                if stage == "llm_phrase_freehand_grounding" and "ud=" in (req.get("custom_id") or "")
            }
        comparable = digests["products"] == digests["contract_products"] and bool(
            digests["products"]
        )
        rows: dict[str, dict[tuple[str, str], frozenset[str]]] = {}
        for field, fd in pair.items():
            rows[field] = {
                (bounds, row["group_id"]): frozenset(
                    (row.get("freehand_grounding") or {}).get("tags") or {}
                )
                for bounds, row in fd.iter_rows()
                if "freehand_grounding" in row
            }
        joint = set(rows["products"]) & set(rows["contract_products"])
        identical = sum(
            1 for k in joint if rows["products"][k] == rows["contract_products"][k]
        )
        flips = sum(
            1
            for k in joint
            if bool(rows["products"][k]) != bool(rows["contract_products"][k])
        )
        n = len(joint)
        out[subject] = {
            "comparable": comparable,
            "groups_compared": n,
            "identical_tag_sets": identical,
            "identical_share": (identical / n) if n else None,
            "row_divergence": (1 - identical / n) if n else None,
            "decline_vs_tag_flips": flips,
        }
    return out


def run_row_divergence(aa: dict[str, Any]) -> float:
    vals = [
        v["row_divergence"]
        for v in aa.values()
        if v.get("comparable") and v.get("row_divergence") is not None
    ]
    return max(vals) if vals else FALLBACK_ROW_DIVERGENCE


def noise_band(clean_n: int, row_divergence: float) -> float:
    """Two-sigma band on a share computed over clean_n items when each underlying
    row re-rolls with probability row_divergence between identical runs. A flipped
    row changes an item's clean/defective bit at most half the time, so the
    per-item flip probability is q = row_divergence / 2 and the band is
    2 * sqrt(2 * q * (1 - q) / n)  (two independent runs, difference of shares)."""
    if clean_n <= 0:
        return 1.0
    q = row_divergence / 2
    return 2 * math.sqrt(2 * q * (1 - q) / clean_n)


def load_eval_config() -> dict[str, Any]:
    cfg_path = CONFIG_DIR / "common.yaml"
    if not cfg_path.exists():
        return {}
    import yaml  # noqa: PLC0415

    return yaml.safe_load(cfg_path.read_text()) or {}


# ---------------------------------------------------------------- Phase A


def phase_a(run_id: str, dumps_root: Path, subject: str | None, field: str | None) -> None:
    run_dir = dumps_root / run_id
    if not run_dir.is_dir():
        sys.exit(f"no dump folder at {run_dir}")
    dumps = discover(run_dir, subject, field)
    if not dumps:
        sys.exit(f"no phrase-field dumps matched in {run_dir}")

    subjects = sorted({fd.subject for fd in dumps})
    missing: dict[str, list[str]] = {}
    if not subject and not field:
        missing = {
            s: sorted(PHRASE_FIELDS - {fd.field for fd in dumps if fd.subject == s})
            for s in subjects
        }
        missing = {s: f for s, f in missing.items() if f}

    labels_cache: dict[str, tuple[str | None, set[str]]] = {}
    per_dump_metrics: dict[str, dict[str, Any]] = {}
    all_findings: list[dict[str, Any]] = []
    for fd in dumps:
        if fd.field in FIELD_TO_ONTOLOGY_FILE and fd.field not in labels_cache:
            labels_cache[fd.field] = load_label_set(fd.field)
        version, labels = labels_cache.get(fd.field, (None, set()))
        m, f = check_field_dump(fd, labels, version)
        per_dump_metrics[fd.key] = m
        all_findings.extend(f)

    aa = reproducibility_aa(dumps)
    row_div = run_row_divergence(aa)

    out_dir = RESULTS_DIR / run_id
    (out_dir / "judgments").mkdir(parents=True, exist_ok=True)
    metrics = {
        "run_id": run_id,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "taxonomy_version": TAXONOMY_VERSION,
        "normalizer": _normalizer_name(),
        "dump_census": {
            "dumps_found": len(dumps),
            "subjects": subjects,
            "missing_fields": missing,
        },
        "row_divergence_estimate": row_div,
        "reproducibility_aa": aa,
        "deterministic": per_dump_metrics,
        "findings": all_findings,
    }
    existing = out_dir / "metrics.json"
    if existing.exists():
        prior = json.loads(existing.read_text())
        if "judged" in prior:
            metrics["judged"] = prior["judged"]
    existing.write_text(json.dumps(metrics, indent=1))
    write_deterministic_report(out_dir, metrics)

    print(f"Phase A complete: {len(dumps)} dumps, {len(all_findings)} findings.")
    if missing:
        print(f"MISSING FIELD DUMPS (a missing dump is not always a crash): {missing}")
    print(f"row-divergence estimate for noise bands: {row_div:.3f}")
    print(f"wrote {existing} and DETERMINISTIC.md")
    print("\nJudge-agent worklist (one agent per line, full census):")
    for fd in dumps:
        m = per_dump_metrics[fd.key]["items"]
        print(
            f"  {fd.key}: {m['tag_instances']} tags, {m['declines']} declines,"
            f" {m['descent_hops']} descent hops"
        )


def write_deterministic_report(out_dir: Path, metrics: dict[str, Any]) -> None:
    lines = [
        f"# Deterministic report — run {metrics['run_id']}",
        "",
        f"Generated {metrics['generated_at']} · taxonomy v{metrics['taxonomy_version']}"
        f" · normalizer {metrics['normalizer']}",
        "",
        f"Dumps: {metrics['dump_census']['dumps_found']}"
        f" · subjects: {', '.join(metrics['dump_census']['subjects'])}",
    ]
    if metrics["dump_census"]["missing_fields"]:
        lines.append(f"**MISSING FIELD DUMPS:** {metrics['dump_census']['missing_fields']}")
    lines += [
        f"Row-divergence estimate (noise floor input): {metrics['row_divergence_estimate']:.3f}",
        "",
        "| dump | tags | declines | hops | gate viol (shipped) | membership viol |"
        " sentinel | false drops | twins div/tot | churn clusters | descent screened |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for key, m in metrics["deterministic"].items():
        tw = m["twins"]
        de = m["descent"]
        lines.append(
            f"| {key} | {m['items']['tag_instances']} | {m['items']['declines']}"
            f" | {m['items']['descent_hops']}"
            f" | {m['gate_violations']['count']} ({m['gate_violations']['shipped']})"
            f" | {m['membership']['violations']} | {m['sentinel_leaks']}"
            f" | {m['dropped_options']['false_drops']}/{m['dropped_options']['total']}"
            f" | {tw['divergent']}/{tw['twin_groups']}"
            f" | {m['tag_churn']['clusters']}"
            f" | {de['screened']}/{de['minted_tags']} |"
        )
    lines += ["", "## A/A reproducibility (products vs contract_products)", ""]
    for subject, v in metrics["reproducibility_aa"].items():
        lines.append(
            f"- {subject}: comparable={v['comparable']},"
            f" identical {v['identical_tag_sets']}/{v['groups_compared']}"
            f" ({(v['identical_share'] or 0) * 100:.1f}%),"
            f" decline-vs-tag flips {v['decline_vs_tag_flips']}"
        )
    lines += ["", f"## Findings ({len(metrics['findings'])})", ""]
    for f in metrics["findings"]:
        lines.append(
            f"- **{f['check']}** {f['subject']}/{f['field']} chunk {f['chunk']}"
            f" group {f['group_id']}: {f['detail']}"
        )
    (out_dir / "DETERMINISTIC.md").write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------- Phase C merge


def merge_judgments(run_id: str) -> None:
    out_dir = RESULTS_DIR / run_id
    metrics_path = out_dir / "metrics.json"
    if not metrics_path.exists():
        sys.exit("run Phase A first (no metrics.json)")
    metrics = json.loads(metrics_path.read_text())

    judged: dict[str, Any] = {}
    for jf in sorted((out_dir / "judgments").glob("*.jsonl")):
        key = jf.stem
        tag_counts: Counter[str] = Counter()
        sub_kinds: Counter[str] = Counter()
        decline_counts: Counter[str] = Counter()
        hop_counts: Counter[str] = Counter()
        fabrications: list[dict[str, Any]] = []
        shipped_defects = 0
        watch_hits: Counter[str] = Counter()
        summaries: list[dict[str, Any]] = []
        judged_items = 0
        bad_lines = 0
        for line in jf.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1
                continue
            if j.get("item") == "summary":
                summaries.append(j)
                continue
            judged_items += 1
            code = j.get("code")
            for w in j.get("watch_items") or []:
                watch_hits[w] += 1
            if j.get("item") == "tag":
                if code in TAG_CODES:
                    tag_counts[code] += 1
                if j.get("sub_kind"):
                    sub_kinds[j["sub_kind"]] += 1
                if code == "F":
                    fabrications.append(
                        {"chunk": j.get("chunk"), "group_id": j.get("group_id"), "tag": j.get("tag")}
                    )
                if (
                    code not in CLEAN_CODES
                    and j.get("screening") == "passed"
                    and j.get("row_status") == "grounded"
                ):
                    shipped_defects += 1
            elif j.get("item") == "decline" and code in DECLINE_CODES:
                decline_counts[code] += 1
            elif j.get("item") == "descent_hop" and code in HOP_CODES:
                hop_counts[code] += 1

        n_tags = sum(tag_counts.values())
        n_declines = sum(decline_counts.values())
        n_hops = sum(hop_counts.values())
        expected = metrics["deterministic"].get(key, {}).get("items", {})
        judged[key] = {
            "source": "judge agents + verification",
            "judged_items": judged_items,
            "bad_lines": bad_lines,
            "completeness": {
                "tags": [n_tags, expected.get("tag_instances")],
                "declines": [n_declines, expected.get("declines")],
                "descent_hops": [n_hops, expected.get("descent_hops")],
            },
            "tag_codes": dict(tag_counts),
            "sub_kinds": dict(sub_kinds),
            "clean_share": (sum(tag_counts[c] for c in CLEAN_CODES) / n_tags) if n_tags else None,
            "fabrication_count": tag_counts.get("F", 0),
            "fabrications": fabrications,
            "shipped_defects": shipped_defects,
            "decline_codes": dict(decline_counts),
            "decline_sound_share": (decline_counts.get("DS", 0) / n_declines)
            if n_declines
            else None,
            "hop_codes": dict(hop_counts),
            "descent_defect_share": (
                sum(hop_counts[c] for c in DEFECT_HOP_CODES) / n_hops
            )
            if n_hops
            else None,
            "watch_item_hits": dict(watch_hits),
            "judge_summaries": summaries,
        }

    # update-merge: keep judged entries from other sources (e.g. an imported
    # baseline census) for dumps that have no JSONL file this pass
    metrics.setdefault("judged", {}).update(judged)
    metrics["tiers"] = compute_tiers(metrics)
    metrics_path.write_text(json.dumps(metrics, indent=1))
    append_ledger(metrics)
    print(f"merged {len(judged)} judgment files; tiers: {metrics['tiers']}")
    for key, jv in judged.items():
        comp = jv["completeness"]
        for kind, (got, want) in comp.items():
            if want is not None and got < want:
                print(f"  INCOMPLETE CENSUS {key}: {kind} {got}/{want}")


def compute_tiers(metrics: dict[str, Any]) -> dict[str, str]:
    cfg = load_eval_config()
    enforced: dict[str, bool] = (cfg.get("deterministic_checks") or {}).get("enforced", {}) or {}
    ledger = json.loads(LEDGER_JSON.read_text()) if LEDGER_JSON.exists() else []
    row_div = metrics.get("row_divergence_estimate", FALLBACK_ROW_DIVERGENCE)

    tiers: dict[str, str] = {}
    for key, m in metrics["deterministic"].items():
        tier = "PASS"
        violations = {
            "failed_rule_gate": m["gate_violations"]["count"],
            "vocab_membership": m["membership"]["violations"],
            "sentinel_leak": m["sentinel_leaks"],
            "decoration_echo": m["dropped_options"]["false_drops"],
        }
        if any(enforced.get(name) and count for name, count in violations.items()):
            tier = "FAIL"
        elif any(violations.values()):
            tier = "WATCH"

        jv = (metrics.get("judged") or {}).get(key)
        if jv and tier != "FAIL":
            prev = [
                r
                for r in ledger
                if r["key"] == key
                and r["run_id"] != metrics["run_id"]
                and r.get("clean_share") is not None
                and r.get("taxonomy_version") == metrics["taxonomy_version"]
            ]
            if jv.get("watch_item_hits") or jv.get("fabrication_count"):
                tier = "WATCH"
            if prev and jv.get("clean_share") is not None:
                last = prev[-1]
                n = sum((jv.get("tag_codes") or {}).values())
                band = noise_band(n, row_div)
                if jv["clean_share"] < last["clean_share"] - band:
                    tier = "WATCH"
        if not any(
            r["key"] == key and r["run_id"] != metrics["run_id"] for r in ledger
        ):
            tier = f"BASELINE/{tier}"
        tiers[key] = tier
    return tiers


def append_ledger(metrics: dict[str, Any]) -> None:
    ledger: list[dict[str, Any]] = (
        json.loads(LEDGER_JSON.read_text()) if LEDGER_JSON.exists() else []
    )
    ledger = [r for r in ledger if r["run_id"] != metrics["run_id"]]
    for key, m in metrics["deterministic"].items():
        jv = (metrics.get("judged") or {}).get(key) or {}
        ledger.append(
            {
                "run_id": metrics["run_id"],
                "key": key,
                "taxonomy_version": metrics["taxonomy_version"],
                "judged_source": jv.get("source"),
                "n_tags": m["items"]["tag_instances"],
                "clean_share": jv.get("clean_share"),
                "fabrication_count": jv.get("fabrication_count"),
                "shipped_defects": jv.get("shipped_defects"),
                "decline_sound_share": jv.get("decline_sound_share"),
                "descent_defect_share": jv.get("descent_defect_share"),
                "gate_violations": m["gate_violations"]["count"],
                "membership_violations": m["membership"]["violations"],
                "false_drops": m["dropped_options"]["false_drops"],
                "twin_divergence_rate": m["twins"]["divergence_rate"],
                "churn_clusters": m["tag_churn"]["clusters"],
                "tier": (metrics.get("tiers") or {}).get(key),
            }
        )
    ledger.sort(key=lambda r: (r["key"], r["run_id"]))
    LEDGER_JSON.write_text(json.dumps(ledger, indent=1))
    write_ledger_md(ledger)


def write_ledger_md(ledger: list[dict[str, Any]]) -> None:
    def fmt(v: Any, pct: bool = False) -> str:
        if v is None:
            return "—"
        if pct:
            return f"{v * 100:.1f}%"
        return str(v)

    lines = [
        "# Grounding evaluation ledger",
        "",
        "One row per (run, subject, field). Judged columns come from the census",
        "(taxonomy version noted); deterministic columns from Phase A. Compare",
        "judged rates across runs only within one taxonomy version and above the",
        "noise band (see README).",
        "",
    ]
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in ledger:
        by_key[r["key"]].append(r)
    for key in sorted(by_key):
        lines += [
            f"## {key}",
            "",
            "| run | tax | tags | clean | fab | shipped def | declines sound |"
            " descent def | gate | member | drops | twin div | churn | tier |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for r in by_key[key]:
            lines.append(
                f"| {r['run_id']} | v{r['taxonomy_version']} | {r['n_tags']}"
                f" | {fmt(r['clean_share'], pct=True)} | {fmt(r['fabrication_count'])}"
                f" | {fmt(r['shipped_defects'])} | {fmt(r['decline_sound_share'], pct=True)}"
                f" | {fmt(r['descent_defect_share'], pct=True)} | {r['gate_violations']}"
                f" | {r['membership_violations']} | {r['false_drops']}"
                f" | {fmt(r['twin_divergence_rate'], pct=True)} | {r['churn_clusters']}"
                f" | {r['tier'] or '—'} |"
            )
        lines.append("")
    LEDGER_MD.write_text("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", dest="run_id", required=True, help="dump run id, e.g. 20260825T194457")
    ap.add_argument("--subject")
    ap.add_argument("--field", choices=sorted(PHRASE_FIELDS))
    ap.add_argument("--dumps-root", type=Path, default=DEFAULT_DUMPS_ROOT)
    ap.add_argument("--merge-judgments", action="store_true")
    args = ap.parse_args()
    if args.merge_judgments:
        merge_judgments(args.run_id)
    else:
        phase_a(args.run_id, args.dumps_root, args.subject, args.field)


if __name__ == "__main__":
    main()
