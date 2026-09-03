"""The mechanical metric battery for one (subject, field) of one run.

Every metric here is computable without judgment; the judged metrics (J1–J5)
live in the RUNBOOK protocol, not in code. Metrics that need an input this
invocation lacks (wire text without a pull; phrases on a pre-2026-08-26 dump
without a pull) land in ``skipped_missing_input`` instead of degrading
silently.

Matching rules are the locked ones: word boundaries always; forms of three
characters or fewer match case-sensitively only (the `Lead` 52-hits-0-real
lesson, D7)."""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))
from expectations import Expectations  # noqa: E402
from masking import normalize_spaces  # noqa: E402
from _shared.text_matching import (  # noqa: E402
    collapse_whitespace,
    flexible_pattern,
    form_covers,
)
from loading import FieldRun, WindowRecord, is_live  # noqa: E402

from core.utils.floor_scan import form_pattern, is_short_form  # type: ignore[import-untyped]  # noqa: E402


# ------------------------------------------------------------- verbatim tiers

def flexible_form_pattern(form: str, *, case_sensitive: bool) -> Optional[re.Pattern[str]]:
    """``form`` as a word-boundary pattern with elastic internal whitespace.
    Thin alias over the shared primitive so the search harness and every other
    stage instrument cannot drift apart on this."""
    return flexible_pattern(form, case_sensitive=case_sensitive)


def classify_form(form: str, domain: str) -> str:
    """exact | casing_only | substring_only | not_in_window."""
    stripped = normalize_spaces(form).strip()
    # scan_domain already normalizes, but normalizing here too keeps this
    # function correct for any caller (the mapping is 1:1, so offsets hold).
    domain = normalize_spaces(domain)
    if not stripped:
        return "not_in_window"
    exact_pattern = flexible_form_pattern(stripped, case_sensitive=True)
    if exact_pattern is not None and exact_pattern.search(domain):
        return "exact"
    loose_pattern = flexible_form_pattern(stripped, case_sensitive=False)
    if (
        not is_short_form(stripped)
        and loose_pattern is not None
        and loose_pattern.search(domain)
    ):
        return "casing_only"
    if stripped in domain or stripped.casefold() in domain.casefold():
        return "substring_only"
    return "not_in_window"


def verbatim_metrics(windows: list[WindowRecord]) -> Optional[dict[str, Any]]:
    tiers: Counter[str] = Counter()
    offenders: list[dict[str, str]] = []
    judged = 0
    for w in windows:
        if w.phrases is None or w.domain is None:
            continue
        for phrase in w.phrases:
            tier = classify_form(phrase, w.domain)
            tiers[tier] += 1
            judged += 1
            if tier in ("substring_only", "not_in_window") and len(offenders) < 40:
                offenders.append({"form": phrase, "window": w.sub_bounds, "tier": tier})
    if judged == 0:
        return None
    return {
        "forms_judged": judged,
        "exact": tiers["exact"],
        "casing_only": tiers["casing_only"],
        "substring_only": tiers["substring_only"],
        "not_in_window": tiers["not_in_window"],
        "not_in_window_rate": round(tiers["not_in_window"] / judged, 4),
        "offenders": offenders,
    }


# ------------------------------------------------------------- length profile

def length_metrics(windows: list[WindowRecord]) -> Optional[dict[str, Any]]:
    counts: list[int] = []
    crossers = 0
    for w in windows:
        for phrase in w.phrases or []:
            counts.append(len(phrase.split()))
            if "\n" in phrase:
                crossers += 1
    if not counts:
        return None
    long_forms = sum(1 for c in counts if c >= 6)
    return {
        "forms": len(counts),
        "mean_words": round(sum(counts) / len(counts), 2),
        "max_words": max(counts),
        "six_plus_words": long_forms,
        "six_plus_rate": round(long_forms / len(counts), 4),
        "line_break_crossers": crossers,
    }


# ---------------------------------------------------------------- degeneration

def degeneration_metrics(windows: list[WindowRecord]) -> dict[str, Any]:
    loops = []
    near_cap = []
    length_stops = []
    unparseable = []
    for w in windows:
        if w.unparseable_content:
            unparseable.append(w.sub_bounds)
        if w.finish_reason == "length":
            length_stops.append(w.sub_bounds)
        cap = w.completion_cap
        if cap and w.output_tokens and w.output_tokens >= 0.95 * cap:
            near_cap.append(w.sub_bounds)
        phrases = w.phrases or []
        unique = len({p.casefold() for p in phrases})
        # the production warning threshold: >=50 elements, >=2x unique
        if len(phrases) >= 50 and unique and len(phrases) >= 2 * unique:
            most, count = Counter(p.casefold() for p in phrases).most_common(1)[0]
            loops.append({"window": w.sub_bounds, "elements": len(phrases),
                          "unique": unique, "top": most, "top_count": count})
    return {
        "repetition_loops": loops,
        "near_cap_windows": near_cap,
        "finish_reason_length_windows": length_stops,
        "unparseable_windows": unparseable,
    }


# --------------------------------------------------------------- window health

# The smoke alarm (user request 2026-09-02): a window that answers with almost
# nothing despite holding a full page of text. `empty_windows` only sees an
# exactly-empty list, which let howcogroup's skipped ESG page (17.5k chars,
# 1 phrase returned, 10 census misses) pass unremarked. A flag, NEVER a red:
# a thin answer can be correct (lucasmilhaupt's equipments windows are legit
# zero), so this names windows for a reader, it does not gate.
LOW_YIELD_TEXT_FLOOR = 8_000  # chars of wire text
LOW_YIELD_MAX_PHRASES = 2


def window_metrics(windows: list[WindowRecord]) -> dict[str, Any]:
    first = [w for w in windows if w.round_index is None]
    with_phrases = [w for w in first if w.phrases is not None]
    counts = [len(w.phrases or []) for w in with_phrases]
    empty = [w.sub_bounds for w in with_phrases if not w.phrases]
    low_yield = [
        {"window": w.sub_bounds, "phrases": len(w.phrases or []),
         "text_chars": len(w.wire_text)}
        for w in with_phrases
        if w.wire_text is not None
        and len(w.wire_text) >= LOW_YIELD_TEXT_FLOOR
        and len(w.phrases or []) <= LOW_YIELD_MAX_PHRASES
    ]
    return {
        "first_search_windows": len(first),
        "recursive_requests": len(windows) - len(first),
        "windows_with_response": len(with_phrases),
        "empty_windows": empty,
        "low_yield_windows": low_yield,
        "phrases_per_window": {
            "min": min(counts) if counts else None,
            "median": sorted(counts)[len(counts) // 2] if counts else None,
            "max": max(counts) if counts else None,
        },
    }


# ----------------------------------------------------------------- duplicates

def duplicate_metrics(windows: list[WindowRecord]) -> Optional[dict[str, Any]]:
    any_phrases = False
    within_window_dupes = 0
    families: dict[str, set[str]] = {}
    family_chunks: dict[str, set[str]] = {}
    for w in windows:
        if w.phrases is None:
            continue
        any_phrases = True
        counter = Counter(w.phrases)
        within_window_dupes += sum(c - 1 for c in counter.values() if c > 1)
        for p in w.phrases:
            fam = p.casefold()
            families.setdefault(fam, set()).add(p)
            family_chunks.setdefault(fam, set()).add(w.chunk_bounds)
    if not any_phrases:
        return None
    casing_split = {f: sorted(v) for f, v in families.items() if len(v) > 1}
    twins = [f for f, chunks in family_chunks.items() if len(chunks) > 1]
    return {
        "within_window_exact_duplicates": within_window_dupes,
        "casing_split_families": len(casing_split),
        "casing_split_examples": dict(list(casing_split.items())[:15]),
        "cross_chunk_shared_families": len(twins),
        "distinct_families": len(families),
    }


# ------------------------------------------------- window consistency (Q3 port)

def consistency_metrics(windows: list[WindowRecord]) -> Optional[dict[str, Any]]:
    """When the field's own document-wide vocabulary occurs in a window, did
    that window's search list it? Port of the 2026-08-22 Q3 instrument
    (baseline 74% overall). Known caveat carried over: inflated by generic
    families and by wrong-field listings — read beside the judged census."""
    first = [w for w in windows if w.round_index is None
             and w.phrases is not None and w.domain is not None]
    if not first:
        return None
    doc_families: dict[str, set[str]] = {}
    for w in first:
        for p in w.phrases or []:
            if p.strip():
                doc_families.setdefault(p.casefold(), set()).add(p)
    occurrences = listed = in_longer = missed = 0
    missed_examples: list[dict[str, str]] = []
    for w in first:
        local = {p.casefold() for p in (w.phrases or []) if p.strip()}
        domain = w.domain or ""
        listed_positions: set[int] = set()
        for fam in local:
            for form in doc_families.get(fam, ()):
                pattern = form_pattern(form, case_sensitive=is_short_form(form))
                for m in pattern.finditer(domain):
                    listed_positions.update(range(m.start(), m.end()))
        for fam, forms in doc_families.items():
            for form in forms:
                pattern = form_pattern(form, case_sensitive=is_short_form(form))
                for m in pattern.finditer(domain):
                    occurrences += 1
                    if fam in local:
                        listed += 1
                    elif m.start() in listed_positions:
                        in_longer += 1
                    else:
                        missed += 1
                        if len(missed_examples) < 20:
                            snippet = domain[max(0, m.start() - 40): m.end() + 40]
                            missed_examples.append(
                                {"family": fam, "window": w.sub_bounds,
                                 "context": snippet.replace("\n", "⏎")}
                            )
    if occurrences == 0:
        return None
    return {
        "occurrences": occurrences,
        "listed": listed,
        "inside_longer_listed_form": in_longer,
        "missed": missed,
        "consistency": round((listed + in_longer) / occurrences, 4),
        "missed_examples": missed_examples,
    }


# ------------------------------------------------------ sweep lexicon (recall floor)

def sweep_metrics(
    windows: list[WindowRecord], sweeps: list[dict[str, Any]]
) -> Optional[dict[str, Any]]:
    """High-precision pattern families (config/fields/<field>.yaml: sweeps)
    matched against the scan domain; a hit is covered when some returned form
    of the SAME window contains it (casefold, containment either way)."""
    if not sweeps:
        return None
    usable = [w for w in windows if w.round_index is None
              and w.phrases is not None and w.domain is not None]
    if not usable:
        return None
    total_hits = covered = 0
    uncovered: list[dict[str, str]] = []
    for sweep in sweeps:
        pattern = re.compile(sweep["pattern"], re.I if sweep.get("ignore_case", True) else 0)
        for w in usable:
            forms_cf = [_collapse(p).casefold() for p in (w.phrases or [])]
            for m in pattern.finditer(w.domain or ""):
                hit = m.group().strip()
                if not hit:
                    continue
                total_hits += 1
                hit_cf = _collapse(hit).casefold()
                if any(hit_cf in f or f in hit_cf for f in forms_cf):
                    covered += 1
                elif len(uncovered) < 30:
                    uncovered.append(
                        {"sweep": sweep["name"], "hit": hit, "window": w.sub_bounds}
                    )
    if total_hits == 0:
        return {"hits": 0, "covered": 0, "floor_recall": None, "uncovered": []}
    return {
        "hits": total_hits,
        "covered": covered,
        "floor_recall": round(covered / total_hits, 4),
        "uncovered": uncovered,
    }


# ------------------------------------------------------- expectations (recall)

def _collapse(text: str) -> str:
    return collapse_whitespace(text)


def _form_covers(acceptable: str, returned: str) -> bool:
    """Does a returned search form cover an expected acceptable form?
    Shared implementation — whitespace-tolerant, word-boundary-anchored at every
    length, and DIRECTIONAL: the expected designation must occur inside what was
    returned, never the reverse (trap 5, measured at 8.0% false credit on
    2026-08-28). Was `forms_overlap` until then."""
    return form_covers(acceptable, returned)


def expectation_metrics(
    windows: list[WindowRecord], exp: Optional[Expectations]
) -> Optional[dict[str, Any]]:
    if exp is None:
        return None
    usable = [w for w in windows if w.phrases is not None and w.domain is not None]
    if not usable:
        return None
    results: dict[str, list[str]] = {"covered": [], "missed": [], "out_of_coverage": []}
    candidate_results: dict[str, int] = {"covered": 0, "missed": 0, "out_of_coverage": 0}
    missed_detail: list[dict[str, Any]] = []
    for entry in exp.entries:
        status = entry.get("status")
        if status in ("disputed", "retired"):
            continue
        quotes = [e.get("quote", "") for e in entry.get("evidence") or []]
        in_windows = []
        for w in usable:
            haystack = _collapse(w.wire_text or "")
            haystack_cf = haystack.casefold()
            for quote in quotes:
                needle = _collapse(quote)
                if needle and (needle in haystack or needle.casefold() in haystack_cf):
                    in_windows.append(w)
                    break
        if not in_windows:
            bucket = "out_of_coverage"
        else:
            forms = [p for w in in_windows for p in (w.phrases or [])]
            hit = any(
                _form_covers(acc, ret)
                for acc in entry.get("acceptable_forms") or []
                for ret in forms
            )
            bucket = "covered" if hit else "missed"
        if status == "confirmed":
            results[bucket].append(entry["id"])
            if bucket == "missed":
                missed_detail.append(
                    {"id": entry["id"], "name": entry.get("name"),
                     "windows": sorted({w.sub_bounds for w in in_windows})}
                )
        else:
            candidate_results[bucket] += 1
    confirmed_scored = len(results["covered"]) + len(results["missed"])
    false_friend_hits: list[dict[str, str]] = []
    ff_forms = {str(f.get("form", "")).casefold(): str(f.get("form")) for f in exp.false_friends}
    for w in usable:
        for p in w.phrases or []:
            if p.casefold() in ff_forms:
                false_friend_hits.append({"form": p, "window": w.sub_bounds})
    return {
        "eval_set_version": exp.eval_set_version,
        "expected_empty": exp.expected_empty,
        "confirmed_scored": confirmed_scored,
        "confirmed_covered": len(results["covered"]),
        "confirmed_missed": missed_detail,
        "confirmed_recall": (
            round(len(results["covered"]) / confirmed_scored, 4)
            if confirmed_scored else None
        ),
        "confirmed_out_of_coverage": len(results["out_of_coverage"]),
        "candidates": candidate_results,
        "false_friend_hits": false_friend_hits,
        "schema_problems": exp.problems,
    }


# ------------------------------------------------------ merged designations

# A separator followed by another id, right after a designation match.
# Includes U+2044 FRACTION SLASH and U+2215 DIVISION SLASH, which look
# like "/" but are not, so a naive splitter passes them through intact.
_SHORTHAND_TAIL_RE = re.compile(r"^\s*[/,&\u2044\u2215]\s*(?:and\s+)?[A-Z]{0,2}-?\d")


def merged_designation_metrics(
    windows: list[WindowRecord], sweeps: list[dict[str, Any]]
) -> Optional[dict[str, Any]]:
    """Forms that pack TWO OR MORE designations into one string.

    Found by the judged census 2026-08-26 (steelcraft conformity, 10 of 234
    forms): `ASTM E1886/E1996`, `FEMA P-320 & P-361/ICC500-2014 standards`,
    `Compliance with ASTM A666, ASTM E152, UL-10B, and USL-10C`. Search is
    supposed to emit ONE designation per form, so each of these is an entity
    the downstream stages must split or lose — and one of them separates its
    two ids with U+2044 FRACTION SLASH, which any splitter keyed on "/" misses
    silently. Cheap to detect wherever the field has a high-precision sweep,
    so the judged finding becomes a tracked number.
    """
    if not sweeps:
        return None
    usable = [w for w in windows if w.round_index is None and w.phrases is not None]
    if not usable:
        return None
    patterns = [
        re.compile(sweep["pattern"], re.I if sweep.get("ignore_case", True) else 0)
        for sweep in sweeps
    ]
    offenders: list[dict[str, Any]] = []
    forms_seen = 0
    for w in usable:
        for form in w.phrases or []:
            forms_seen += 1
            hits: list[str] = []
            shorthand = False
            for pattern in patterns:
                for match in pattern.finditer(form):
                    hits.append(match.group().strip())
                    # SHORTHAND MERGE: the body is stated once and a second id
                    # is tacked on after a separator — "ASTM E1886/E1996",
                    # "FEMA 361<U+2044>320". Counting bodies alone misses these,
                    # and they are the ones a "/"-keyed splitter mangles.
                    if _SHORTHAND_TAIL_RE.match(form[match.end():]):
                        shorthand = True
            distinct = {h.casefold() for h in hits if h}
            if len(distinct) >= 2 or (shorthand and distinct):
                offenders.append(
                    {
                        "form": form,
                        "window": w.sub_bounds,
                        "designations": sorted(distinct),
                        # U+2044 and other non-ASCII separators break naive splitters
                        "non_ascii_separator": any(
                            ord(ch) > 127 for ch in form if not ch.isalnum() and not ch.isspace()
                        ),
                    }
                )
    if not forms_seen:
        return None
    return {
        "forms_checked": forms_seen,
        "merged_forms": len(offenders),
        "merged_rate": round(len(offenders) / forms_seen, 4),
        "with_non_ascii_separator": sum(1 for o in offenders if o["non_ascii_separator"]),
        "examples": offenders[:15],
    }


# -------------------------------------------------------- cross-field overlap

def field_form_key(form: str) -> str:
    """The key two fields' forms are compared on: whitespace-collapsed and
    casefolded. Deliberately NOT stemmed — this measures whether the SAME
    surface form was returned for two different fields, which is the signature
    of the field-axis defect, not a semantic similarity question."""
    return collapse_whitespace(form).casefold()


def cross_field_overlap(
    subject_runs: Mapping[str, list[WindowRecord]], field_name: str
) -> Optional[dict[str, Any]]:
    """How much of this field's returned vocabulary is ALSO returned for other
    fields of the same subject.

    The measured signature of the field-axis defect: steelcraft equipments
    shares 68 of 75 record groups with products, and alecmfg equipments
    returned five process forms for a window whose text lists named machines.
    A high overlap with `process_caps` or `products` means the field is not
    holding its own boundary — recall loss and precision loss at once.
    """
    own = {
        field_form_key(p)
        for w in subject_runs.get(field_name, [])
        for p in (w.phrases or [])
        if p.strip()
    }
    if not own:
        return None
    overlaps: dict[str, Any] = {}
    every_other: set[str] = set()
    for other_field, windows in sorted(subject_runs.items()):
        if other_field == field_name:
            continue
        other = {
            field_form_key(p)
            for w in windows
            for p in (w.phrases or [])
            if p.strip()
        }
        if not other:
            continue
        shared = own & other
        every_other |= other
        overlaps[other_field] = {
            "shared_forms": len(shared),
            "share_of_own": round(len(shared) / len(own), 4),
            "union_forms": len(own | other),
            "examples": sorted(shared)[:10],
        }
    worst = max(
        overlaps.items(), key=lambda kv: kv[1]["share_of_own"], default=None
    )
    # `shared_with_any` is NOT the sum of the per-pair counts — a form claimed by
    # two siblings is one collision, not two — so it has to be counted here, and
    # it is what the corpus rollup in SUMMARY.md sums.
    claimed = own & every_other
    return {
        "distinct_forms": len(own),
        "shared_with_any": len(claimed),
        "share_claimed_by_any": round(len(claimed) / len(own), 4),
        "by_field": overlaps,
        "worst_field": worst[0] if worst else None,
        "worst_share": worst[1]["share_of_own"] if worst else None,
    }


# ------------------------------------------------------------------ cost/delivery

def cost_metrics(fr: FieldRun, prices: dict[str, Any]) -> dict[str, Any]:
    tokens_in = sum(w.input_tokens or 0 for w in fr.windows)
    tokens_out = sum(w.output_tokens or 0 for w in fr.windows)
    live = sum(1 for w in fr.windows if is_live(w, fr.run_id) is True)
    replayed = sum(1 for w in fr.windows if is_live(w, fr.run_id) is False)
    unknown = len(fr.windows) - live - replayed
    price_in = float(prices.get("input_per_million", 0.0))
    price_out = float(prices.get("output_per_million", 0.0))
    return {
        "requests": len(fr.windows),
        "live": live,
        "replayed": replayed,
        "live_unknown": unknown,
        "input_tokens": tokens_in,
        "output_tokens": tokens_out,
        "usd_estimate": round(
            tokens_in / 1e6 * price_in + tokens_out / 1e6 * price_out, 4
        ),
    }


# --------------------------------------------------------------------- verdict

def verdict(metrics: dict[str, Any], thresholds: dict[str, Any]) -> dict[str, Any]:
    """Recall-only gating (user decision 2026-08-26): red flags are recall
    misses, fabrication, and the fixed-flaw tripwires. Precision-family
    numbers are tracked, never gated."""
    reds: list[str] = []
    exp = metrics.get("expectations")
    if exp and exp.get("confirmed_missed"):
        reds.append(f"confirmed_recall_misses:{len(exp['confirmed_missed'])}")
    verb = metrics.get("verbatim")
    if verb:
        limit = float(thresholds.get("not_in_window_rate_max", 0.02))
        if verb["not_in_window_rate"] > limit:
            reds.append(
                f"not_in_window_rate:{verb['not_in_window_rate']}>{limit}"
            )
    degen = metrics.get("degeneration", {})
    if degen.get("repetition_loops"):
        reds.append(f"repetition_loops:{len(degen['repetition_loops'])}")
    if degen.get("unparseable_windows"):
        reds.append(f"unparseable_windows:{len(degen['unparseable_windows'])}")
    lengths = metrics.get("lengths")
    if lengths and lengths.get("line_break_crossers"):
        reds.append(f"line_break_crossers:{lengths['line_break_crossers']}")
    warnings: list[str] = []
    wh = metrics.get("window_health") or {}
    if wh.get("low_yield_windows"):
        warnings.append(f"low_yield_windows:{len(wh['low_yield_windows'])}")
    out = {"status": "RED" if reds else "OK", "reds": reds}
    if warnings:  # absent key when clean, so stored verdicts stay byte-stable
        out["warnings"] = warnings
    return out
