"""The deterministic half: everything checkable from a dump with no judgment.

Each check is a TRIPWIRE (exact, gates), a BAND (numeric, warns) or a TREND
(reported only). Tripwires are all mechanical facts — a model's wording never
gates, because model wording is the thing with a noise floor.

Every tripwire here exists because the defect it names actually happened. The
`fixed` field on each finding names the run or change that closed it, so a
regression is legible without archaeology.

TEXT COMPARISON GOES THROUGH `_shared.text_matching`. This corpus carries
non-breaking spaces mid-phrase, whitespace runs inside phrases, and short forms
that plain containment credits wrongly ("tight" for TIG). The search harness
produced a false RED on correct output before that was understood. Do not
re-implement matching here.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Any, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from _shared.text_matching import normalize_spaces, occurs_in  # noqa: E402

try:
    from . import loading, paths
except ImportError:  # pragma: no cover - script/package dual import
    import loading  # type: ignore[no-redef]
    import paths  # type: ignore[no-redef]


GATE_TRIPWIRE = "tripwire"
GATE_BAND = "band"
GATE_TREND = "trend"

# A location must not carry a web address, restate the passage, refer to another
# mention, or open with the self-referential lead-in the prompt bans. Each was a
# measured defect: URLs were 39-83% of locations before 2026-08-23; the opener
# was 92.7% before the 2026-08-24 rewrite; cross-references were 2.7%.
_URL_RE = re.compile(r"https?://|www\.", re.IGNORECASE)
_BANNED_OPENER_RE = re.compile(
    r"^\s*(this|the)\s+(passage|sentence|phrase|line|text|snippet)\b", re.IGNORECASE
)
_BANNED_VERB_RE = re.compile(
    r"^\s*\S+\s+(appears|is found|is located|sits)\b", re.IGNORECASE
)
_CROSS_REF_RE = re.compile(
    r"\b(the (previous|preceding|next|other|above|earlier) mention|as (mentioned|noted) above)\b",
    re.IGNORECASE,
)
# The nonce is a bare uuid4 hex the request carries so it misses the prefix
# cache; a model once echoed it as a mention id and described 0 of 47 passages.
_NONCE_RE = re.compile(r"^[0-9a-f]{32}$")


@dataclass
class Finding:
    check: str
    gate: str
    passed: bool
    detail: str
    count: int = 0
    examples: list[str] = dc_field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "check": self.check,
            "gate": self.gate,
            "passed": self.passed,
            "detail": self.detail,
            "count": self.count,
        }
        if self.examples:
            out["examples"] = self.examples[:5]
        return out


@dataclass
class FieldReport:
    key: str
    subject: str
    field: str
    judged: bool
    has_fold: bool
    findings: list[Finding] = dc_field(default_factory=list)
    metrics: dict[str, Any] = dc_field(default_factory=dict)

    @property
    def reds(self) -> list[str]:
        return [f.check for f in self.findings if f.gate == GATE_TRIPWIRE and not f.passed]

    @property
    def warnings(self) -> list[str]:
        return [f.check for f in self.findings if f.gate == GATE_BAND and not f.passed]

    @property
    def status(self) -> str:
        if not self.has_fold:
            return "BLIND"
        if self.reds:
            return "RED"
        if self.warnings:
            return "WARN"
        return "OK"

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "subject": self.subject,
            "field": self.field,
            "judged": self.judged,
            "has_fold": self.has_fold,
            "status": self.status,
            "reds": self.reds,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "findings": [f.as_dict() for f in self.findings],
        }


def _add(report: FieldReport, finding: Finding) -> None:
    report.findings.append(finding)


def _snippet_holds_form(mention: loading.Mention) -> bool:
    """Does the occurrence's own snippet actually contain its form?

    The snippet is cut from the text around the span, so this must hold by
    construction. It fails when a boundary rule is wrong — the `Steel` inside
    `Steelcraft` class. Whitespace-tolerant and short-form-safe via the shared
    matcher; a naive check here would report the corpus's non-breaking spaces
    as defects.
    """
    if not mention.form or not mention.snippet:
        return False
    return occurs_in(mention.form, mention.snippet, case_sensitive=False)


def check_delivery(report: FieldReport, run: loading.FieldRun) -> None:
    """Did the model answer everything it was asked, after its one retry?"""
    not_described = run.summary_total("not_described")
    _add(
        report,
        Finding(
            "delivery.not_described",
            GATE_TRIPWIRE,
            not_described == 0,
            "snippets left undescribed after the retry pass "
            "(fixed 2026-08-23 by the Location retry; a mention is never lost, "
            "only left uncoloured)",
            not_described,
        ),
    )

    unknown = [
        uid for window in run.windows for uid in window.unknown_answer_ids
    ]
    _add(
        report,
        Finding(
            "delivery.unknown_answer_ids",
            GATE_TRIPWIRE,
            not unknown,
            "ids answered that were never sent (a mis-echo)",
            len(unknown),
            unknown,
        ),
    )

    nonce_echoes = [uid for uid in unknown if _NONCE_RE.match(uid)]
    _add(
        report,
        Finding(
            "delivery.nonce_echo",
            GATE_TRIPWIRE,
            not nonce_echoes,
            "the request nonce echoed as a mention id "
            "(fixed 2026-08-22; once described 0 of 47 passages)",
            len(nonce_echoes),
            nonce_echoes,
        ),
    )

    report.metrics["retried"] = run.summary_total("retried")
    report.metrics["windows_retried"] = run.summary_total("windows_retried")
    report.metrics["windows_with_undescribed"] = run.summary_total(
        "windows_with_undescribed"
    )


def check_span_integrity(report: FieldReport, run: loading.FieldRun) -> None:
    """The mechanical facts: spans, snippets and ids must agree with each other."""
    mentions = list(run.mentions())

    bad_form = [m for m in mentions if not _snippet_holds_form(m)]
    _add(
        report,
        Finding(
            "span.form_in_own_snippet",
            GATE_TRIPWIRE,
            not bad_form,
            "every occurrence's form must appear whole-word in its own snippet "
            "(the `Steel`-inside-`Steelcraft` class)",
            len(bad_form),
            [f"{m.group_key}:{m.form!r} not in {m.snippet[:60]!r}" for m in bad_form],
        ),
    )

    by_id: dict[str, set[str]] = {}
    for m in mentions:
        by_id.setdefault(m.mention_id, set()).add(m.snippet)
    collisions = {k: v for k, v in by_id.items() if len(v) > 1}
    _add(
        report,
        Finding(
            "span.mention_id_is_snippet_hash",
            GATE_TRIPWIRE,
            not collisions,
            "one mention id must mean exactly one snippet",
            len(collisions),
            list(collisions)[:5],
        ),
    )

    seen: set[tuple[str, int, int, int, str]] = set()
    duplicates: list[str] = []
    for m in mentions:
        token = (m.chunk_bounds, m.window, m.span[0], m.span[1], m.group_id)
        if token in seen:
            duplicates.append(f"{m.group_key}@{m.chunk_bounds}:{m.span}")
        seen.add(token)
    _add(
        report,
        Finding(
            "span.no_duplicate_occurrences",
            GATE_TRIPWIRE,
            not duplicates,
            "one group must not record the same span twice "
            "(the overlap-duplicate class, 297 cases before overlap went to 0)",
            len(duplicates),
            duplicates,
        ),
    )

    negative = [m for m in mentions if m.span[1] <= m.span[0]]
    _add(
        report,
        Finding(
            "span.well_formed",
            GATE_TRIPWIRE,
            not negative,
            "spans must be non-empty and forward",
            len(negative),
        ),
    )

    report.metrics["mentions"] = len(mentions)
    # Globally distinct snippets. The dump's own `distinct_snippets` counter
    # sums per-window distinct counts, so a snippet occurring in two windows is
    # 2 there and 1 here. Both are reported; neither is wrong.
    report.metrics["distinct_snippets_global"] = len(by_id)
    report.metrics["distinct_snippets_windowed"] = run.summary_total("distinct_snippets")
    nested = run.nested_mentions()
    report.metrics["nested_occurrences"] = len(nested)
    report.metrics["nested_groups"] = len({inner.group_id for inner, _ in nested})
    report.metrics["discovered_casing_mentions"] = sum(
        1 for m in mentions if m.is_discovered_casing
    )


def check_location_content(report: FieldReport, run: loading.FieldRun) -> None:
    """The standing bans on what a location may contain.

    These are content rules the prompt states outright, so they are mechanical
    and gate. Whether a location is CORRECT is a judgment (S2/S3/S4) and is not
    decided here.
    """
    described = [
        m for m in run.distinct_snippets().values() if m.described
    ]
    total = len(described)

    def ban(check: str, pattern: re.Pattern[str], detail: str) -> list[loading.Mention]:
        hits = [m for m in described if pattern.search(m.location)]
        _add(
            report,
            Finding(
                check,
                GATE_TRIPWIRE,
                not hits,
                detail,
                len(hits),
                [f"{m.mention_id}: {m.location[:80]}" for m in hits],
            ),
        )
        return hits

    ban(
        "location.no_url",
        _URL_RE,
        "a location must not carry a web address (was 39-83%; 0 since 2026-08-23)",
    )
    ban(
        "location.no_banned_opener",
        _BANNED_OPENER_RE,
        "a location must not open by naming itself (was 92.7%; 0 since the "
        "2026-08-24 rewrite)",
    )
    ban(
        "location.no_banned_verb_opener",
        _BANNED_VERB_RE,
        "a location must not open with appears/is found/is located/sits",
    )
    ban(
        "location.no_cross_reference",
        _CROSS_REF_RE,
        "a location must stand alone, not lean on another mention (was 2.7%)",
    )

    empty = [m for m in described if not m.location.strip()]
    _add(
        report,
        Finding(
            "location.non_empty",
            GATE_TRIPWIRE,
            not empty,
            "a described snippet must carry location text",
            len(empty),
        ),
    )

    lengths = sorted(len(m.location) for m in described)
    report.metrics["locations_described"] = total
    report.metrics["locations_default"] = sum(
        1 for m in run.distinct_snippets().values() if not m.described
    )
    if lengths:
        report.metrics["location_len_median"] = lengths[len(lengths) // 2]
        report.metrics["location_len_p90"] = lengths[int(len(lengths) * 0.9) - 1]
        report.metrics["location_len_max"] = lengths[-1]
        report.metrics["location_chars"] = sum(lengths)

    # The attribution slot is filled ~100% of the time but 99.5% of it is one
    # boilerplate phrase. Whether that phrase is CORRECT for the passage is the
    # S4 judgment; counting it here only sizes the population.
    boilerplate = sum(
        1 for m in described if "site's own" in m.location.lower()
    )
    report.metrics["attribution_boilerplate"] = boilerplate
    report.metrics["attribution_boilerplate_share"] = (
        round(boilerplate / total, 4) if total else None
    )


def check_groups(report: FieldReport, run: loading.FieldRun) -> None:
    unknown_status = [g for g in run.groups if g.status not in paths.BUNDLE_STATUSES]
    _add(
        report,
        Finding(
            "group.known_status",
            GATE_TRIPWIRE,
            not unknown_status,
            f"every group status must be one of {paths.BUNDLE_STATUSES}",
            len(unknown_status),
            [f"{g.key}:{g.status}" for g in unknown_status],
        ),
    )

    bad_collapsed = [
        g for g in run.groups if g.status == "collapsed" and not g.collapsed_into
    ]
    _add(
        report,
        Finding(
            "group.collapsed_names_its_target",
            GATE_TRIPWIRE,
            not bad_collapsed,
            "a collapsed group must say what it collapsed into (D21, 2026-08-27)",
            len(bad_collapsed),
        ),
    )

    inconsistent = [
        g for g in run.groups if g.mention_count != len(g.mentions)
    ]
    _add(
        report,
        Finding(
            "group.count_matches_mentions",
            GATE_TRIPWIRE,
            not inconsistent,
            "a group's mention_count must equal the mentions it carries",
            len(inconsistent),
            [f"{g.key}:{g.mention_count}!={len(g.mentions)}" for g in inconsistent],
        ),
    )

    empty_with_mentions = [g for g in run.groups if g.is_empty and g.mentions]
    _add(
        report,
        Finding(
            "group.empty_has_no_mentions",
            GATE_TRIPWIRE,
            not empty_with_mentions,
            "a `no_mentions` group must carry no mentions",
            len(empty_with_mentions),
        ),
    )

    report.metrics["groups"] = len(run.groups)
    report.metrics["empty_groups"] = sum(1 for g in run.groups if g.is_empty)
    report.metrics["collapsed_groups"] = sum(
        1 for g in run.groups if g.status == "collapsed"
    )
    report.metrics["multi_form_groups"] = sum(1 for g in run.groups if g.multi_form)
    entry_counts = sorted(
        (g.distinct_snippets for g in run.groups if not g.is_empty), reverse=True
    )
    report.metrics["max_entries_in_a_group"] = entry_counts[0] if entry_counts else 0
    # Where decentralization turns into a downstream defect: synthesis packs at
    # most 50 entries per request, so a group past that is split across the
    # packing boundary that produces cross-record evidence bleed.
    report.metrics["groups_over_50_entries"] = sum(1 for n in entry_counts if n > 50)
    report.metrics["single_mention_groups"] = sum(
        1 for g in run.groups if g.mention_count == 1
    )


def check_page_exclusion(report: FieldReport, run: loading.FieldRun) -> None:
    """Legal pages are dropped before chunking, so no mention may sit on one."""
    excluded = {
        page.get("url")
        for page in (run.scraped_text.get("excluded_pages") or {}).get("pages") or []
        if isinstance(page, dict)
    }
    if not excluded:
        report.metrics["excluded_pages"] = 0
        return
    offenders = [m for m in run.mentions() if m.page in excluded]
    _add(
        report,
        Finding(
            "page.no_mention_on_excluded_page",
            GATE_TRIPWIRE,
            not offenders,
            "legal pages are removed before chunking; nothing may be collected "
            "from one (fixed 2026-08-23, was ~80k wasted tokens)",
            len(offenders),
            [f"{m.group_key}@{m.page}" for m in offenders],
        ),
    )
    report.metrics["excluded_pages"] = len(excluded)


def check_identity(report: FieldReport, run: loading.FieldRun) -> None:
    """The dump must not contradict itself about which prompt ran."""
    declared = (run.metadata.get(paths.STAGE_REQUEST_TOKEN) or {}).get(
        "prompt_version_id"
    )
    seen: set[str] = set()
    for request in run.requests:
        parsed = paths.parse_custom_id(str(request.get("custom_id", "")))
        if parsed and parsed.get("pv"):
            seen.add(parsed["pv"])
    mismatch = declared is not None and seen and any(pv != declared for pv in seen)
    _add(
        report,
        Finding(
            "identity.pv_matches_metadata",
            GATE_TRIPWIRE,
            not mismatch,
            "the prompt version in every request id must match the header",
            len(seen),
            sorted(seen),
        ),
    )
    report.metrics["prompt_version_id"] = declared
    report.metrics["requests"] = len(run.requests)
    usage = run.token_usage.get("by_stage") or {}
    stage_usage = usage.get(paths.STAGE_REQUEST_TOKEN) or {}
    report.metrics["input_tokens"] = stage_usage.get("input_tokens")
    report.metrics["output_tokens"] = stage_usage.get("output_tokens")


def check_shared_identity(reports: dict[str, FieldReport], runs: list[loading.FieldRun]) -> None:
    """`products` and `contract_products` must be the same fold, byte for byte.

    They share one physical request: the contract node mints the products
    custom_id. If they ever diverge, the sharing wiring broke, and every count
    summed over the dump folder is wrong by ~32%.
    """
    by_subject: dict[str, dict[str, loading.FieldRun]] = {}
    for run in runs:
        by_subject.setdefault(run.subject, {})[run.field] = run

    for subject, fields in by_subject.items():
        source = fields.get(paths.SHARED_SOURCE)
        duplicate = fields.get(paths.SHARED_DUPLICATE)
        if not source or not duplicate or not source.has_fold or not duplicate.has_fold:
            continue
        left = [(m.mention_id, m.span, m.group_id) for m in source.mentions()]
        right = [(m.mention_id, m.span, m.group_id) for m in duplicate.mentions()]
        report = reports.get(source.key)
        if report is None:
            continue
        _add(
            report,
            Finding(
                "identity.products_equals_contract_products",
                GATE_TRIPWIRE,
                left == right,
                "contract_products is a byte-copy of products through this "
                "stage; divergence means the shared-identity wiring broke",
                abs(len(left) - len(right)),
                [subject],
            ),
        )


def evaluate_run(runs: Iterable[loading.FieldRun]) -> dict[str, FieldReport]:
    """Every deterministic check over one run, keyed `<slug>__<field>`."""
    runs = list(runs)
    reports: dict[str, FieldReport] = {}
    for run in runs:
        report = FieldReport(
            key=run.key,
            subject=run.subject,
            field=run.field,
            judged=run.judged,
            has_fold=run.has_fold,
        )
        reports[run.key] = report
        if not run.has_fold:
            # Not a failure: a pre-2026-08-27 full-run dump simply cannot show
            # the mention stage. Say so instead of reporting zeros.
            report.metrics["row_groups"] = len(run.row_mention_counts)
            report.metrics["row_mentions"] = sum(run.row_mention_counts.values())
            continue
        check_delivery(report, run)
        check_span_integrity(report, run)
        check_location_content(report, run)
        check_groups(report, run)
        check_page_exclusion(report, run)
        check_identity(report, run)
    check_shared_identity(reports, runs)
    return reports


def nominate_for_judgment(run: loading.FieldRun) -> dict[str, list[str]]:
    """String scans that NOMINATE items for a reader. Never verdicts.

    Regex over this pipeline's model prose has mismeasured in both directions
    repeatedly; these lists exist to steer attention, and a judge is free to
    disagree with every one of them.
    """
    nominations: dict[str, list[str]] = {
        "repeated_snippet": [],
        "third_party_named": [],
        "restatement": [],
        "very_short_location": [],
        "inherited": [],
    }
    counts: dict[str, int] = {}
    for mention in run.mentions():
        counts[mention.mention_id] = counts.get(mention.mention_id, 0) + 1

    for mention_id, mention in run.distinct_snippets().items():
        if counts.get(mention_id, 0) >= 5:
            nominations["repeated_snippet"].append(mention_id)
        location = normalize_spaces(mention.location)
        if len(location) < 40 and mention.described:
            nominations["very_short_location"].append(mention_id)
        snippet_words = {
            w.lower() for w in re.findall(r"\w+", mention.snippet) if len(w) > 4
        }
        location_words = {
            w.lower() for w in re.findall(r"\w+", location) if len(w) > 4
        }
        if snippet_words and len(snippet_words & location_words) / len(snippet_words) > 0.6:
            nominations["restatement"].append(mention_id)

    for inner, outer in run.nested_mentions():
        nominations["inherited"].append(
            f"{inner.group_id}:{inner.mention_id} inside {outer.form!r}"
        )
    return nominations
