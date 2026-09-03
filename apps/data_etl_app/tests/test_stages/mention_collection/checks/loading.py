"""Reading a run's mention data out of the extraction dumps.

TWO DUMP SHAPES EXIST and both are supported, because the runs on disk straddle
the change that fixed this:

* A **full-run** dump carried only the group spine (`rows`) until 2026-08-27;
  `keyword_reconcile_node` / `concept_reconcile_node` now also write the `fold`
  block. Every run before that is spine-only — it can say how many mentions a
  group had and NOTHING about the mentions themselves.
* A **partial** dump (`*__partial.json`, written when a run is stopped with
  `StageToggles().stop_after(PipelineStage.mention_collection)`) has always
  carried the `fold` block.

So `load_run` reports, per (subject, field), whether it found real mention rows
or only counts. A field with only counts is not a failure — it is a run this
instrument cannot judge, and it says so rather than reporting zeros.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Any, Iterator, Optional

try:
    from . import paths
except ImportError:  # pragma: no cover - script/package dual import
    import paths  # type: ignore[no-redef]


@dataclass(frozen=True)
class Mention:
    """One occurrence, as the fold recorded it."""

    form: str
    record_id: str
    window: int
    span: tuple[int, int]
    page: Optional[str]
    mention_id: str
    location: str
    location_source: str
    snippet: str
    sent_form: Optional[str]
    doc_span: Optional[tuple[int, int]]
    group_id: str
    group_key: str
    chunk_bounds: str

    @property
    def is_discovered_casing(self) -> bool:
        """The scan found a casing search never sent (`sent_form` differs)."""
        return self.sent_form is not None and self.sent_form != self.form

    @property
    def described(self) -> bool:
        return self.location_source == paths.LOCATION_SOURCE_LLM

    @property
    def window_key(self) -> tuple[str, int]:
        return (self.chunk_bounds, self.window)


@dataclass(frozen=True)
class Group:
    group_id: str
    key: str
    forms: tuple[str, ...]
    status: str
    mention_count: int
    distinct_snippets: int
    collapsed_into: Optional[str]
    own_name_hits_in_snippets: int
    mentions: tuple[Mention, ...]
    chunk_bounds: str

    @property
    def is_empty(self) -> bool:
        return self.status == "no_mentions"

    @property
    def multi_form(self) -> bool:
        return len(self.forms) > 1


@dataclass(frozen=True)
class Window:
    window: int
    sub_bounds: str
    sent_forms: int
    forms_with_hits: int
    zero_hit_forms: tuple[str, ...]
    mentions: int
    distinct_snippets: int
    described: int
    not_described: tuple[str, ...]
    retried: tuple[str, ...]
    unknown_answer_ids: tuple[str, ...]
    discovered_casings: dict[str, list[str]]
    short_forms: tuple[str, ...]
    excluded_pages: tuple[str, ...]
    chunk_bounds: str


@dataclass
class FieldRun:
    """One (subject, field) of one run."""

    run_id: str
    subject: str
    field: str
    path: Path
    partial: bool
    has_fold: bool
    groups: list[Group] = dc_field(default_factory=list)
    windows: list[Window] = dc_field(default_factory=list)
    requests: list[dict[str, Any]] = dc_field(default_factory=list)
    summaries: list[dict[str, Any]] = dc_field(default_factory=list)
    metadata: dict[str, Any] = dc_field(default_factory=dict)
    scraped_text: dict[str, Any] = dc_field(default_factory=dict)
    token_usage: dict[str, Any] = dc_field(default_factory=dict)
    row_mention_counts: dict[str, int] = dc_field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{paths.subject_slug(self.subject)}__{self.field}"

    @property
    def judged(self) -> bool:
        """contract_products shares products' physical fold; never judged."""
        return self.field != paths.SHARED_DUPLICATE

    def mentions(self) -> Iterator[Mention]:
        for group in self.groups:
            yield from group.mentions

    def distinct_snippets(self) -> dict[str, Mention]:
        """mention_id -> one representative mention (they share the snippet).

        NOT the judging unit. A mention_id is content-derived, so one id covers
        every occurrence of that passage anywhere in the document — and the
        model describes it once PER WINDOW, correctly giving a different
        description for each place it recurs. Collapsing to one representative
        therefore hides real, separately-checkable claims: measured 2026-08-29,
        28,260 ids against 36,441 claims, so 22.5% would never be read.
        Use `location_claims()` for anything that judges a location.

        Kept for the mechanical counts, where "how many distinct passages are
        there" is a legitimately different question from "how many claims did
        the model make".
        """
        out: dict[str, Mention] = {}
        for mention in self.mentions():
            out.setdefault(mention.mention_id, mention)
        return out

    def location_claims(self) -> list[tuple[Mention, list[Mention]]]:
        """One entry per location the model actually produced.

        A claim is `(mention_id, chunk_bounds, window)`: the model is asked
        about each mention once per window, and it answers once — verified on
        run 20260829T022413, where 0 of 36,441 window-mention pairs carried
        more than one distinct location. So this is exactly the set of
        assertions the S-codes grade.

        Each entry carries EVERY occurrence its claim covers, because a correct
        description may legitimately span several of them:

            "Sentence of prose under the main content area on both the Boeing
             and Airbus fixed wing replacement parts pages, in the site's own
             copy."

        That is one window, one description, four occurrences, two pages — and
        the prompt explicitly asks for it ("A passage may occur at more than one
        place ... Describe it once, covering where it recurs"). A judge handed a
        single representative occurrence would read that sentence against one
        page and call it wrong, manufacturing exactly the false failure the
        verification brief exists to prevent.

        Ordered by first appearance so work orders are stable across re-runs.
        """
        claims: dict[tuple[str, str, int], list[Mention]] = {}
        for mention in self.mentions():
            key = (mention.mention_id, mention.chunk_bounds, mention.window)
            claims.setdefault(key, []).append(mention)
        return [(covered[0], covered) for covered in claims.values()]

    def nested_mentions(self) -> list[tuple[Mention, Mention]]:
        """Every (inner, outer) pair where one occurrence sits strictly inside
        another collected occurrence of the same window.

        This is what the D8 reversal (2026-08-27) made possible. Before it,
        longest-span containment dropped the inner hit, so this list was empty
        by construction; now `door` keeps its own occurrence inside
        "doors and frames" and gets its own record.

        Note the span recorded on a mention is the span of ITS OWN form's match,
        not of the phrase enclosing it, so nesting can only be found by
        comparing spans across groups within a window — never from one mention
        alone.
        """
        by_window: dict[tuple[str, int], list[Mention]] = {}
        for mention in self.mentions():
            by_window.setdefault(mention.window_key, []).append(mention)

        pairs: list[tuple[Mention, Mention]] = []
        for mentions in by_window.values():
            ordered = sorted(mentions, key=lambda m: (m.span[0], -(m.span[1] - m.span[0])))
            for i, inner in enumerate(ordered):
                for outer in ordered[:i]:
                    if outer.span[0] > inner.span[0]:
                        break
                    strictly_inside = (
                        outer.span[0] <= inner.span[0]
                        and inner.span[1] <= outer.span[1]
                        and (outer.span[1] - outer.span[0]) > (inner.span[1] - inner.span[0])
                    )
                    if strictly_inside and outer.group_id != inner.group_id:
                        pairs.append((inner, outer))
                        break
        return pairs

    def summary_total(self, key: str) -> int:
        total = 0
        for summary in self.summaries:
            value = summary.get(key)
            if isinstance(value, int):
                total += value
            elif isinstance(value, list):
                total += len(value)
        return total


def _tuple_of_str(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(str(v) for v in value)


def _mention(raw: dict[str, Any], group_id: str, key: str, bounds: str) -> Mention:
    span = raw.get("span") or [0, 0]
    doc_span = raw.get("doc_span")
    return Mention(
        form=str(raw.get("form", "")),
        record_id=str(raw.get("record_id", "")),
        window=int(raw.get("window", 0)),
        span=(int(span[0]), int(span[1])),
        page=raw.get("page"),
        mention_id=str(raw.get("mention_id", "")),
        location=str(raw.get("location", "")),
        location_source=str(raw.get("location_source", "")),
        snippet=str(raw.get("snippet", "")),
        sent_form=raw.get("sent_form"),
        doc_span=(int(doc_span[0]), int(doc_span[1])) if doc_span else None,
        group_id=group_id,
        group_key=key,
        chunk_bounds=bounds,
    )


def load_field_run(path: Path, run_id: str) -> FieldRun:
    payload = json.loads(path.read_text(encoding="utf-8"))
    run = payload.get("run") or {}
    field_run = FieldRun(
        run_id=run_id,
        subject=str(payload.get("subject_unique_id", "")),
        field=str(payload.get("field_type", "")),
        path=path,
        partial=bool(run.get("partial")),
        has_fold=False,
        metadata=run.get("extraction_metadata") or {},
        scraped_text=run.get("scraped_text") or {},
        token_usage=run.get("token_usage") or {},
    )

    for bounds, chunk in (payload.get("chunks") or {}).items():
        for row in chunk.get("rows") or []:
            gid = row.get("group_id")
            if isinstance(gid, str) and isinstance(row.get("mention_count"), int):
                field_run.row_mention_counts[gid] = row["mention_count"]

        for stage_key in (paths.STAGE_REQUEST_TOKEN, paths.RETRY_REQUEST_TOKEN):
            entries = (chunk.get("requests") or {}).get(stage_key)
            if isinstance(entries, dict):
                for sub, items in entries.items():
                    for item in items or []:
                        field_run.requests.append({**item, "sub_bounds": sub})
            elif isinstance(entries, list):
                for item in entries:
                    field_run.requests.append(dict(item))

        fold = chunk.get("fold")
        if not isinstance(fold, dict):
            continue
        field_run.has_fold = True
        summary = fold.get("summary")
        if isinstance(summary, dict):
            field_run.summaries.append(summary)

        for raw_group in fold.get("groups") or []:
            gid = str(raw_group.get("group_id", ""))
            key = str(raw_group.get("key", ""))
            mentions = tuple(
                _mention(m, gid, key, bounds) for m in (raw_group.get("mentions") or [])
            )
            field_run.groups.append(
                Group(
                    group_id=gid,
                    key=key,
                    forms=_tuple_of_str(raw_group.get("forms")),
                    status=str(raw_group.get("status", "")),
                    mention_count=int(raw_group.get("mention_count", 0)),
                    distinct_snippets=int(raw_group.get("distinct_snippets", 0)),
                    collapsed_into=raw_group.get("collapsed_into"),
                    own_name_hits_in_snippets=int(
                        raw_group.get("own_name_hits_in_snippets", 0)
                    ),
                    mentions=mentions,
                    chunk_bounds=bounds,
                )
            )

        for raw_window in fold.get("windows") or []:
            field_run.windows.append(
                Window(
                    window=int(raw_window.get("window", 0)),
                    sub_bounds=str(raw_window.get("sub_bounds", "")),
                    sent_forms=int(raw_window.get("sent_forms", 0)),
                    forms_with_hits=int(raw_window.get("forms_with_hits", 0)),
                    zero_hit_forms=_tuple_of_str(raw_window.get("zero_hit_forms")),
                    mentions=int(raw_window.get("mentions", 0)),
                    distinct_snippets=int(raw_window.get("distinct_snippets", 0)),
                    described=int(raw_window.get("described", 0)),
                    not_described=_tuple_of_str(raw_window.get("not_described")),
                    retried=_tuple_of_str(raw_window.get("retried")),
                    unknown_answer_ids=_tuple_of_str(
                        raw_window.get("unknown_answer_ids")
                    ),
                    discovered_casings=raw_window.get("discovered_casings") or {},
                    short_forms=_tuple_of_str(raw_window.get("short_forms")),
                    excluded_pages=_tuple_of_str(raw_window.get("excluded_pages")),
                    chunk_bounds=bounds,
                )
            )
    return field_run


def load_run(run_id: str, dumps_root: Optional[Path] = None) -> list[FieldRun]:
    """Every phrase field of one run, newest dump per (subject, field).

    A run folder can hold both `x.json` and `x__partial.json` for one field —
    the partial from a stopped run and the full from a later resume. The one
    carrying a fold wins; if both do, the full-run dump wins.
    """
    root = (dumps_root or paths.DUMP_ROOT) / run_id
    if not root.is_dir():
        raise FileNotFoundError(f"no dump directory for run {run_id}: {root}")

    best: dict[str, FieldRun] = {}
    for path in sorted(root.glob("*.json")):
        field_run = load_field_run(path, run_id)
        if field_run.field not in paths.PHRASE_FIELDS:
            continue
        current = best.get(field_run.key)
        if current is None:
            best[field_run.key] = field_run
            continue
        # Prefer a dump with mention data; between two that have it, prefer the
        # full run (a partial's numbers stop at the stage it was cut off at).
        if field_run.has_fold and not current.has_fold:
            best[field_run.key] = field_run
        elif field_run.has_fold == current.has_fold and not field_run.partial:
            best[field_run.key] = field_run
    return [best[k] for k in sorted(best)]


def latest_run(dumps_root: Optional[Path] = None) -> Optional[str]:
    root = dumps_root or paths.DUMP_ROOT
    if not root.is_dir():
        return None
    runs = [p.name for p in root.iterdir() if p.is_dir()]
    return max(runs) if runs else None


def load_text(subject: str) -> Optional[str]:
    """The pinned snapshot a golden label is anchored to, if present.

    This is the evaluation copy under `test_stages/sample_scraped_texts/`. It is
    NOT byte-identical to what the pipeline read: the pipeline normalizes the S3
    object and drops excluded pages first. Anchor by quote, never by offset.
    """
    path = paths.text_path(subject)
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8", errors="replace")
