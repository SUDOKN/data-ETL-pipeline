"""Rendered-DOM HTML -> Markdown with sanitized-HTML-island tables: the
``markdown_v1`` scrape rendering (decided 2026-08-28, user; direct cutover from
the legacy innerText rendering, no dual-emit).

WHY (measured on all 20 golden-corpus manufacturers, 144 live pages —
``pipeline_v3_evidence/2026-08-28_scraper_output_format_sample/``, re-runnable
scripts + per-site tables inside): the legacy ``document.body.innerText``
rendering flattens lists and headings (bullets, nesting and the document
outline are all erased), leaves tables as invisible tab-separated rows, and
silently OMITS unrendered content — 53 ``<details>`` accordions on 2 of the 20
sites. Raw HTML was ruled out at 43.2x the tokens of plain text (range
4-178x); this Markdown rendering measured 1.07x pre-dedup and **1.032x after
the scraper's dedup pass**, i.e. the structure is nearly free. The corpus is
list-heavy, not table-heavy: 12 real ``<table>`` elements and ZERO merged-cell
tables in those 144 pages, so the HTML island below is deterministic insurance,
not a hot path.

THE RENDERING CONTRACT (``FORMAT_MARKDOWN``) — one flag, one frozen rendering:
this module is a PURE function of the HTML string. Same input, same output
bytes, forever. Determinism is load-bearing, not cosmetic: the scraper's dedup
strips repeated navigation by EXACT line match, phrase identity keys on
verbatim text (see the sent-phrases contract), and eval baselines are only
comparable against a known rendering. A rule change here is therefore never a
new toggle or a silent edit — it is a NEW format version string (bump
``FORMAT_MARKDOWN`` to ``markdown_v2``), mirroring the
``page_exclusion_version`` pattern in ``core.models.chunking_strat``.

The rules (each measured or decided 2026-08-28 unless noted):

- Envelope: NOT this module's concern. The scraper's page-block envelope
  (50-hash separator / URL line / blank / body) is unchanged; this module only
  renders bodies. Markdown headings are at most 6 hashes plus a space plus
  text, so ``floor_scan``'s separator regex (``#{10,}`` alone on a line) can
  never match an emitted heading.
- Headings ``<h1>``-``<h6>`` -> ``#``-``######`` (restores the outline).
- Lists -> ``-`` / real ``1.`` numbering, 2-space indent per nesting level.
  An item whose content renders EMPTY emits nothing (user-spotted 2026-08-28:
  a Bootstrap carousel's ``<ol class="carousel-indicators">`` pagination dots
  are empty ``<li>`` elements — innerText never showed them, and rendering
  bare markers would have minted new noise). Suppression is structural, so
  ordered numbering stays gapless.
- Simple tables -> GitHub-flavored pipe tables; first row is the header row
  (GFM requires one). Pipes inside cell text are escaped ``\\|``.
- THE ISLAND (the only HTML that survives): a table whose structure Markdown
  cannot express — a rowspan/colspan span, a nested table, a list inside a
  cell, or a multi-paragraph cell — is emitted as sanitized HTML: structural
  tags only, every attribute stripped except rowspan/colspan, ONE ``<tr>`` per
  line so dedup's line matching and the fold's line-oriented snippet clip both
  see rows as units. GFM explicitly permits embedded HTML blocks.
- Links keep their text, hrefs are DROPPED. Two reasons: token cost, and
  ``floor_scan``'s URL-line regex treats a bare URL alone on a line as a page
  BARRIER — bodies must not mint new barrier lines.
- ``<img alt="...">`` -> the alt text inline (certification badges shipped as
  images become text; direct conformity_attestations recall).
- Hidden-but-in-DOM content (closed ``<details>``, inactive tab panels) is
  INCLUDED — no CSS awareness, by policy. Cuts both ways (SEO-hidden junk
  enters too); the accordions won the trade on measurement.
- Dropped subtrees: scripts, styles, SVG, media, frames, and form CONTROLS
  (input/select/option/button/textarea) — interaction and graphics, not text
  data. Form containers and labels are kept (label text can be content).
- ``<blockquote>`` and ``<pre>`` render as plain blocks — no ``>`` or fence
  markers. Deliberate: every marker family this module emits becomes part of
  the snippet universe the fold clips and the models quote; bullets, heading
  hashes and table pipes carry their weight as location signals, quote/fence
  markers on this corpus would not.
- Entities are decoded by the parser (``&amp;`` -> ``&``): downstream phrase
  identity lives on the visible-text plane, never on markup escapes (see the
  unicode-escape-echo incident for what markup escapes do to verbatim echo).

THE V2 RULES (2026-08-29, user decisions; measured on the freshly re-scraped
20-subject corpus — alecmfg's per-page residue was the driver: dedup's common-
prefix vote was broken by varying logo-alt runs, letting the full nav ×2 plus
the Complianz consent panel through on all 80 blocks, 171 ``- Home`` lines and
221 consent lines on non-policy pages):

- NAVIGATION COMPACTS, NEVER DROPS (user decision — second preference chosen
  over dropping, to keep the tail recall of nav-only mentions and stay
  self-labeling): every ``<nav>`` / ``role="navigation"`` subtree renders as
  ONE line — ``Navigation: Home · About · Contact`` — built from its link
  texts (falling back to its flat text when it has no links). The label makes
  a snippet clipped from it announce itself to the Location stage; the middle
  dot survives ``normalize_scraped_text`` and is not a floor-scan word
  character, so forms inside items still match. Identical nav lines within
  one page (desktop + mobile menu duplicates) emit once.
- DOM-DECLARED HIDDEN DROPS: ``[hidden]`` and ``aria-hidden="true"`` subtrees
  are removed. This NARROWS v1's hidden-content inclusion: content hidden
  only by CSS is still included (no CSS awareness), and ``<details>`` content
  is untouched — but a JS accordion that marks collapsed panels
  ``aria-hidden`` now loses them. Accepted trade: it is what kills dismissed
  consent banners, skip-links and inert modals at the source.
- MODAL UI DROPS: ``role="dialog"`` / ``aria-modal="true"`` subtrees are
  interruption chrome (consent dialogs, newsletter popups, lightboxes), never
  page content.
- CONSENT-VENDOR CONTAINERS DROP: an element whose id or any class starts
  with one of ``_CONSENT_VENDOR_PREFIXES`` (the major consent-management
  platforms; alecmfg's Complianz = ``cmplz-``). The list is FROZEN as part of
  this format version — editing it is a version bump like any other rule
  change. Deliberately absent: any bare-``cookie`` match on attributes or
  content — this corpus contains food-machinery manufacturers (cookie
  depositors are real products), and the compound vendor tokens above are the
  only sanctioned use of the word.
- CONSECUTIVE DUPLICATE IMAGE ALTS COLLAPSE: the same alt text emitted again
  with nothing but whitespace/markup since its last emission is skipped
  (multi-variant logos produced ``Alec Model`` ×1–3 as a page's first line,
  which is exactly what broke the dedup prefix vote). Real text between two
  identical alts resets the rule.
- ``<header>`` / ``<footer>`` content stays: footers hold addresses and
  certification text the single-stage fields extract. Repetition is dedup's
  job, and with the alt rule above its prefix vote works again.

Interactions verified before cutover (same evidence folder): the dedup pass
strips repeated Markdown navigation exactly as it does legacy text (26.9% vs
29.3% of corpus tokens removed); ``normalize_scraped_text`` folds punctuation
but never collapses leading indentation, so nesting survives ingestion; the
fold's snippet clip is already line-oriented, and its unit scan skips pipe
separator rows (``core.utils.aggregation_fold``).

THE V3 RULE (2026-09-02, Phase B of the search-recall roadmap —
``docs_local/SEARCH_RECALL_ROADMAP_2026-09-02.md``; measured BEFORE freezing:
a text-level prototype of exactly this transform took a dealer-catalog
window's model-code recall from 2/335 to 332/335 on gpt-4.1 with UNCHANGED
search prompts, and the list-item extension measured NO gain on three
list-heavy windows, so it is deliberately absent — evidence in
``apps/data_etl_app/tests/test_stages/search/history/runs/20260901T013332/table_ab/``):

- BARE-LABEL RUNS INHERIT THEIR HEADING: after cleanup, a run of >=5
  label-shaped lines (non-empty, <=48 chars, no terminal ``.!?:``, not a
  heading/table/list/blockquote/island/``Navigation:`` line; blank lines may
  sit inside the run) is rewritten so each label carries the nearest
  preceding heading of any level: ``58BD (1)`` under ``# WELLSAW`` becomes
  ``WELLSAW: 58BD (1)`` — the heading's text, a colon, a space, the exact
  shape the A/B measured. A heading longer than 60 chars propagates nothing
  (sentence-shaped headings would bloat every line); a label that already
  starts with the heading's text is left alone. WHY: catalog tiles and
  inventory lines print bare designations whose kind and brand live only in
  the heading — the single largest never-found class of the 2026-09-01
  census (533 of 1,162 entities have NO contiguous surface form without
  this) — and the prefix manufactures the span that search, the mechanical
  mention scan, and synthesis can then carry. List items keep their v2
  rendering: the same propagation measured null on list runs. The nearest-
  any-level heading (rather than the prototype's h1-only) is the one
  generalization beyond the measured arm: pages without an h1 (measured:
  tanfel's carousels) otherwise get nothing, and within one rendered body
  the heading stack IS the document outline.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup
from bs4.element import Comment, Doctype, NavigableString, Tag

# The format version stamped into the S3 object tags of every upload
# (``text_format`` tag; see ``ScrapedTextFile``). Objects without the tag
# predate 2026-08-28 and are legacy text. v1 (2026-08-28) was never published
# to S3; v2 (2026-08-29) added the nav/hidden/modal/consent/alt rules above;
# v3 (2026-09-02) added heading propagation onto bare-label runs (THE V3
# RULE).
FORMAT_MARKDOWN = "markdown_v3"
FORMAT_LEGACY_TEXT = "text_v1"

# Consent-management-platform container prefixes (id or class, lowercased
# prefix match). FROZEN with this format version — see THE V2 RULES.
_CONSENT_VENDOR_PREFIXES = (
    "cmplz-",            # Complianz (alecmfg's)
    "onetrust-", "ot-sdk",
    "cybotcookiebot",    # Cookiebot
    "cookie-law-info", "cli-modal", "cli-bar", "cli-settings",
    "cky-",              # CookieYes
    "didomi-",
    "qc-cmp2",           # Quantcast
    "truste-",
    "osano-cm",
    "iubenda-cs",
    "borlabscookie",
    "moove_gdpr",
    "cookiescript_",
    "termly-",
    "cc-window", "cc-banner",  # cookieconsent.js
)

# Subtrees that are interaction or graphics, never text data. ``<form>`` and
# ``<label>`` are deliberately NOT here — sites put real content inside form
# containers; only the controls themselves are dropped.
_DROP_TAGS = frozenset({
    "script", "style", "svg", "noscript", "template", "iframe", "canvas",
    "video", "audio", "source", "picture", "object", "embed", "map",
    "input", "select", "option", "optgroup", "datalist", "button", "textarea",
    "head", "title", "meta", "link", "base", "dialog",
})

# Tags whose boundaries force a line break when unwrapped (everything not
# handled explicitly below is unwrapped: divs, spans, semantic wrappers, ...).
_BLOCK_TAGS = frozenset({
    "div", "section", "article", "nav", "header", "footer", "main", "aside",
    "form", "fieldset", "address", "ul", "ol", "table", "tr",
})

_HEADING_TAGS = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})

# Structural tags an island keeps; every other tag inside an island is
# unwrapped to its text.
_ISLAND_TAGS = frozenset({"table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption"})
_ISLAND_ATTRS = frozenset({"rowspan", "colspan"})

# Block-level tags inside a table cell that a single pipe-cell line cannot
# hold: their presence sends the whole table to the island path.
_CELL_BLOCK_TAGS = frozenset({"table", "ul", "ol", "dl", "blockquote", "pre"} | _HEADING_TAGS)


def html_to_markdown(html: str) -> str:
    """*html* (a rendered DOM's ``outerHTML``) as ``markdown_v1`` text.

    Pure and deterministic — see the module docstring for the contract and
    every rendering rule. Returns ``""`` for markup with no text content; the
    caller (``ScraperService``) treats that as extraction failure and falls
    back to the legacy innerText path.
    """
    soup = BeautifulSoup(html, "html.parser")
    for element in soup.find_all(string=lambda s: isinstance(s, (Comment, Doctype))):
        element.extract()
    for tag in soup.find_all(list(_DROP_TAGS)):
        tag.decompose()
    # V2 drop passes (see THE V2 RULES): DOM-declared hidden, modal UI, and
    # consent-vendor containers — all pure attribute predicates.
    for tag in soup.find_all(_is_dropped_by_v2_rules):
        tag.decompose()
    text = _render_children(soup, depth=0, ctx={"last_alt": None})
    return _propagate_headings(_cleanup(text))


def _is_dropped_by_v2_rules(tag: Tag) -> bool:
    if tag.has_attr("hidden"):
        return True
    if str(tag.get("aria-hidden", "")).strip().lower() == "true":
        return True
    if str(tag.get("role", "")).strip().lower() == "dialog":
        return True
    if str(tag.get("aria-modal", "")).strip().lower() == "true":
        return True
    tokens = [str(tag.get("id", ""))] + [str(c) for c in (tag.get("class") or [])]
    return any(
        t.lower().startswith(prefix)
        for t in tokens
        if t
        for prefix in _CONSENT_VENDOR_PREFIXES
    )


# --- the walk -------------------------------------------------------------------


def _render_children(
    node: Tag, depth: int, ol_counter: list[int] | None = None, ctx: dict | None = None
) -> str:
    ctx = ctx if ctx is not None else {"last_alt": None}
    parts: list[str] = []
    for child in node.children:
        if isinstance(child, NavigableString):
            raw = str(child)
            if not raw.strip():
                # A whitespace-only node still separates its inline siblings
                # ("<b>A</b> <i>B</i>" must not fuse to "AB").
                parts.append(" ")
            else:
                parts.append(re.sub(r"\s+", " ", raw))
                ctx["last_alt"] = None  # real text resets the alt-collapse rule
            continue
        if not isinstance(child, Tag):
            continue
        parts.append(_render_tag(child, depth, ol_counter, ctx))
    return "".join(parts)


def _nav_line(tag: Tag) -> str:
    """A whole navigation subtree as one labeled line (THE V2 RULES): its link
    texts joined with a middle dot, ``Navigation: Home · About · Contact``.
    Falls back to the flat text for a link-less nav; an empty nav emits
    nothing. Duplicate lines within the page collapse in ``_cleanup``."""
    items: list[str] = []
    for a in tag.find_all("a"):
        text = re.sub(r"\s+", " ", a.get_text(" ")).strip()
        if text and (not items or items[-1] != text):
            items.append(text)
    body = " · ".join(items) if items else re.sub(r"\s+", " ", tag.get_text(" ")).strip()
    return f"\nNavigation: {body}\n" if body else "\n"


def _render_tag(tag: Tag, depth: int, ol_counter: list[int] | None, ctx: dict) -> str:
    name = tag.name

    if name == "nav" or str(tag.get("role", "")).strip().lower() == "navigation":
        return _nav_line(tag)

    if name in _HEADING_TAGS:
        text = re.sub(r"\s+", " ", _render_children(tag, depth, ctx=ctx)).strip()
        return f"\n{'#' * int(name[1])} {text}\n" if text else "\n"

    if name == "table":
        if _table_needs_html_island(tag):
            return "\n" + _table_to_island(tag) + "\n"
        return "\n" + _table_to_pipes(tag) + "\n"

    if name in ("ul", "ol"):
        counter = [0] if name == "ol" else None
        return _render_children(tag, depth + 1, counter, ctx)

    if name == "li":
        body = _render_children(tag, depth, None, ctx).strip()
        if not body:
            # Empty items (carousel dots, icon-only entries) emit nothing —
            # structural, so ordered numbering below stays gapless.
            return ""
        if ol_counter is not None:
            ol_counter[0] += 1
            marker = f"{ol_counter[0]}."
        else:
            marker = "-"
        indent = "  " * max(depth - 1, 0)
        return f"\n{indent}{marker} {body}"

    if name == "dt":
        text = re.sub(r"\s+", " ", _render_children(tag, depth, ctx=ctx)).strip()
        return f"\n**{text}:** " if text else "\n"

    if name == "dd":
        return _render_children(tag, depth, ol_counter, ctx) + "\n"

    if name == "img":
        alt = str(tag.get("alt") or "").strip()
        if not alt:
            return ""
        if ctx.get("last_alt") == alt:
            # consecutive duplicate alt (multi-variant logos: "Alec Model"
            # x1-3 per page) — collapse; real text in between resets (see
            # THE V2 RULES)
            return ""
        ctx["last_alt"] = alt
        return f" {alt} "

    if name in ("br", "hr"):
        return "\n"

    if name == "pre":
        # Preformatted text keeps its LINE structure as a plain block. Interior
        # space runs still collapse in _cleanup (the run-collapse that removes
        # doubles left by vanished inline elements is global): pre column
        # alignment is the cheaper loss — zero <pre> sightings in the 144-page
        # corpus measurement.
        return "\n" + tag.get_text() + "\n"

    if name in ("p", "blockquote", "figcaption", "caption", "summary", "figure", "details", "dl"):
        inner = _render_children(tag, depth, ol_counter, ctx)
        return f"\n{inner}\n"

    # Everything else — a, div, span, strong, em, label, ... — unwraps to
    # its children; block-level tags contribute line boundaries.
    inner = _render_children(tag, depth, ol_counter, ctx)
    if name in _BLOCK_TAGS:
        return f"\n{inner}\n"
    return inner


# --- tables ---------------------------------------------------------------------


def _int_attr(cell: Tag, attr: str) -> int:
    """The cell's rowspan/colspan as an int; unparseable values count as 1
    (the browser's own error handling is looser, but 1 keeps us deterministic
    and a garbage span never forces an island)."""
    raw = str(cell.get(attr, "1") or "1").strip()
    try:
        return int(raw)
    except ValueError:
        return 1


def _table_needs_html_island(table: Tag) -> bool:
    """Whether Markdown pipes cannot express *table* — the island predicate.

    True when any cell spans rows/columns, or any cell holds block content
    (nested table/list/dl/blockquote/pre/heading, or 2+ paragraphs). A pure
    function of the subtree: the Markdown/HTML mix in the output is exactly as
    deterministic as the page markup."""
    for cell in table.find_all(("td", "th")):
        if _int_attr(cell, "rowspan") != 1 or _int_attr(cell, "colspan") != 1:
            return True
        if cell.find(list(_CELL_BLOCK_TAGS)) is not None:
            return True
        if len(cell.find_all("p")) >= 2:
            return True
    return False


def _cell_text(cell: Tag) -> str:
    """A cell flattened to one pipe-safe line: whitespace collapsed, interior
    pipes escaped ``\\|`` (GFM's escape — the character survives, the column
    geometry does not break)."""
    text = re.sub(r"\s+", " ", cell.get_text(" ")).strip()
    return text.replace("|", "\\|")


def _table_to_pipes(table: Tag) -> str:
    rows: list[list[str]] = []
    for tr in table.find_all("tr"):
        cells = [_cell_text(c) for c in tr.find_all(("td", "th"), recursive=False)]
        if cells:
            rows.append(cells)
    caption = table.find("caption")
    caption_text = re.sub(r"\s+", " ", caption.get_text(" ")).strip() if caption else ""
    if not rows:
        return caption_text
    width = max(len(r) for r in rows)
    lines = ["| " + " | ".join(r + [""] * (width - len(r))) + " |" for r in rows]
    # GFM: the first row is the header row (a headerless data table still
    # needs one; the convention costs nothing and keeps the block parseable).
    lines.insert(1, "|" + "---|" * width)
    if caption_text:
        lines.insert(0, caption_text)
    return "\n".join(lines)


def _table_to_island(table: Tag) -> str:
    """*table* as a sanitized HTML island: structural tags only, rowspan/colspan
    the only surviving attributes, one ``<tr>`` per line (dedup matches lines,
    and the fold clips snippets line-wise — a row is the natural unit)."""
    lines: list[str] = ["<table>"]
    caption = table.find("caption")
    if caption:
        text = re.sub(r"\s+", " ", caption.get_text(" ")).strip()
        if text:
            lines.append(f"<caption>{text}</caption>")
    # Only THIS table's rows: a nested table (one of the island triggers) keeps
    # its content flattened inside its parent cell's text, its rows are not
    # promoted to rows of the outer table.
    for tr in (t for t in table.find_all("tr") if t.find_parent("table") is table):
        cells: list[str] = []
        for cell in tr.find_all(("td", "th"), recursive=False):
            attrs = ""
            for attr in ("rowspan", "colspan"):
                if _int_attr(cell, attr) != 1:
                    attrs += f' {attr}="{_int_attr(cell, attr)}"'
            text = re.sub(r"\s+", " ", cell.get_text(" ")).strip()
            cells.append(f"<{cell.name}{attrs}>{text}</{cell.name}>")
        if cells:
            lines.append("<tr>" + "".join(cells) + "</tr>")
    lines.append("</table>")
    return "\n".join(lines)


# --- final cleanup --------------------------------------------------------------


def _cleanup(text: str) -> str:
    # Interior space runs collapse (doubles appear where an empty inline
    # element vanished between spaced text). The \S guards keep line-LEADING
    # runs — nested-list indentation — untouched.
    text = re.sub(r"(?<=\S)[ \t]{2,}(?=\S)", " ", text)
    text = re.sub(r"[ \t]+\n", "\n", text)      # no trailing spaces
    text = re.sub(r"\n[ \t]+\n", "\n\n", text)  # no whitespace-only lines
    text = re.sub(r"\n{3,}", "\n\n", text)      # at most one blank line
    text = re.sub(r"^(#{1,6}) +", r"\1 ", text, flags=re.M)  # single space after #
    # Identical nav lines within the page (desktop + mobile menus) emit once
    # (THE V2 RULES); first occurrence keeps its position.
    seen_navs: set[str] = set()
    lines: list[str] = []
    for line in text.split("\n"):
        if line.startswith("Navigation: "):
            if line in seen_navs:
                continue
            seen_navs.add(line)
        lines.append(line)
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)  # re-collapse blanks the dedupe left
    return text.strip()


# --- v3 heading propagation ------------------------------------------------------

# THE V3 RULE's shape constants — frozen with the format version, like every
# other rule here. The values are the ones the 2026-09-02 A/B measured
# (label <=48 chars, run >=5); the heading cap keeps sentence-shaped headings
# from bloating every line of a run.
_LABEL_MAX_CHARS = 48
_HEADING_MAX_CHARS = 60
_LABEL_RUN_MIN = 5
_HEADING_LINE_RE = re.compile(r"^(#{1,6}) (.*)$")


def _is_label_line(line: str) -> bool:
    """Label-shaped: short, sentence-free, and not any other markup family
    this module emits (headings, tables, islands, lists, blockquotes, nav)."""
    s = line.strip()
    if not s or len(s) > _LABEL_MAX_CHARS:
        return False
    if s.startswith(("#", "|", "-", "*", ">", "<", "http", "Navigation:")):
        return False
    return re.search(r"[.!?:]$", s) is None


def _propagate_headings(text: str) -> str:
    """THE V3 RULE: each label in a run of >=5 label-shaped lines (blank lines
    allowed inside the run) is prefixed with the nearest preceding heading —
    ``WELLSAW: 58BD (1)``. Pure text-in/text-out, running on the cleaned-up
    rendering so the run structure it sees is exactly what ships."""
    lines = text.split("\n")
    heading = ""
    nearest: list[str] = []
    for line in lines:
        match = _HEADING_LINE_RE.match(line.strip())
        if match:
            title = match.group(2).strip()
            heading = title if len(title) <= _HEADING_MAX_CHARS else ""
        nearest.append(heading)

    out = list(lines)
    i, n = 0, len(lines)
    while i < n:
        if not _is_label_line(lines[i]):
            i += 1
            continue
        j, members = i, []
        while j < n and (_is_label_line(lines[j]) or not lines[j].strip()):
            if _is_label_line(lines[j]):
                members.append(j)
            j += 1
        if len(members) >= _LABEL_RUN_MIN:
            for k in members:
                title = nearest[k]
                label = lines[k].strip()
                if title and not label.lower().startswith(title.lower()):
                    out[k] = lines[k].replace(label, f"{title}: {label}", 1)
        i = j
    return "\n".join(out)
