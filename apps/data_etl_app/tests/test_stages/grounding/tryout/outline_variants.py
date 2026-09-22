"""The OUTLINE tryout (2026-09-21, user decision after
``docs_local/grounding_gap_survey_2026-09-14/VOCABULARY_FORMAT_ASSESSMENT_2026-09-21.md``):
five ways of pasting the vocabulary into the round-5 grounding prompt, one
variable per arm, in a NEW request layout that the provider's prefix cache can
serve — everything static first (system message = the instructions, then the
vocabulary), the nonce next, the records last.

Variants (``--outline`` on the runner; arm files ``b-<variant>_<k>.json``):

    dash      the round-5 outline unchanged: label line + a "— " line carrying
              the other names and the full definition (the reference arm in the
              new position; against ``b_<k>.json`` of round 5 it measures the
              position change alone)
    today     today's production outline: aliases inline as "(also: …)", no
              definitions
    defs2     the dash-line outline with definitions on depth 1 and 2 only
              (the user's proposal 2)
    layered   flat lists under "depth 1:", "depth 2:", … headings, no
              definitions, and the instruction to prefer the deeper option
              (the user's proposal 1)
    sandwich  the dash outline, plus the three decisive sentences repeated
              AFTER the records (the GPT-4.1 guide's instruction placement;
              the repeat sits in the dynamic tail, so the cache is untouched)

The system text of an arm is the assembled ``phrase_grounding`` prompt with
exactly the substitutions each shape needs (the outline paragraph; the phrase
that names what an option means; the "lines beneath it" note for the layered
shape) — recorded here, applied by string replacement, never written back to
a catalog. Every sentence is general; no domain instance is named.
"""

from __future__ import annotations

from collections import defaultdict
from types import SimpleNamespace
from typing import Any, Iterable

OUTLINES = ("dash", "today", "defs2", "layered", "sandwich")

VOCAB_HEADING = "the vocabulary to match against:"

# The skeleton's outline paragraph (round 5). Replaced whole for the shapes that
# differ; the entity noun is substituted per field.
_PARAGRAPH_DASH = (
    'You will also be given the vocabulary to match against, written as an indented outline. Each line that '
    'does not begin with "—" names one {noun}; a line indented under another names a narrower kind of the one '
    'above it, and every such line is an option you may choose, whether or not lines are indented beneath it. '
    'A line beginning with "—" belongs to the option directly above it: it gives the other names that option '
    'goes by and what it means, and is never an option itself. Name an option by the words of its own line '
    'only, without its indentation and without the lines above or beneath it.'
)

_PARAGRAPH_TODAY = (
    "You will also be given the vocabulary to match against, written as an indented outline. Each line names "
    "one {noun}, and a line indented under another names a narrower kind of the one above it. Every line is an "
    'option you may choose, whether or not it has lines indented beneath it. Where a line ends in "(also: …)", '
    "those are other names for that same {noun}, and choosing any of them chooses it. Name an option by its "
    'own words alone, without its indentation, without the lines above it, and without the "(also: …)" part.'
)

_PARAGRAPH_DEFS2 = _PARAGRAPH_DASH + (
    ' Not every option carries a line beginning with "—": an option without one means what its name says, '
    "read in its place in the outline."
)

_PARAGRAPH_LAYERED = (
    'You will also be given the vocabulary to match against, listed by depth. Under the heading "depth 1:" '
    'are the broadest kinds of {noun}; under "depth 2:" narrower kinds; and so on, each heading holding '
    "narrower kinds than the heading before it. Every line under a heading is an option you may choose. "
    'Where a line ends in "(also: …)", those are other names for that same {noun}, and choosing any of them '
    'chooses it. Name an option by its own words alone, without its heading and without the "(also: …)" '
    "part. Prefer a deeper option over a shallower one whenever the subject's words support the deeper one."
)

# What an option means (the catalog's ``option_evidence`` phrase inside GR-M1).
_EVIDENCE_DASH = "its own line and the line beneath it"
_EVIDENCE = {
    "dash": _EVIDENCE_DASH,
    "sandwich": _EVIDENCE_DASH,
    "today": "what its name says, read in its place in the outline",
    "defs2": "its own line, the line beneath it when there is one, and its place in the outline",
    "layered": "what its name says, read at the depth it is listed",
}

# GR-M2a as rendered (round 5) and its layered counterpart: the layered list has
# no "lines beneath", so the note names depth instead.
_M2A_DASH = (
    "Match the subject at the narrowest option its words support; an option with lines beneath it is chosen "
    "only when the subject supports none of them. Every option may be chosen, including one with lines "
    "indented beneath it."
)
_M2A_LAYERED = (
    "Match the subject at the deepest option its words support; a shallower option is chosen only when the "
    "subject supports no deeper option that is a kind of it. Every option may be chosen, at any depth."
)

# The sandwich tail: the three decisive sentences, repeated after the records.
SANDWICH_TAIL = (
    "Reminders that apply to every record above: the subject's own match comes first, and is never withheld "
    "or replaced by a match for something the synthesis mentions only beside the subject. Match the subject "
    "at the narrowest option its words support; an option with lines beneath it is chosen only when the "
    "subject supports none of them. Name an option by the words of its own line only."
)


def system_text(assembled: str, variant: str, noun: str) -> str:
    """The assembled round-5 prompt with the substitutions the shape needs."""
    if variant not in OUTLINES:
        raise ValueError(f"unknown outline variant {variant!r}")
    dash = _PARAGRAPH_DASH.format(noun=noun)
    if dash not in assembled:
        raise ValueError("the assembled prompt does not carry the round-5 outline paragraph")
    paragraph = {
        "dash": dash,
        "sandwich": dash,
        "today": _PARAGRAPH_TODAY.format(noun=noun),
        "defs2": _PARAGRAPH_DEFS2.format(noun=noun),
        "layered": _PARAGRAPH_LAYERED.format(noun=noun),
    }[variant]
    text = assembled.replace(dash, paragraph)
    if _EVIDENCE_DASH not in text:
        raise ValueError("the assembled prompt does not carry the round-5 option_evidence phrase")
    text = text.replace(_EVIDENCE_DASH, _EVIDENCE[variant])
    if variant == "layered":
        if _M2A_DASH not in text:
            raise ValueError("the assembled prompt does not carry the round-5 GR-M2a note")
        text = text.replace(_M2A_DASH, _M2A_LAYERED)
    return text


def _depth(concept: Any) -> int:
    return len(concept.ancestors) + 1


def _with_definitions_upto(concepts: Iterable[Any], upto: int) -> list[Any]:
    """Shallow clones with the definition blanked below ``upto``; the renderer
    then prints no dash line for those (a label without other names and
    without a definition gets none)."""
    out: list[Any] = []
    for c in concepts:
        out.append(
            SimpleNamespace(
                name=c.name,
                altLabels=list(c.altLabels),
                ancestors=list(c.ancestors),
                definition=(c.definition if _depth(c) <= upto else ""),
            )
        )
    return out


def _layered(concepts: Iterable[Any]) -> str:
    by_depth: dict[int, list[Any]] = defaultdict(list)
    for c in concepts:
        by_depth[_depth(c)].append(c)
    lines: list[str] = []
    for d in sorted(by_depth):
        lines.append(f"depth {d}:")
        for c in sorted(by_depth[d], key=lambda c: c.name):
            line = f"  {c.name}"
            if c.altLabels:
                line += f" (also: {', '.join(sorted(c.altLabels))})"
            lines.append(line)
    return "\n".join(lines)


def render_outline(concepts: Iterable[Any], variant: str) -> str:
    """The outline body for a variant, rendered by the production renderer
    wherever the shape is the production one."""
    from core.utils.rdf_to_graph_util import render_concept_outline  # noqa: E402

    cs = list(concepts)
    if variant in ("dash", "sandwich"):
        return render_concept_outline(cs, with_definitions=True)
    if variant == "today":
        return render_concept_outline(cs)
    if variant == "defs2":
        return render_concept_outline(_with_definitions_upto(cs, 2), with_definitions=True)
    if variant == "layered":
        return _layered(cs)
    raise ValueError(f"unknown outline variant {variant!r}")
