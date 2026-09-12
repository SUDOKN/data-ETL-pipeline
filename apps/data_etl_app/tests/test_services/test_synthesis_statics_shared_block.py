"""The six synthesis statics share one block of text by design.

Decision 2026-09-11 (field-specific synthesis, survey doc §7.2 item 3): the
common logic of the six hand-written synthesis prompts is kept identical by a
lint test rather than by a catalog/skeleton render. A static may carry ONE
field-specific section, headed by ``FIELD_SECTION_HEADING``; everything before
that heading and everything after the section (the next ``## `` heading
onwards) must be byte-identical across the six files. Without a field section
the six files must be byte-identical in full.

``final_texts/`` is gitignored build output, so the test skips where the tree
is absent rather than failing a clone that never rendered prompts.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from data_etl_app.services.prompt_assembly_service import STATIC_PROMPTS_DIR

SYNTHESIS_DIR = STATIC_PROMPTS_DIR / "multi_stage" / "4_phrase_synthesis"
STATIC_NAMES = (
    "conformity_attestation_phrase_synthesis.txt",
    "equipment_phrase_synthesis.txt",
    "industry_phrase_synthesis.txt",
    "material_cap_phrase_synthesis.txt",
    "process_cap_phrase_synthesis.txt",
    "product_phrase_synthesis.txt",
)
FIELD_SECTION_HEADING = "## What to settle about the dealing"


def split_shared(text: str) -> tuple[str, str | None, str]:
    """``(before, field_section, after)``; ``field_section`` is None when the
    static carries none. The section runs from its heading to the next ``## ``
    heading (exclusive), so the shared tail can be compared too."""
    start = text.find(FIELD_SECTION_HEADING)
    if start < 0:
        return text, None, ""
    nxt = text.find("\n## ", start + len(FIELD_SECTION_HEADING))
    if nxt < 0:
        return text[:start], text[start:], ""
    return text[:start], text[start : nxt + 1], text[nxt + 1 :]


def _load() -> dict[str, str]:
    if not SYNTHESIS_DIR.is_dir():
        pytest.skip(f"{SYNTHESIS_DIR} not rendered in this checkout")
    texts = {}
    for name in STATIC_NAMES:
        path = SYNTHESIS_DIR / name
        if not path.exists():
            pytest.skip(f"{path.name} not rendered in this checkout")
        texts[name] = path.read_text(encoding="utf-8")
    return texts


def test_shared_block_identical_across_the_six_statics() -> None:
    texts = _load()
    reference_name = STATIC_NAMES[-1]
    ref_before, _, ref_after = split_shared(texts[reference_name])
    for name, text in texts.items():
        before, _, after = split_shared(text)
        assert before == ref_before, (
            f"{name}: text BEFORE the field section differs from {reference_name}"
        )
        assert after == ref_after, (
            f"{name}: text AFTER the field section differs from {reference_name}"
        )


def test_field_section_appears_at_most_once() -> None:
    for name, text in _load().items():
        assert text.count(FIELD_SECTION_HEADING) <= 1, name


def test_split_shared_round_trips() -> None:
    text = "## A\nshared\n" + FIELD_SECTION_HEADING + "\nfield text\n## B\ntail\n"
    before, section, after = split_shared(text)
    assert section is not None
    assert before + section + after == text
    assert section == FIELD_SECTION_HEADING + "\nfield text\n"
    assert split_shared("## A\nonly shared\n") == ("## A\nonly shared\n", None, "")
