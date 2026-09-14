"""Run B's six field sections (approved in chat 2026-09-14; requirements doc §10 drafts revised against the
C16 base and the label experiment's lessons). One section per synthesis static, under the heading the
shared-block lint test knows (``test_synthesis_statics_shared_block.FIELD_SECTION_HEADING``), inserted after
"## Reading the shape of a snippet" and before "## The manufacturer's own name". Everything else in the six
statics stays byte-identical.

Used by run_tryout_b.py (arm "b" system texts) and by ``apply_to_statics`` (the publishable statics).

    field_sections.py arms      → pass2/b_<field>_system.txt from pass2/c16_system.txt
    field_sections.py statics   → rewrite the six statics under final_texts/ (C16 base + section)
"""

from __future__ import annotations

import pathlib
import sys

HEADING = "## What to settle about the dealing"
ANCHOR = "## The manufacturer's own name"

SECTIONS: dict[str, str] = {
    "products": (
        "Where the snippets show the manufacturer dealing in the focal entity, say in what capacity they fix it, in "
        "the snippets' own words for it: a thing it makes and sells as its own; a thing it makes or works on to "
        "another party's order, and whose specification, drawing, or brand that is; a thing another party makes "
        "that it resells, distributes, or represents; a thing it services, installs, tests, or inspects; a thing it "
        "uses as an input or a tool; or whatever else the snippets show. These are shapes such a dealing commonly "
        "takes, not a list to choose from: where the snippets fix none of them, say the dealing as shown and that "
        "the capacity is unstated, as with an entry on a line card or a list that says nothing about who made the "
        "things listed; where the sentence introducing a list fixes it, the item takes it, as said above. Where the "
        "snippets present the focal entity as something other than a thing dealt in, a kind of work, a substance, "
        "a document, a heading, say what they present it as."
    ),
    "equipments": (
        "Where the focal entity is a thing that does work, say what the snippets show it doing and for whom: a "
        "thing the manufacturer runs, keeps, or has at hand for its own work; a thing it offers to others, made by "
        "it or by another party as the snippets say; a thing a customer asks for or specifies; a thing an article "
        "explains; or whatever else the snippets show. A thing the manufacturer works with is not thereby a thing "
        "it makes, and a thing it lists is not thereby a thing it uses: keep the two apart as the snippets keep "
        "them. Keep the words that say what it does to the work and how it is built, driven, or controlled, and "
        "keep its maker and designation as given, without adding any the snippets do not state."
    ),
    "industries": (
        "Where the focal entity is a field of activity, a market, or a kind of customer, say on which side of the "
        "manufacturer's dealings it stands: the people the manufacturer makes for or sells into, its product going "
        "into it, its supplying those who work in it, a customer from it coming to the manufacturer; or the "
        "manufacturer's own line of work called by the same kind of word; or a channel it sells through; or "
        "whatever else the snippets show. Serving, supplying into, or making for it is a dealing in its own right: "
        "say it as such, and do not turn it into making the focal entity itself, nor into no dealing. Where the "
        "same word also names the manufacturer's own process, department, or equipment, the snippet's words decide "
        "which it is."
    ),
    "conformity_attestations": (
        "Where the focal entity is a standard, a rule, a mark, or a credential, say in what mode the snippets place "
        "the manufacturer against it and who says so, in the snippets' own words: granted, audited, registered, or "
        "listed by a named body; claimed to be met, by the manufacturer or by what it makes; tested to, and by whom; "
        "described or explained; required of others, suppliers, applicants, customers; a form the manufacturer asks "
        "others to complete; or whatever else the snippets show. Holding it and meeting it are different modes: keep "
        "the one the snippets give. Where the page shows it as the manufacturer's own, in a list of what it holds, "
        "as a badge, as a certificate offered for viewing, that showing is the dealing. Keep its number, issuer, and "
        "scope verbatim."
    ),
    "material_caps": (
        "Where the focal entity is a substance or a grade of one, say what the snippets show the manufacturer doing "
        "with it, working, forming, machining, finishing, supplying, stocking, offering it as an option, or whatever "
        "else they show, and where: its own plant or a partner's at its direction. Working a substance and making "
        "it are different dealings: keep the one the snippets give. Where the substance appears only as what a part "
        "is made of, as an entry in a customer's requirement, or in a supplier's sheet the manufacturer carries, say "
        "that and no more. A grade or series named in the focal form is a form of a substance: keep the designation "
        "and make the substance visible."
    ),
    "process_caps": (
        "Where the focal entity is a kind of work done to a thing, say who does it and where the snippets place it: "
        "the manufacturer in its own shop; a partner or outside facility the manufacturer sends work to; another "
        "company whose work the manufacturer lists or represents; a testing or certifying body; work at a "
        "customer's site or by the customer, including work a customer does with what the manufacturer supplies "
        "it; a step in one delivered project; or whatever else the snippets show. A sentence that says what the "
        "work is used for or suited to is evidence of the manufacturer's own work when the manufacturer's words "
        "claim the work, and the uses are its applications. Where the focal entity names a business arrangement, a "
        "credential, or a program rather than work done to a thing, say so."
    ),
}

# field -> static file name (one products call serves products and contract_products)
STATIC_OF: dict[str, str] = {
    "products": "product_phrase_synthesis.txt",
    "equipments": "equipment_phrase_synthesis.txt",
    "industries": "industry_phrase_synthesis.txt",
    "conformity_attestations": "conformity_attestation_phrase_synthesis.txt",
    "material_caps": "material_cap_phrase_synthesis.txt",
    "process_caps": "process_cap_phrase_synthesis.txt",
}


def render(base: str, field: str) -> str:
    """The static with the field's section inserted before the anchor heading."""
    if HEADING in base:
        raise ValueError("base text already carries a field section")
    at = base.find(ANCHOR)
    if at < 0:
        raise ValueError(f"anchor heading {ANCHOR!r} not found in the base text")
    return base[:at] + f"{HEADING}\n{SECTIONS[field]}\n\n" + base[at:]


HERE = pathlib.Path(__file__).resolve().parent
P2 = HERE / "pass2"
ROOT = next(p for p in HERE.parents if (p / ".git").exists())
STATICS_DIR = (
    ROOT / "apps/data_etl_app/src/data_etl_app/knowledge/prompts/final_texts/static/multi_stage/4_phrase_synthesis"
)


def build_arms() -> None:
    base = (P2 / "c16_system.txt").read_text(encoding="utf-8")
    for field in SECTIONS:
        out = P2 / f"b_{field}_system.txt"
        out.write_text(render(base, field), encoding="utf-8")
        print(f"wrote {out.name} ({len(out.read_text(encoding='utf-8')):,} chars)", file=sys.stderr)


def apply_to_statics() -> None:
    base = (P2 / "c16_system.txt").read_text(encoding="utf-8")
    for field, name in STATIC_OF.items():
        path = STATICS_DIR / name
        current = path.read_text(encoding="utf-8")
        if current != base:
            raise SystemExit(f"{name} is not the C16 base (md5 differs); refusing to rewrite it")
        path.write_text(render(base, field), encoding="utf-8")
        print(f"rewrote {name}", file=sys.stderr)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "arms"
    {"arms": build_arms, "statics": apply_to_statics}[mode]()
