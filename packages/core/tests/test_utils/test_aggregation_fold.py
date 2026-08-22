"""Phase 2.3 of pipeline v3 (PIPELINE_V3_PLAN.md D8, D9, D11, D19): the
aggregation fold — locate, re-attribute with longest-match containment,
same-occurrence dedup, bundles in locked order, tier-1 hold obligations."""

from hypothesis import given, settings
from hypothesis import strategies as st

from core.models.extraction_schemas.mention_collection import Mention
from core.services.phrase_blocks_contract import render_synthesis_record_blocks
from core.utils.aggregation_fold import (
    BUNDLE_STATUS_NO_MENTIONS,
    BUNDLE_STATUS_OK,
    FoldResult,
    WindowInput,
    fold_document,
    fold_window,
    obligations_by_form,
)
from core.utils.floor_scan import Occurrence, floor_scan, page_at
from core.utils.form_normalizer import NORMALIZER_VERSION, group_id_for_key, normalize
from core.utils.record_id_util import record_id_for_phrase

SEP = "#" * 50


def M(location, snippet):
    return Mention(location=location, snippet=snippet)


# ---------------------------------------------------------------------------
# Golden case: the prototype scenario of PIPELINE_V3_WALKTHROUGH_2_2.md §6
# ---------------------------------------------------------------------------

WINDOW = (
    f"{SEP}\nhttps://acme.example/materials\n\n"
    "We stock Aluminum and Brass. Sample Lead Time: 2 weeks.\n"
    "aluminum alloys ship daily.\n"
    f"{SEP}\nhttps://acme.example/about\n\n"
    "Lead-free solder only. Aluminum | Brass | Steel\n"
)
SENT = ["Aluminum", "aluminum", "Brass", "Lead", "Lead Time", "Sample Lead Time", "die-casting"]
INTRO = "We stock Aluminum and Brass. Sample Lead Time: 2 weeks."
ANSWER = {  # deliberately imperfect: sloppy casing, one sentence under several forms, lazy empties
    "Aluminum": [
        M("materials page, intro", INTRO),
        M("about page, footer menu", "Aluminum | Brass | Steel"),
    ],
    "aluminum": [
        M("materials page, intro", INTRO),
        M("materials page, line 2", "aluminum alloys ship daily."),
    ],
    "Brass": [M("materials page, intro", INTRO)],
    "Lead": [
        M("materials page, intro", "Sample Lead Time: 2 weeks."),
        M("about page", "Lead-free solder only."),
    ],
    "Lead Time": [],
    "Sample Lead Time": [],
    "die-casting": [],
}
MATERIALS, ABOUT = "https://acme.example/materials", "https://acme.example/about"


def _golden() -> FoldResult:
    return fold_document([WindowInput(WINDOW, SENT, ANSWER)])


def test_golden_bundles_keys_forms_and_order():
    result = _golden()
    assert [(b.key, b.forms, b.status) for b in result.bundles] == [
        ("aluminum", ("Aluminum", "aluminum"), BUNDLE_STATUS_OK),
        ("brass", ("Brass",), BUNDLE_STATUS_OK),
        ("sample lead time", ("Sample Lead Time",), BUNDLE_STATUS_OK),
        ("lead", ("Lead",), BUNDLE_STATUS_OK),
        ("die casting", ("die-casting",), BUNDLE_STATUS_NO_MENTIONS),  # never anchored
        ("lead time", ("Lead Time",), BUNDLE_STATUS_NO_MENTIONS),  # swallowed by containment
    ]
    assert [b.group_id for b in result.bundles] == [group_id_for_key(b.key) for b in result.bundles]
    assert result.normalizer_version == NORMALIZER_VERSION


def test_golden_mentions_are_re_attributed_and_paged_from_the_text():
    result = _golden()
    by_key = {b.key: b for b in result.bundles}
    assert [(m.start, m.form, m.page) for m in by_key["aluminum"].mentions] == [
        (92, "Aluminum", MATERIALS),
        (139, "aluminum", MATERIALS),
        (269, "Aluminum", ABOUT),
    ]
    # the intro sentence was filed once under Brass yet is a mention of Aluminum AND Brass
    assert [(m.start, m.snippet) for m in by_key["brass"].mentions] == [
        (105, INTRO),
        (280, "Aluminum | Brass | Steel"),
    ]
    # 'Lead' filed "Sample Lead Time: 2 weeks." — the spot belongs to the fuller span
    assert [(m.start, m.snippet) for m in by_key["sample lead time"].mentions] == [(112, INTRO)]
    assert [(m.start, m.snippet) for m in by_key["lead"].mentions] == [
        (246, "Lead-free solder only.")
    ]
    for b in result.bundles:
        for m in b.mentions:
            assert WINDOW[m.start:m.end] == m.form
            assert m.record_id == record_id_for_phrase(m.form)
            assert WINDOW[m.snippet_start:m.snippet_start + len(m.snippet)] == m.snippet


def test_golden_dedup_counts_and_hold():
    window = _golden().windows[0]
    assert (window.candidates, len(window.mentions)) == (14, 7)
    assert {f: len(v) for f, v in window.obligations.items()} == {
        "Aluminum": 2, "aluminum": 1, "Brass": 2, "Lead": 1, "Lead Time": 0,
        "Sample Lead Time": 1, "die-casting": 0,
    }
    assert not window.has_discrepancy and window.unaccounted_count == 0
    assert window.unlocated == [] and window.unanchored == []
    assert [(r.reported_form, r.position, r.attributed_forms) for r in window.rekeyed] == [
        ("aluminum", 83, ("Aluminum", "Brass", "Sample Lead Time")),
        ("Lead", 112, ("Sample Lead Time",)),
    ]


def test_golden_reported_form_prefers_the_collectors_own_filing():
    # The intro sentence was filed under Aluminum, aluminum AND Brass; the Brass
    # mention keeps the Brass-filed copy (not the lexically-first one).
    by_key = {b.key: b for b in _golden().bundles}
    brass_intro = by_key["brass"].mentions[0]
    assert brass_intro.reported_form == "Brass" and not brass_intro.rekeyed
    slt = by_key["sample lead time"].mentions[0]
    assert slt.rekeyed  # nobody filed it under 'Sample Lead Time'


def test_golden_synthesis_records_skip_empty_bundles_and_render():
    result = _golden()
    records = result.synthesis_records()
    assert [r.record_id for r in records] == [b.group_id for b in result.bundles if not b.is_empty]
    assert len(records) == 4 and len(result.empty_bundles) == 2
    first = records[0]
    assert [(e.location, e.snippet) for e in first.entries] == [
        ("materials page, intro", INTRO),
        ("materials page, line 2", "aluminum alloys ship daily."),
        ("about page, footer menu", "Aluminum | Brass | Steel"),
    ]
    rendered = render_synthesis_record_blocks([r.model_dump() for r in records])
    assert first.record_id in rendered and "die casting" not in rendered
    assert result.bundle(first.record_id) is result.bundles[0]
    assert result.bundle("gnothere") is None


# ---------------------------------------------------------------------------
# A: locate
# ---------------------------------------------------------------------------


def test_unlocated_snippet_anchors_nothing_and_is_reported():
    text = "We stock Aluminum here."
    answer = {"Aluminum": [M("p", "We stock aluminium here.")]}
    w = fold_window(WindowInput(text, ["Aluminum"], answer))
    assert w.mentions == []
    assert [(r.reported_form, r.snippet, r.position) for r in w.unlocated] == [
        ("Aluminum", "We stock aluminium here.", None)
    ]
    assert w.unaccounted == {"Aluminum": [Occurrence(9, 17)]} and w.has_discrepancy


def test_snippet_occurring_at_several_positions_yields_a_mention_per_position():
    text = (
        f"{SEP}\nhttps://x.example/a\n\nAluminum | Brass\nbody a\n"
        f"{SEP}\nhttps://x.example/b\n\nAluminum | Brass\nbody b\n"
    )
    answer = {"Aluminum": [M("footer", "Aluminum | Brass")]}  # reported once
    w = fold_window(WindowInput(text, ["Aluminum", "Brass"], answer))
    assert [(m.form, m.page) for m in w.mentions] == [
        ("Aluminum", "https://x.example/a"), ("Brass", "https://x.example/a"),
        ("Aluminum", "https://x.example/b"), ("Brass", "https://x.example/b"),
    ]
    assert not w.has_discrepancy


def test_anaphoric_snippet_is_unanchored():
    text = "Aluminum is stocked. It ships daily."
    w = fold_window(WindowInput(text, ["Aluminum"], {"Aluminum": [M("p", "It ships daily.")]}))
    assert w.mentions == []
    assert [(r.snippet, r.position) for r in w.unanchored] == [("It ships daily.", 21)]
    assert w.unaccounted["Aluminum"] == [Occurrence(0, 8)]


# ---------------------------------------------------------------------------
# B: re-attribution + longest-match containment (D8)
# ---------------------------------------------------------------------------


def test_fragment_of_a_fuller_span_cannot_claim_the_spot():
    text = "Sample Lead Time: 2 weeks."
    sent = ["Lead", "Lead Time", "Sample Lead Time"]
    w = fold_window(WindowInput(text, sent, {"Lead Time": [M("p", "Lead Time")]}))
    assert w.mentions == []  # the fragment anchors no owner: the spot is 'Sample Lead Time's
    assert [r.snippet for r in w.unanchored] == ["Lead Time"]
    assert w.obligations == {"Lead": [], "Lead Time": [], "Sample Lead Time": [Occurrence(0, 16)]}
    assert w.unaccounted["Sample Lead Time"] == [Occurrence(0, 16)]


def test_partial_overlap_is_not_containment_both_forms_own_their_span():
    text = "Sample Lead Time: 2 weeks."
    w = fold_window(WindowInput(text, ["Sample Lead", "Lead Time"], {"Lead Time": [M("p", text)]}))
    assert [(m.form, m.start, m.end) for m in w.mentions] == [
        ("Sample Lead", 0, 11),
        ("Lead Time", 7, 16),
    ]


def test_attribution_uses_the_window_not_the_snippet_edges():
    # A snippet cut mid-token: 'lead' at the snippet's first char is preceded by
    # a word char in the window, so it is no whole-word occurrence there.
    text = "unlead alloys are sold; lead alloys too."
    w = fold_window(WindowInput(text, ["lead"], {"lead": [M("p", "lead alloys")]}))
    assert [(m.start, m.end) for m in w.mentions] == [(24, 28)]
    assert not w.has_discrepancy


def test_sent_forms_are_window_local():
    w0 = WindowInput("Brass and Aluminum.", ["Brass"], {"Brass": [M("p", "Brass and Aluminum.")]})
    w1 = WindowInput("Aluminum only.", ["Aluminum"], {"Aluminum": [M("p", "Aluminum only.")]})
    result = fold_document([w0, w1])
    by_key = {b.key: b for b in result.bundles}
    assert [(m.window_index, m.start) for m in by_key["aluminum"].mentions] == [(1, 0)]
    assert [(m.window_index, m.start) for m in by_key["brass"].mentions] == [(0, 0)]


def test_a_report_under_an_unknown_key_still_attributes_from_the_text():
    text = "Brass parts."
    w = fold_window(WindowInput(text, ["Brass"], {"brass": [M("p", text)]}))
    assert [(m.form, m.reported_form, m.rekeyed) for m in w.mentions] == [("Brass", "brass", True)]
    assert [r.reported_form for r in w.rekeyed] == ["brass"]


# ---------------------------------------------------------------------------
# C: D19 dedup on (group, occurrence span)
# ---------------------------------------------------------------------------


def test_longer_snippet_of_one_spot_wins_whatever_the_answer_order():
    text = "We stock Brass. Sample Lead Time: 2 weeks."
    short, long = "Sample Lead Time: 2 weeks.", text
    a = {"Sample Lead Time": [M("p", short), M("q", long)]}
    b = {"Sample Lead Time": [M("q", long), M("p", short)]}
    for answer in (a, b):
        w = fold_window(WindowInput(text, ["Sample Lead Time"], answer))
        assert [(m.snippet, m.location) for m in w.mentions] == [(long, "q")]
        assert w.candidates == 2


def test_two_genuine_occurrences_in_one_sentence_stay_two_mentions():
    text = "Aluminum, and more Aluminum."
    w = fold_window(WindowInput(text, ["Aluminum"], {"Aluminum": [M("p", text)]}))
    assert [(m.start, m.end) for m in w.mentions] == [(0, 8), (19, 27)]


def test_case_variant_forms_never_collide_on_a_spot():
    text = "Aluminum and aluminum."
    w = fold_window(WindowInput(text, ["Aluminum", "aluminum"], {"aluminum": [M("p", text)]}))
    assert [(m.form, m.start) for m in w.mentions] == [("Aluminum", 0), ("aluminum", 13)]


# ---------------------------------------------------------------------------
# D: grouping + bundles
# ---------------------------------------------------------------------------


def test_groups_are_global_and_mentions_keep_window_then_offset_order():
    w0 = WindowInput(
        f"{SEP}\nhttps://x.example/a\n\nAluminum sheet. Steel.\n",
        ["Aluminum", "Steel"],
        {"Aluminum": [M("a", "Aluminum sheet.")], "Steel": [M("a", "Steel.")]},
    )
    w1 = WindowInput(
        "aluminum bar.\n",
        ["aluminum"],
        {"aluminum": [M("b", "aluminum bar.")]},
        preceding_page="https://x.example/a",
    )
    result = fold_document([w0, w1])
    assert [(b.key, b.forms) for b in result.bundles] == [
        ("aluminum", ("Aluminum", "aluminum")), ("steel", ("Steel",)),
    ]
    aluminum = result.bundles[0]
    assert [(m.window_index, m.start, m.page) for m in aluminum.mentions] == [
        (0, 72, "https://x.example/a"), (1, 0, "https://x.example/a"),
    ]


def test_bundle_order_is_first_mention_then_empties_by_key():
    text = "Zinc first, then Brass. Aluminum last."
    sent = ["Brass", "Aluminum", "Zinc", "Tin", "Copper"]
    result = fold_document([WindowInput(text, sent, {"Zinc": [M("p", text)]})])
    assert [b.key for b in result.bundles] == ["zinc", "brass", "aluminum", "copper", "tin"]


def test_verb_fold_is_a_dial_applied_to_the_keys():
    text = "CNC milled parts; CNC milling too."
    sent = ["CNC milled", "CNC milling"]
    answer = {"CNC milled": [M("p", text)]}
    off = fold_document([WindowInput(text, sent, answer)])
    on = fold_document([WindowInput(text, sent, answer)], verb_fold=True)
    # L1 alone does not fold -ed/-ing; the dial does
    assert sorted(b.key for b in off.bundles) == ["cnc milled", "cnc milling"]
    assert [b.key for b in on.bundles] == ["cnc mill"] and on.verb_fold
    assert len(on.bundles[0].mentions) == 2 and on.bundles[0].forms == ("CNC milled", "CNC milling")


def test_blank_forms_are_ignored_and_an_empty_document_folds_to_nothing():
    w = fold_window(WindowInput("a b", ["", "  ", "a"], {}))
    assert list(w.obligations) == ["a"]
    assert fold_document([]).bundles == [] and fold_document([]).synthesis_records() == []


# ---------------------------------------------------------------------------
# E: tier-1 hold under the same containment rule
# ---------------------------------------------------------------------------


def test_obligations_of_sub_forms_exclude_spots_owned_by_fuller_spans():
    text = "Sample Lead Time: 2 weeks. Lead-free. Lead Time matters."
    scan = floor_scan(text, ["Lead", "Lead Time", "Sample Lead Time"])
    assert [len(v) for v in scan.tier1.values()] == [3, 2, 1]  # raw hits
    assert obligations_by_form(scan) == {
        "Lead": [Occurrence(27, 31)],
        "Lead Time": [Occurrence(38, 47)],
        "Sample Lead Time": [Occurrence(0, 16)],
    }


def test_unaccounted_is_exactly_the_uncovered_obligations():
    text = "Brass here. Brass there. Brass everywhere."
    w = fold_window(WindowInput(text, ["Brass"], {"Brass": [M("p", "Brass there.")]}))
    assert w.unaccounted["Brass"] == [Occurrence(0, 5), Occurrence(25, 30)]
    assert w.unaccounted_count == 2


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

_letters = st.text(
    alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd"), max_codepoint=0x24F),
    min_size=1,
    max_size=6,
)
_token = st.one_of(
    _letters,
    st.sampled_from(
        ["6061-T6", "C++", "Lead-free", "Lead", "Time", "Sample", "Aluminum", "aluminum",
         "Lead Time"]
    ),
)
_sep = st.sampled_from([" ", " ", " ", "\n", ", ", ". "])
_location = st.text(max_size=8)


@st.composite
def _scenario(draw):
    tokens = draw(st.lists(_token, min_size=1, max_size=14))
    seps = draw(st.lists(_sep, min_size=len(tokens) - 1, max_size=len(tokens) - 1))
    header = f"{SEP}\nhttps://x.example/p\n\n" if draw(st.booleans()) else ""
    text, starts = header, []
    for i, tok in enumerate(tokens):
        starts.append(len(text))
        text += tok + (seps[i] if i < len(seps) else "")
    ends = [s + len(t) for s, t in zip(starts, tokens, strict=True)]
    n = len(tokens)
    singles = draw(st.lists(st.sampled_from(tokens), max_size=5))
    pair_ix = draw(st.lists(st.integers(0, max(n - 2, 0)), max_size=3)) if n >= 2 else []
    pairs = [text[starts[i]:ends[i + 1]] for i in pair_ix]
    sent = list(dict.fromkeys(singles + pairs + draw(st.lists(_token, max_size=2))))
    reports = []
    for _ in range(draw(st.integers(0, 6))):
        i = draw(st.integers(0, n - 1))
        j = draw(st.integers(i, n - 1))
        snippet = text[starts[i]:ends[j]] if draw(st.integers(0, 4)) else draw(st.text(max_size=10))
        reported = draw(st.sampled_from(sent)) if sent and draw(st.booleans()) else draw(_token)
        reports.append((reported, draw(_location), snippet))
    return text, sent, reports


def _window(text, sent, reports, *, rng=None):
    sent, reports = list(sent), list(reports)
    if rng is not None:
        rng.shuffle(sent)
        rng.shuffle(reports)
    answer = {}
    for reported, loc, snippet in reports:
        answer.setdefault(reported, []).append(M(loc, snippet))
    return WindowInput(text, sent, answer)


def _signature(result: FoldResult):
    bundles = tuple(
        (b.group_id, b.key, b.forms, tuple(
            (m.window_index, m.start, m.end, m.form, m.record_id, m.page,
             m.snippet, m.snippet_start, m.location, m.reported_form) for m in b.mentions))
        for b in result.bundles
    )
    windows = tuple(
        (
            {f: tuple(v) for f, v in w.obligations.items()},
            {f: tuple(v) for f, v in w.unaccounted.items()},
            tuple(sorted((r.reported_form, r.location, r.snippet) for r in w.unlocated)),
            tuple(
                sorted((r.reported_form, r.location, r.snippet, r.position) for r in w.unanchored)
            ),
            tuple(
                sorted(
                    (r.reported_form, r.snippet, r.position, r.attributed_forms) for r in w.rekeyed
                )
            ),
            w.candidates,
        )
        for w in result.windows
    )
    return bundles, windows


@settings(max_examples=150, deadline=None)
@given(_scenario(), st.randoms(use_true_random=False), st.booleans())
def test_fold_is_independent_of_sent_and_answer_order(scenario, rng, verb_fold):
    text, sent, reports = scenario
    plain = fold_document([_window(text, sent, reports)], verb_fold=verb_fold)
    shuffled = fold_document([_window(text, sent, reports, rng=rng)], verb_fold=verb_fold)
    assert _signature(plain) == _signature(shuffled)


@settings(max_examples=150, deadline=None)
@given(_scenario())
def test_fold_is_idempotent_on_its_own_mentions(scenario):
    text, sent, reports = scenario
    first = fold_window(_window(text, sent, reports))
    fed_back = {}
    for m in first.mentions:
        fed_back.setdefault(m.form, []).append(M(m.location, m.snippet))
    second = fold_window(WindowInput(text, sent, fed_back))
    key = lambda m: (m.form, m.start, m.end, m.snippet, m.location)  # noqa: E731
    assert sorted(map(key, second.mentions)) == sorted(map(key, first.mentions))
    assert second.unaccounted == first.unaccounted


@settings(max_examples=200, deadline=None)
@given(_scenario(), st.booleans())
def test_fold_invariants(scenario, verb_fold):
    text, sent, reports = scenario
    window = _window(text, sent, reports)
    result = fold_document([window], verb_fold=verb_fold)
    w = result.windows[0]
    scan = floor_scan(text, [f for f in sent if f.strip()])
    all_hits = [(o.start, o.end) for occs in scan.tier1.values() for o in occs]
    located_spans = []
    for _, _, snippet in reports:
        if snippet:
            i = text.find(snippet)
            while i != -1:
                located_spans.append((i, i + len(snippet)))
                i = text.find(snippet, i + 1)

    # every kept mention is anchored in the text, exactly
    assert w.mentions == sorted(w.mentions)
    spans = [(m.start, m.end) for m in w.mentions]
    assert len(set(spans)) == len(spans)
    for m in w.mentions:
        assert text[m.start:m.end] == m.form and m.form in sent
        assert text[m.snippet_start:m.snippet_start + len(m.snippet)] == m.snippet
        assert m.snippet_start <= m.start and m.end <= m.snippet_start + len(m.snippet)
        assert m.page == page_at(scan.pages, m.start)
        assert m.record_id == record_id_for_phrase(m.form)
        assert (m.start, m.end) in all_hits
        # containment: no kept occurrence sits strictly inside another kept or scanned one
        assert not any(
            s <= m.start and m.end <= e and (e - s) > (m.end - m.start) for s, e in all_hits
        )
    # obligations = tier-1 hits minus contained ones; unaccounted = the uncovered obligations
    for form, occs in w.obligations.items():
        for o in occs:
            assert o in scan.tier1[form]
            assert not any(
                s <= o.start and o.end <= e and (e - s) > (o.end - o.start) for s, e in all_hits
            )
            covered = any(s <= o.start and o.end <= e for s, e in located_spans)
            assert (o in w.unaccounted[form]) == (not covered)
    # bundles: a partition of the sent forms by key, ids unique, mentions keyed to their bundle
    forms = [f for b in result.bundles for f in b.forms]
    assert sorted(forms) == sorted({f for f in sent if f.strip()})
    assert len({b.group_id for b in result.bundles}) == len(result.bundles)
    for b in result.bundles:
        assert all(normalize(f, verb_fold=verb_fold) == b.key for f in b.forms)
        assert all(m.form in b.forms for m in b.mentions)
        assert list(b.mentions) == sorted(b.mentions)
    assert len(result.synthesis_records()) == sum(1 for b in result.bundles if not b.is_empty)
