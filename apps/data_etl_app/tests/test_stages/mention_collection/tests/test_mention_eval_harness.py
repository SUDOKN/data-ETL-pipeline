"""The mention-eval harness's own tests. They guard this code, not the pipeline.

Unit half: pure logic, no dumps, runs in the default suite. The structural
invariants over a real run live in `test_mention_eval_run.py` behind the
`integration` marker, because a checkout has no dumps.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# Imports come from the uniquely-named package conftest.py registers. Never
# `from checks import ...` here: two sibling stages both ship a `checks/`, and
# a bare import binds whichever is seen first for the whole pytest session.
from _shared.text_matching import occurs_in  # noqa: E402
# `mention_checks` is registered at runtime by conftest.py, so no static
# analyser can resolve it; the alias is what keeps this stage from
# shadowing a sibling's `checks` package.
from mention_checks import loading, mechanical, paths  # type: ignore[import-not-found]  # noqa: E402


def _mention(
    form: str,
    span: tuple[int, int],
    *,
    group_id: str = "g1",
    key: str = "k",
    snippet: str = "",
    location: str = "Sentence of prose, in the site's own copy.",
    location_source: str = paths.LOCATION_SOURCE_LLM,
    window: int = 0,
    mention_id: str | None = None,
    sent_form: str | None = None,
    page: str | None = "https://example.com/",
) -> loading.Mention:
    return loading.Mention(
        form=form,
        record_id="r" + form[:6],
        window=window,
        span=span,
        page=page,
        mention_id=mention_id or ("m" + form[:6]),
        location=location,
        location_source=location_source,
        snippet=snippet or f"We supply {form} to the trade.",
        sent_form=sent_form,
        doc_span=None,
        group_id=group_id,
        group_key=key,
        chunk_bounds="0:100",
    )


def _group(mentions: list[loading.Mention], *, status: str = "ok", **kw) -> loading.Group:
    first = mentions[0] if mentions else None
    return loading.Group(
        group_id=kw.get("group_id", first.group_id if first else "g0"),
        key=kw.get("key", first.group_key if first else "k"),
        forms=tuple(kw.get("forms", tuple({m.form for m in mentions}))),
        status=status,
        mention_count=kw.get("mention_count", len(mentions)),
        distinct_snippets=len({m.mention_id for m in mentions}),
        collapsed_into=kw.get("collapsed_into"),
        own_name_hits_in_snippets=0,
        mentions=tuple(mentions),
        chunk_bounds="0:100",
    )


def _run(groups: list[loading.Group], **kw) -> loading.FieldRun:
    run = loading.FieldRun(
        run_id="TESTRUN",
        subject=kw.get("subject", "example.com"),
        field=kw.get("field", "products"),
        path=Path("/dev/null"),
        partial=False,
        has_fold=True,
        groups=groups,
    )
    run.summaries = kw.get("summaries", [{}])
    run.windows = kw.get("windows", [])
    run.scraped_text = kw.get("scraped_text", {})
    run.metadata = kw.get("metadata", {})
    return run


def _report(run: loading.FieldRun) -> mechanical.FieldReport:
    return mechanical.evaluate_run([run])[run.key]


# --------------------------------------------------------------------------
# paths


def test_subject_slug_keeps_hyphens_and_only_dots_become_underscores():
    # The corpus dir is `anchor-mfg_com`; turning the hyphen into an underscore
    # would silently point every lookup at a directory that does not exist.
    assert paths.subject_slug("anchor-mfg.com") == "anchor-mfg_com"
    assert paths.subject_of_slug("anchor-mfg_com") == "anchor-mfg.com"
    assert paths.subject_of_slug(paths.subject_slug("steelcraft.com")) == "steelcraft.com"


def test_contract_products_is_excluded_from_judged_fields():
    assert paths.SHARED_DUPLICATE in paths.PHRASE_FIELDS
    assert paths.SHARED_DUPLICATE not in paths.JUDGED_FIELDS
    assert len(paths.PHRASE_FIELDS) == 7
    assert len(paths.JUDGED_FIELDS) == 6


def test_parse_custom_id_reads_the_address_and_settings():
    cid = (
        "alecmfg.com>material_caps>llm_phrase_mention_collection>chunk>0:96974"
        ">sub>0:23771>group>0>gpt-4.1|temperature=0.0|pv=r8WpLIc|gs=50|ud=5772ebb5"
    )
    parsed = paths.parse_custom_id(cid)
    assert parsed is not None
    assert parsed["subject"] == "alecmfg.com"
    assert parsed["field"] == "material_caps"
    assert parsed["chunk"] == "0:96974"
    assert parsed["sub"] == "0:23771"
    assert parsed["pv"] == "r8WpLIc"
    assert parsed["ud"] == "5772ebb5"


def test_parse_custom_id_rejects_another_stage():
    # The search harness asserts the mirror of this; a stage that claims another
    # stage's requests reports numbers for work it did not do.
    assert paths.parse_custom_id("a.com>products>llm_search>chunk>0:1>sub>0:1") is None


def test_taxonomy_version_changes_with_the_file(tmp_path, monkeypatch):
    first = paths.taxonomy_version()
    fake = tmp_path / "TAXONOMY.md"
    fake.write_text("different contract", encoding="utf-8")
    monkeypatch.setattr(paths, "TAXONOMY_FILE", fake)
    assert paths.taxonomy_version() != first


# --------------------------------------------------------------------------
# nesting (the D8 reversal)


def test_nested_mentions_finds_a_shorter_form_inside_a_longer_one():
    """The measurement the 2026-08-27 D8 reversal created.

    A pre-reversal run yields zero pairs because containment dropped the inner
    hit; without this test a broken detector and a correct zero look identical.
    """
    text_span_outer = (10, 27)  # "doors and frames"
    text_span_inner = (10, 15)  # "doors"
    outer = _mention("doors and frames", text_span_outer, group_id="gOUT", key="doors and frame")
    inner = _mention("doors", text_span_inner, group_id="gIN", key="door")
    run = _run([_group([outer]), _group([inner])])

    pairs = run.nested_mentions()
    assert len(pairs) == 1
    got_inner, got_outer = pairs[0]
    assert got_inner.form == "doors"
    assert got_outer.form == "doors and frames"


def test_nested_mentions_ignores_a_form_nested_in_its_own_group():
    # Two casings of one form land in one group; that is the casing rescue, not
    # decentralization, and counting it as inherited evidence would inflate the
    # I1 population with cases no one needs to judge.
    outer = _mention("Doors", (10, 15), group_id="g1", key="door")
    inner = _mention("door", (10, 14), group_id="g1", key="door")
    run = _run([_group([outer, inner])])
    assert run.nested_mentions() == []


def test_nested_mentions_ignores_merely_overlapping_spans():
    left = _mention("stainless steel", (10, 25), group_id="gA", key="stainless steel")
    right = _mention("steel plate", (20, 31), group_id="gB", key="steel plate")
    run = _run([_group([left]), _group([right])])
    assert run.nested_mentions() == []


def test_nested_mentions_does_not_cross_windows():
    outer = _mention("doors and frames", (10, 27), group_id="gOUT", key="a", window=0)
    inner = _mention("doors", (10, 15), group_id="gIN", key="b", window=1)
    run = _run([_group([outer]), _group([inner])])
    assert run.nested_mentions() == []


# --------------------------------------------------------------------------
# span integrity


def test_form_must_occur_in_its_own_snippet():
    good = _mention("frames", (0, 6), snippet="We supply frames to the trade.")
    bad = _mention("frames", (0, 6), snippet="We supply doors to the trade.")
    assert _report(_run([_group([good])])).status == "OK"
    report = _report(_run([_group([bad])]))
    assert "span.form_in_own_snippet" in report.reds


def test_non_breaking_space_in_a_snippet_is_not_a_defect():
    """Trap 1 from `_shared/text_matching`, reproduced at this stage.

    steelcraft alone carries 843 non-breaking spaces mid-phrase. A naive
    containment check calls the faithful snippet a defect; the search harness
    shipped a false RED on exactly this before the cause was found.
    """
    snippet = "Tested in accordance with ASTM D4585 for durability."
    mention = _mention("ASTM D4585", (26, 36), snippet=snippet)
    assert occurs_in("ASTM D4585", snippet)
    assert _report(_run([_group([mention])])).status == "OK"


def test_short_form_is_not_credited_to_a_longer_word():
    """Trap 3: plain containment credits 'competitive' for PET and 'Military'
    for ITAR — both live on superiortech.org."""
    assert not occurs_in("PET", "our competitive pricing")
    assert not occurs_in("ITAR", "Military and defense work")
    assert occurs_in("PET", "PET sheet stock")


def test_one_mention_id_must_mean_one_snippet():
    a = _mention("frames", (0, 6), mention_id="mSAME", snippet="We supply frames here.")
    b = _mention("frames", (9, 15), mention_id="mSAME", snippet="Different frames line.")
    report = _report(_run([_group([a, b])]))
    assert "span.mention_id_is_snippet_hash" in report.reds


def test_duplicate_occurrence_of_one_group_at_one_span_is_red():
    a = _mention("frames", (5, 11), mention_id="m1")
    b = _mention("frames", (5, 11), mention_id="m1")
    report = _report(_run([_group([a, b])]))
    assert "span.no_duplicate_occurrences" in report.reds


# --------------------------------------------------------------------------
# location content bans


@pytest.mark.parametrize(
    "location, expected_check",
    [
        ("Body copy on https://example.com/products", "location.no_url"),
        ("This passage is a heading in the catalogue.", "location.no_banned_opener"),
        ("It appears in the FEATURES section.", "location.no_banned_verb_opener"),
        ("Same section as the previous mention.", "location.no_cross_reference"),
    ],
)
def test_banned_location_content_gates(location, expected_check):
    mention = _mention("frames", (0, 6), location=location)
    report = _report(_run([_group([mention])]))
    assert expected_check in report.reds


def test_a_clean_location_passes_every_ban():
    mention = _mention(
        "frames",
        (0, 6),
        location="Bullet point under the 'Hollow Metal' heading, in the site's own copy.",
    )
    assert _report(_run([_group([mention])])).status == "OK"


def test_undescribed_snippet_is_not_judged_for_content():
    # A defaulted location is a delivery fact, not a content violation; running
    # the bans over the literal sentinel would invent findings.
    mention = _mention(
        "frames",
        (0, 6),
        location=paths.DEFAULT_LOCATION,
        location_source=paths.LOCATION_SOURCE_NONE,
    )
    report = _report(_run([_group([mention])]))
    assert report.metrics["locations_described"] == 0
    assert report.metrics["locations_default"] == 1
    assert not report.reds


def test_attribution_boilerplate_is_measured_not_gated():
    """`in the site's own copy` fills 99.5% of the attribution slot. Whether it
    is CORRECT is the S4 judgment; it must never gate mechanically."""
    mention = _mention("frames", (0, 6), location="Quote from a customer, in the site's own copy.")
    report = _report(_run([_group([mention])]))
    assert report.metrics["attribution_boilerplate"] == 1
    assert report.metrics["attribution_boilerplate_share"] == 1.0
    assert not report.reds


# --------------------------------------------------------------------------
# delivery, groups, pages


def test_undescribed_snippets_after_retry_gate():
    run = _run([_group([_mention("frames", (0, 6))])], summaries=[{"not_described": 3}])
    assert "delivery.not_described" in _report(run).reds


def test_nonce_echo_is_its_own_finding():
    nonce = "0" * 32
    window = loading.Window(
        window=0, sub_bounds="0:100", sent_forms=1, forms_with_hits=1,
        zero_hit_forms=(), mentions=1, distinct_snippets=1, described=0,
        not_described=(), retried=(), unknown_answer_ids=(nonce,),
        discovered_casings={}, short_forms=(), excluded_pages=(),
        chunk_bounds="0:100",
    )
    run = _run([_group([_mention("frames", (0, 6))])], windows=[window])
    report = _report(run)
    assert "delivery.nonce_echo" in report.reds
    assert "delivery.unknown_answer_ids" in report.reds


def test_collapsed_group_must_name_its_target():
    """`collapsed` is the D21 status added 2026-08-27. It is legal here and is
    NOT yet known to the grounding harness — see BOUNDARY.md."""
    ok = _group([], status="collapsed", group_id="gC", key="doors and frames",
                collapsed_into="gA", mention_count=0)
    assert "group.known_status" not in _report(_run([ok])).reds
    assert "group.collapsed_names_its_target" not in _report(_run([ok])).reds

    bad = _group([], status="collapsed", group_id="gC", key="k", mention_count=0)
    assert "group.collapsed_names_its_target" in _report(_run([bad])).reds


def test_unknown_group_status_gates():
    weird = _group([], status="something_new", group_id="gX", key="k", mention_count=0)
    assert "group.known_status" in _report(_run([weird])).reds


def test_mention_on_an_excluded_legal_page_gates():
    legal = "https://example.com/privacy-policy"
    mention = _mention("frames", (0, 6), page=legal)
    run = _run(
        [_group([mention])],
        scraped_text={"excluded_pages": {"pages": [{"url": legal, "start": 0, "end": 9}]}},
    )
    assert "page.no_mention_on_excluded_page" in _report(run).reds


def test_group_over_the_synthesis_packing_limit_is_counted():
    # 50 entries per synthesis request is where decentralization stops being a
    # recall win and starts feeding cross-record evidence bleed.
    mentions = [
        _mention("door", (i * 10, i * 10 + 4), mention_id=f"m{i}", snippet=f"line {i} door here")
        for i in range(60)
    ]
    report = _report(_run([_group(mentions)]))
    assert report.metrics["max_entries_in_a_group"] == 60
    assert report.metrics["groups_over_50_entries"] == 1


# --------------------------------------------------------------------------
# run-level


def test_a_dump_without_a_fold_block_is_blind_not_failing():
    """Every full-run dump before 2026-08-27 carried group counts and no
    mentions. Reporting zeros for those would be a lie; BLIND says it."""
    run = loading.FieldRun(
        run_id="OLD", subject="example.com", field="products",
        path=Path("/dev/null"), partial=False, has_fold=False,
    )
    run.row_mention_counts = {"g1": 4, "g2": 7}
    report = _report(run)
    assert report.status == "BLIND"
    assert report.reds == []
    assert report.metrics["row_mentions"] == 11


def test_products_and_contract_products_must_be_the_same_fold():
    left = _run([_group([_mention("frames", (0, 6))])], field="products")
    right = _run([_group([_mention("frames", (9, 15))])], field="contract_products")
    reports = mechanical.evaluate_run([left, right])
    assert "identity.products_equals_contract_products" in reports[left.key].reds


def test_pv_disagreeing_with_the_header_gates():
    run = _run(
        [_group([_mention("frames", (0, 6))])],
        metadata={paths.STAGE_REQUEST_TOKEN: {"prompt_version_id": "DECLARED"}},
    )
    run.requests = [
        {"custom_id": "a.com>products>llm_phrase_mention_collection>chunk>0:1"
                      ">sub>0:1>group>0>gpt-4.1|pv=SOMETHINGELSE|gs=50"}
    ]
    assert "identity.pv_matches_metadata" in _report(run).reds


def test_nominations_are_not_verdicts():
    """String scans steer a reader's attention and decide nothing. Regex over
    this pipeline's prose has mismeasured in both directions repeatedly."""
    restating = _mention(
        "frames",
        (0, 6),
        snippet="We supply hollow metal frames to commercial builders.",
        location="Sentence about supplying hollow metal frames to commercial builders.",
    )
    run = _run([_group([restating])])
    nominations = mechanical.nominate_for_judgment(run)
    assert restating.mention_id in nominations["restatement"]
    assert _report(run).status == "OK"


def test_report_serializes_to_json():
    report = _report(_run([_group([_mention("frames", (0, 6))])]))
    assert json.loads(json.dumps(report.as_dict()))["status"] == "OK"


# --------------------------------------------------------------------------
# golden corpus


from mention_checks import goldens  # type: ignore[import-not-found]  # noqa: E402


def _golden(form: str, **expect) -> goldens.GoldenLabel:
    return goldens.GoldenLabel(
        id=f"t-{form}", form=form, kind="occurrence", status="confirmed",
        expect=expect, evidence=(), notes="", subject="example.com", field="products",
    )


def test_a_form_absent_from_the_text_must_not_be_collected():
    """The exact half of the golden check.

    `PET` inside "competitive" and `ITAR` inside "Military" are live on
    superiortech.org; collecting either is a word-boundary defect in this
    stage, not a difference of opinion.
    """
    run = _run([_group([_mention("PET", (0, 3), snippet="PET sheet stock here")])])
    findings, metrics = goldens.check_occurrences(
        run, [_golden("PET", occurrences=0, must_not_occur=True)]
    )
    assert len(findings) == 1
    assert metrics["occurrence_labels_checked"] == 1


def test_a_form_never_sent_is_not_charged_against_this_stage():
    # Whether search sent it is the search eval's question; charging this stage
    # would import that eval's failures into this one.
    run = _run([_group([_mention("frames", (0, 6))])])
    findings, metrics = goldens.check_occurrences(run, [_golden("doors", occurrences=9)])
    assert findings == []
    assert metrics["occurrence_labels_not_sent"] == 1


def test_more_occurrences_than_the_full_text_holds_is_a_finding():
    mentions = [
        _mention("frames", (i * 10, i * 10 + 6), mention_id=f"m{i}") for i in range(5)
    ]
    run = _run([_group(mentions)])
    findings, _ = goldens.check_occurrences(run, [_golden("frames", occurrences=2)])
    assert len(findings) == 1
    assert findings[0].actual == 5


def _window(zero_hit: tuple[str, ...] = ()) -> loading.Window:
    return loading.Window(
        window=0, sub_bounds="0:100", sent_forms=1 + len(zero_hit),
        forms_with_hits=1, zero_hit_forms=zero_hit, mentions=1,
        distinct_snippets=1, described=1, not_described=(), retried=(),
        unknown_answer_ids=(), discovered_casings={}, short_forms=(),
        excluded_pages=(), chunk_bounds="0:100",
    )


def test_fewer_occurrences_than_golden_is_reported_but_does_not_gate():
    """The pipeline reads a page-trimmed, 2-chunk-capped copy, so a full-text
    golden count is an upper bound, never an equality — which explains an
    under-count without excusing hiding it.

    Standing rule (2026-08-28): a run-vs-corpus difference is examined, never
    agreed in passing. So the shortfall is REPORTED and left non-gating, rather
    than being silently absorbed by the leniency that explains it.
    """
    run = _run([_group([_mention("frames", (0, 6))])])
    findings, metrics = goldens.check_occurrences(run, [_golden("frames", occurrences=9)])
    assert len(findings) == 1
    assert findings[0].gating is False
    assert (findings[0].expected, findings[0].actual) == (9, 1)
    assert metrics["occurrence_findings"] == 0
    assert metrics["occurrence_differences"] == 1


def test_a_form_sent_but_collected_zero_times_is_a_reported_difference():
    """The bucket that used to swallow this.

    A form search SENT that this stage then found nothing for is not the same
    fact as a form search never sent, and counting them together hid an
    under-collection defect behind an out-of-scope label. `zero_hit_forms` on
    the fold's windows is what separates them.
    """
    run = _run(
        [_group([_mention("frames", (0, 6))])],
        windows=[_window(zero_hit=("doors",))],
    )
    findings, metrics = goldens.check_occurrences(run, [_golden("doors", occurrences=9)])
    assert len(findings) == 1
    assert findings[0].form == "doors"
    assert findings[0].gating is False
    assert metrics["occurrence_labels_not_sent"] == 0
    assert metrics["occurrence_labels_checked"] == 1
    assert metrics["occurrence_differences"] == 1


def test_a_must_not_occur_violation_still_gates():
    """The examine-me category must not soften the one exact check."""
    run = _run(
        [_group([_mention("PET", (0, 3), snippet="PET sheet stock here")])],
        windows=[_window()],
    )
    findings, metrics = goldens.check_occurrences(
        run, [_golden("PET", occurrences=0, must_not_occur=True)]
    )
    assert [f.gating for f in findings] == [True]
    assert metrics["occurrence_findings"] == 1
    assert metrics["occurrence_differences"] == 0


def test_only_confirmed_labels_gate():
    candidate = goldens.GoldenLabel(
        id="t1", form="PET", kind="occurrence", status="candidate",
        expect={"must_not_occur": True}, evidence=(), notes="",
        subject="example.com", field="products",
    )
    run = _run([_group([_mention("PET", (0, 3), snippet="PET sheet stock")])])
    findings, _ = goldens.check_occurrences(run, [candidate])
    assert findings == []


def test_confirmed_label_requires_a_verification_provenance_row():
    raw = {
        "id": "x", "form": "frames", "kind": "occurrence", "status": "confirmed",
        "expect": {"occurrences": 1},
        "provenance": [{"action": "added", "by": "generator"}],
    }
    assert any("verified" in p for p in goldens.validate_label(raw))
    raw["provenance"].append({"action": "verified", "by": "reader"})
    assert goldens.validate_label(raw) == []


def test_validate_label_rejects_unknown_status_and_kind():
    problems = goldens.validate_label(
        {"id": "x", "form": "f", "kind": "vibes", "status": "probably",
         "expect": {"occurrences": 1}, "provenance": [{"action": "added"}]}
    )
    assert len(problems) >= 2


@pytest.mark.parametrize("path", sorted(paths.iter_golden_files()))
def test_every_seeded_golden_file_parses_and_respects_the_schema(path):
    """Guards the corpus itself: a malformed label is a silent hole in the
    oracle, and the corpus is large enough that nobody would notice by eye."""
    payload, labels = goldens.load_field_goldens(path)
    assert payload.get("subject"), f"{path} has no subject"
    assert payload.get("field") in paths.PHRASE_FIELDS, f"{path} has a bad field"
    assert "labels" in payload, f"{path} must carry a labels list, even if empty"
    snapshot = payload.get("snapshot") or {}
    assert len(str(snapshot.get("sha256", ""))) == 64, f"{path} needs a real sha256"
    seen_ids = set()
    for label in labels:
        assert label.id not in seen_ids, f"{path} repeats label id {label.id}"
        seen_ids.add(label.id)
        assert label.status in goldens.VALID_STATUSES
        assert label.kind in goldens.VALID_KINDS


def test_the_whole_golden_corpus_validates():
    """The corpus is its own oracle, so a malformed label is a silent hole in
    it. This is `checks/validate_goldens.py` as a test, minus the quote re-read
    (which is slow and belongs to the CLI)."""
    from mention_checks import validate_goldens  # type: ignore[import-not-found]

    report, errors = validate_goldens.validate(check_quotes=False)
    assert errors == [], f"{len(errors)} golden-corpus errors: {errors[:5]}"
    assert report["totals"]["subjects"] == 20
    assert report["totals"]["labels"] > 14000


def test_a_recurring_snippet_is_one_claim_per_window_not_one_per_mention_id():
    """The judging unit is the model's claim, not the passage.

    A mention_id is content-derived, so one id covers a passage wherever it
    recurs; the model describes it once per window and legitimately gives a
    different description each time. Measured on ableengineering: one snippet,
    13 occurrences, 8 pages, 6 descriptions — of which the old enumeration
    judged exactly one.
    """
    shared = "m9rec"
    a = _mention("FAA", (10, 13), mention_id=shared, window=0,
                 location="Heading on the Boeing page, in the site's own copy.",
                 page="https://x/boeing")
    b = _mention("FAA", (99, 102), mention_id=shared, window=1,
                 location="Heading on the Airbus page, in the site's own copy.",
                 page="https://x/airbus")
    run = _run([_group([a, b])])
    assert len(run.distinct_snippets()) == 1
    claims = run.location_claims()
    assert len(claims) == 2
    assert {m.location for m, _ in claims} == {a.location, b.location}


def test_a_claim_carries_every_occurrence_it_covers():
    """One window, one description, several occurrences on several pages.

    The prompt asks for exactly this ("describe it once, covering where it
    recurs"), so a judge handed one representative page would call a correct
    recurrence-covering sentence wrong.
    """
    loc = "Prose on both the Boeing and Airbus pages, in the site's own copy."
    a = _mention("FAA", (10, 13), mention_id="mcov", window=0, location=loc,
                 page="https://x/boeing")
    b = _mention("FAA", (50, 53), mention_id="mcov", window=0, location=loc,
                 page="https://x/airbus")
    run = _run([_group([a, b])])
    claims = run.location_claims()
    assert len(claims) == 1
    _rep, covered = claims[0]
    assert {m.page for m in covered} == {"https://x/boeing", "https://x/airbus"}


def test_a_banned_location_is_caught_in_any_window():
    """The ban scan is an exact gate, so it must see every claim.

    Keyed by mention_id it saw only the first, and a URL in a later window's
    description went undetected.
    """
    clean = _mention("FAA", (10, 13), mention_id="mban", window=0,
                     location="Heading on the parts page, in the site's own copy.")
    dirty = _mention("FAA", (99, 102), mention_id="mban", window=1,
                     location="Heading on https://example.com/parts, in the site's own copy.")
    report = _report(_run([_group([clean, dirty])]))
    assert "location.no_url" in report.reds


def test_products_and_contract_products_must_match_on_location_too():
    """The divergence that passed this gate on 20260829T022413."""
    def build(field, location):
        return _run(
            [_group([_mention("FAA", (10, 13), mention_id="mid", location=location)])],
            field=field, subject="lucasmilhaupt.com",
        )
    source = build(paths.SHARED_SOURCE, "Heading on the parts page, in the site's own copy.")
    duplicate = build(paths.SHARED_DUPLICATE, "(location not described)")
    reports = mechanical.evaluate_run([source, duplicate])
    assert "identity.products_equals_contract_products" in reports[source.key].reds
