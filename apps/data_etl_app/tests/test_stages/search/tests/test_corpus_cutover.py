"""The markdown-cutover machinery: corpus selection, porting, repair.

These cover the three scripts that carried the eval set from the legacy
innerText scrape onto the markdown_v2 one on 2026-08-29, and the two bugs the
cutover exposed in `build_expectations.py`. Every case here is a failure that
actually happened or was one edit away from happening.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import yaml

CHECKS = Path(__file__).resolve().parents[1] / "checks"
sys.path.insert(0, str(CHECKS))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import paths as eval_paths  # type: ignore[import-not-found]  # noqa: E402

TEXT = (
    "##################################################\n"
    "https://widget.co/capabilities\n"
    "\n"
    "## Capabilities\n"
    "\n"
    "We run vacuum brazing in two furnaces.\n"
    "Our partner holds AS9100D for the Houston site.\n"
)


def _entry(eid: str, name: str, quote: str, forms: list[str], status="confirmed"):
    return {
        "id": eid,
        "name": name,
        "status": status,
        "acceptable_forms": forms,
        "evidence": [{"quote": quote, "approx_offset": 0}],
        "provenance": [{"action": "added", "by": "seed", "date": "2026-08-27"}],
    }


@pytest.fixture
def corpus(tmp_path, monkeypatch):
    """A legacy set of one subject, plus a markdown text to port it onto."""
    import port_corpus as pc  # type: ignore[import-not-found]

    legacy = tmp_path / "expectations"
    markdown_out = tmp_path / "expectations_markdown"
    md_texts = tmp_path / "sample_scraped_markdowns"
    legacy_texts = tmp_path / "sample_scraped_texts"
    for directory in (legacy / "widget_co", md_texts, legacy_texts):
        directory.mkdir(parents=True, exist_ok=True)

    (md_texts / "widget.co.txt").write_text(TEXT)
    (legacy_texts / "widget.co.txt").write_text(TEXT)
    (legacy / "widget_co" / "subject.yaml").write_text(
        yaml.safe_dump({"subject": "widget.co", "slug": "widget_co", "role": "full_unit"})
    )

    monkeypatch.setattr(pc, "LEGACY_EXPECTATIONS_DIR", legacy)
    monkeypatch.setattr(pc, "MARKDOWN_EXPECTATIONS_DIR", markdown_out)
    monkeypatch.setattr(pc, "MARKDOWN_TEXTS_DIR", md_texts)
    monkeypatch.setattr(pc, "LEGACY_TEXTS_DIR", legacy_texts)
    return pc, legacy, markdown_out


# --------------------------------------------------------- corpus selection

def test_the_markdown_corpus_is_the_default_and_legacy_is_still_reachable():
    """Both eval sets are kept: the historical runs in metrics.jsonl were all
    scored against the legacy one, so it has to stay addressable."""
    assert eval_paths.CORPUS == "markdown"
    assert eval_paths.EXPECTATIONS_DIR.name == "expectations_markdown"
    assert eval_paths.SAMPLE_TEXTS_DIR.name == "sample_scraped_markdowns"
    assert set(eval_paths.CORPORA) == {"markdown", "legacy"}
    assert eval_paths.CORPORA["legacy"] == ("sample_scraped_texts", "expectations")


# ------------------------------------------------------------------- porting

def test_an_entry_whose_quote_survives_is_ported_and_one_that_does_not_is_held_out(corpus):
    """The whole point of the port: carry what still matches, and hand what
    does not to an agent rather than guessing at it."""
    pc, legacy, out = corpus
    (legacy / "widget_co" / "process_caps.yaml").write_text(yaml.safe_dump({
        "subject": "widget.co", "field": "process_caps", "entries": [
            _entry("widget_co-process_caps-0001", "vacuum brazing",
                   "We run vacuum brazing in two furnaces.", ["vacuum brazing"]),
            _entry("widget_co-process_caps-0002", "anodizing",
                   "We anodize every housing in-house.", ["anodizing", "anodize"]),
        ],
    }))
    report = pc.port_subject("widget_co", today="2026-08-29", dry_run=False)

    assert report["kept"] == 1
    assert report["repair"] == 1
    held = [row["id"] for row in report["repair_rows"]]
    assert held == ["widget_co-process_caps-0002"]
    assert report["repair_rows"][0]["reason"] == "no evidence quote survives"

    written = yaml.safe_load((out / "widget_co" / "process_caps.yaml").read_text())
    assert [e["id"] for e in written["entries"]] == ["widget_co-process_caps-0001"]
    assert written["snapshot"]["file"].endswith("sample_scraped_markdowns/widget.co.txt")
    assert written["entries"][0]["provenance"][-1] == {
        "action": "ported", "by": "port_corpus.py",
        "date": "2026-08-29", "source": "markdown-cutover",
    }


def test_an_entry_whose_surviving_quote_covers_no_form_is_held_out_too(corpus):
    """Validator check 4 errors on this, so porting it would write a
    known-broken row. The quote survives; the form no longer sits inside it."""
    pc, legacy, out = corpus
    (legacy / "widget_co" / "process_caps.yaml").write_text(yaml.safe_dump({
        "subject": "widget.co", "field": "process_caps", "entries": [
            _entry("widget_co-process_caps-0003", "honing",
                   "We run vacuum brazing in two furnaces.", ["honing"]),
        ],
    }))
    report = pc.port_subject("widget_co", today="2026-08-29", dry_run=False)
    assert report["kept"] == 0
    assert report["repair_rows"][0]["reason"] == "surviving quotes cover no acceptable_form"


def test_a_retired_entry_is_carried_over_untouched(corpus):
    """"Retire, never delete" — and the validator exempts retired rows from the
    quote contract precisely so a judgment about vanished text can survive."""
    pc, legacy, out = corpus
    (legacy / "widget_co" / "process_caps.yaml").write_text(yaml.safe_dump({
        "subject": "widget.co", "field": "process_caps", "entries": [
            _entry("widget_co-process_caps-0004", "gone",
                   "text that no longer exists anywhere", ["gone"], status="retired"),
        ],
    }))
    report = pc.port_subject("widget_co", today="2026-08-29", dry_run=False)
    assert report["retired_carried"] == 1
    assert report["repair"] == 0
    written = yaml.safe_load((out / "widget_co" / "process_caps.yaml").read_text())
    assert written["entries"][0]["evidence"][0]["quote"] == "text that no longer exists anywhere"


def test_a_subject_missing_from_the_new_crawl_is_reported_not_written(corpus):
    """austinelectricservices.com and sterlingmfg.net are dead domains."""
    pc, legacy, out = corpus
    (legacy / "gone_co").mkdir()
    (legacy / "gone_co" / "subject.yaml").write_text(
        yaml.safe_dump({"subject": "gone.co", "slug": "gone_co"})
    )
    report = pc.port_subject("gone_co", today="2026-08-29", dry_run=False)
    assert report["status"].startswith("retired: no markdown text")
    assert not (out / "gone_co").exists()


# ------------------------------------------------- build_expectations --merge

def _merge_fixture(tmp_path, monkeypatch, existing_doc, jsonl_rows):
    import build_expectations as be  # type: ignore[import-not-found]

    texts = tmp_path / be.SAMPLE_TEXTS_REL
    texts.mkdir(parents=True, exist_ok=True)
    text_file = texts / "widget.co.txt"
    text_file.write_text(TEXT)
    expectations = tmp_path / "expectations_markdown"
    (expectations / "widget_co").mkdir(parents=True)
    (expectations / "widget_co" / "process_caps.yaml").write_text(
        yaml.safe_dump(existing_doc)
    )
    for other in ("industries", "material_caps", "equipments",
                  "conformity_attestations", "products"):
        (expectations / "widget_co" / f"{other}.yaml").write_text(
            yaml.safe_dump({"subject": "widget.co", "field": other, "entries": []})
        )
    monkeypatch.setattr(be, "EXPECTATIONS_DIR", expectations)
    monkeypatch.setattr(be, "text_path_for", lambda slug: text_file)
    jsonl = tmp_path / "topup.jsonl"
    jsonl.write_text("\n".join(json.dumps(r) for r in jsonl_rows))
    return be, expectations, jsonl


def test_merge_preserves_expected_empty_notes_and_false_friends(tmp_path, monkeypatch):
    """A top-up JSONL says nothing about these, so they must survive it.

    Before this was fixed, merging a top-up pass silently reset expected_empty
    to false, dropped the field notes, and deleted every false friend the
    subject had — all prior judgment, erased by rows that never mentioned it.
    """
    existing = {
        "subject": "widget.co", "field": "process_caps",
        "eval_set_version": 3,
        "expected_empty": True,
        "notes": "a note that cost someone an afternoon",
        "entries": [],
        "false_friends": [{"form": "Lead", "reason": "only in lead time"}],
    }
    be, expectations, jsonl = _merge_fixture(tmp_path, monkeypatch, existing, [
        {"field": "process_caps", "name": "vacuum brazing",
         "acceptable_forms": ["vacuum brazing"],
         "evidence": [{"quote": "We run vacuum brazing in two furnaces."}]},
    ])
    be.build("widget_co", jsonl, True)

    after = yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )
    assert after["expected_empty"] is True
    assert after["notes"] == "a note that cost someone an afternoon"
    assert after["false_friends"] == [{"form": "Lead", "reason": "only in lead time"}]
    assert after["eval_set_version"] == 4          # bumped, as the schema asks
    assert [e["name"] for e in after["entries"]] == ["vacuum brazing"]


def test_merge_does_not_duplicate_a_false_friend_the_file_already_had(tmp_path, monkeypatch):
    be, expectations, jsonl = _merge_fixture(tmp_path, monkeypatch, {
        "subject": "widget.co", "field": "process_caps", "entries": [],
        "false_friends": [{"form": "Lead", "reason": "only in lead time"}],
    }, [
        {"type": "false_friend", "field": "process_caps", "form": "Lead",
         "reason": "re-found by a top-up agent"},
    ])
    be.build("widget_co", jsonl, True)
    after = yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )
    assert len(after["false_friends"]) == 1


# ------------------------------------------------------------ apply_repairs

@pytest.fixture
def repair(tmp_path, monkeypatch):
    import apply_repairs as ar  # type: ignore[import-not-found]

    expectations = tmp_path / "expectations_markdown"
    (expectations / "widget_co").mkdir(parents=True)
    texts = tmp_path / "apps/data_etl_app/tests/test_stages/sample_scraped_markdowns"
    texts.mkdir(parents=True)
    (texts / "widget.co.txt").write_text(TEXT)
    (expectations / "widget_co" / "process_caps.yaml").write_text(yaml.safe_dump({
        "subject": "widget.co", "field": "process_caps",
        "snapshot": {
            "file": "apps/data_etl_app/tests/test_stages/sample_scraped_markdowns/widget.co.txt",
            "sha256": "unused-here",
        },
        "eval_set_version": 1, "entries": [],
    }))
    monkeypatch.setattr(ar, "EXPECTATIONS_DIR", expectations)
    monkeypatch.setattr(ar, "REPO_ROOT", tmp_path)

    worklist = tmp_path / "repair.jsonl"
    worklist.write_text(json.dumps({
        "slug": "widget_co", "subject": "widget.co", "field": "process_caps",
        "id": "widget_co-process_caps-0002", "name": "anodizing",
        "acceptable_forms": ["anodizing"],
        "legacy_evidence": ["We anodize every housing in-house."],
        "reason": "no evidence quote survives",
    }))
    return ar, expectations, worklist, tmp_path


def test_a_reanchored_entry_enters_as_candidate_not_confirmed(repair):
    """A repair agent's word is a seed's word: only the verification pass
    promotes to confirmed."""
    ar, expectations, worklist, tmp_path = repair
    verdicts = tmp_path / "v.jsonl"
    verdicts.write_text(json.dumps({
        "id": "widget_co-process_caps-0002", "field": "process_caps",
        "verdict": "reanchor", "acceptable_forms": ["vacuum brazing"],
        "evidence": [{"quote": "We run vacuum brazing in two furnaces."}],
    }))
    assert ar.apply_repairs("widget_co", worklist, verdicts,
                            today="2026-08-29", dry_run=False) == 0
    after = yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )
    assert after["entries"][0]["status"] == "candidate"


def test_a_quote_that_is_not_in_the_new_text_is_refused(repair):
    """Caught live: an agent copied `ated & shown keen interest...` out of the
    middle of the word *appreciated*. A true substring, and still no match —
    the matcher anchors on a left word boundary."""
    ar, expectations, worklist, tmp_path = repair
    verdicts = tmp_path / "v.jsonl"
    verdicts.write_text(json.dumps({
        "id": "widget_co-process_caps-0002", "field": "process_caps",
        "verdict": "reanchor", "acceptable_forms": ["brazing"],
        "evidence": [{"quote": "cuum brazing in two furnaces."}],
    }))
    assert ar.apply_repairs("widget_co", worklist, verdicts,
                            today="2026-08-29", dry_run=False) == 1
    after = yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )
    assert after["entries"] == []


def test_a_form_must_match_the_quote_as_a_whole_word(repair):
    """Caught live on agstech: `Casting` re-anchored onto `# Metal and Metal
    Alloy Castings`. flexible_pattern bounds both edges, so it never matched
    and the entity was lost."""
    ar, expectations, worklist, tmp_path = repair
    verdicts = tmp_path / "v.jsonl"
    verdicts.write_text(json.dumps({
        "id": "widget_co-process_caps-0002", "field": "process_caps",
        "verdict": "reanchor", "acceptable_forms": ["furnace"],
        "evidence": [{"quote": "We run vacuum brazing in two furnaces."}],
    }))
    assert ar.apply_repairs("widget_co", worklist, verdicts,
                            today="2026-08-29", dry_run=False) == 1
    after = yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )
    assert after["entries"] == []


def test_applying_the_same_verdicts_twice_does_not_duplicate_the_entry(repair):
    """A repair pass is routinely re-run after an agent fixes the rows the
    script complained about. Without the id guard the retry appends a duplicate
    id, which the loader reports as a schema error — the retry would break the
    file it was fixing."""
    ar, expectations, worklist, tmp_path = repair
    verdicts = tmp_path / "v.jsonl"
    verdicts.write_text(json.dumps({
        "id": "widget_co-process_caps-0002", "field": "process_caps",
        "verdict": "reanchor", "acceptable_forms": ["vacuum brazing"],
        "evidence": [{"quote": "We run vacuum brazing in two furnaces."}],
    }))
    ar.apply_repairs("widget_co", worklist, verdicts, today="2026-08-29", dry_run=False)
    ar.apply_repairs("widget_co", worklist, verdicts, today="2026-08-29", dry_run=False)
    after = yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )
    assert len(after["entries"]) == 1


def test_a_retired_repair_keeps_its_legacy_evidence_as_the_record(repair):
    ar, expectations, worklist, tmp_path = repair
    verdicts = tmp_path / "v.jsonl"
    verdicts.write_text(json.dumps({
        "id": "widget_co-process_caps-0002", "field": "process_caps",
        "verdict": "retire", "reason": "the anodizing page is gone from the crawl",
    }))
    ar.apply_repairs("widget_co", worklist, verdicts, today="2026-08-29", dry_run=False)
    entry = yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )["entries"][0]
    assert entry["status"] == "retired"
    assert entry["evidence"][0]["quote"] == "We anodize every housing in-house."
    assert "anodizing page is gone" in entry["notes"]


def test_a_merged_entry_never_claims_an_id_an_existing_entry_already_holds(
    tmp_path, monkeypatch
):
    """Ids used to be numbered by POSITION in the list. `apply_repairs` inserts
    repaired entries in document order, so an entry carrying id -0002 can sit
    at position 3 — and the next new entry landing at position 2 would claim
    -0002 as well. Hit agstech and blackadvtech for real on 2026-08-29; the
    loader reports it as a schema error, so the merge broke the file."""
    existing = {
        "subject": "widget.co", "field": "process_caps", "entries": [
            _entry("widget_co-process_caps-0002", "brazing",
                   "We run vacuum brazing in two furnaces.", ["brazing"]),
        ],
    }
    be, expectations, jsonl = _merge_fixture(tmp_path, monkeypatch, existing, [
        {"field": "process_caps", "name": "capabilities",
         "acceptable_forms": ["Capabilities"],
         "evidence": [{"quote": "## Capabilities"}]},
    ])
    be.build("widget_co", jsonl, True)
    ids = [e["id"] for e in yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )["entries"]]
    assert len(ids) == len(set(ids)), ids
    assert "widget_co-process_caps-0003" in ids


def test_an_entry_whose_quote_is_absent_is_left_out_rather_than_written(
    tmp_path, monkeypatch
):
    """`build_expectations` used to complain about an unfindable quote and
    write the row anyway, so a top-up pass put 24 rows into the eval set that
    the validator then errored on."""
    be, expectations, jsonl = _merge_fixture(tmp_path, monkeypatch, {
        "subject": "widget.co", "field": "process_caps", "entries": [],
    }, [
        {"field": "process_caps", "name": "anodizing",
         "acceptable_forms": ["anodizing"],
         "evidence": [{"quote": "We anodize every housing in-house."}]},
        {"field": "process_caps", "name": "vacuum brazing",
         "acceptable_forms": ["vacuum brazing"],
         "evidence": [{"quote": "We run vacuum brazing in two furnaces."}]},
    ])
    be.build("widget_co", jsonl, True)
    names = [e["name"] for e in yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )["entries"]]
    assert names == ["vacuum brazing"]


def test_a_quote_cut_mid_word_at_the_end_is_left_out(tmp_path, monkeypatch):
    """The dominant agent failure of the cutover: windowing to a fixed width
    cuts the trailing word, and flexible_pattern bounds the RIGHT edge too.
    Three separate agents lost 20, 16 and 122 entries to exactly this."""
    be, expectations, jsonl = _merge_fixture(tmp_path, monkeypatch, {
        "subject": "widget.co", "field": "process_caps", "entries": [],
    }, [
        {"field": "process_caps", "name": "vacuum brazing",
         "acceptable_forms": ["vacuum brazing"],
         "evidence": [{"quote": "We run vacuum brazing in two furn"}]},
    ])
    be.build("widget_co", jsonl, True)
    assert yaml.safe_load(
        (expectations / "widget_co" / "process_caps.yaml").read_text()
    )["entries"] == []
