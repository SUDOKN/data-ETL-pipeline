"""`assemble_prompts check` must fail on a catalog prompt that was re-rendered
but never published.

Until 2026-08-23 only STATIC prompts were compared against the digest they were
published under; a catalog prompt was compared only against its own rendered
file on disk. So `render` alone made `check` pass while the pipeline went on
reading the superseded S3 object — the same failure that bit us on 2026-08-11,
still open on the other half of the prompts.
"""

import argparse
from types import SimpleNamespace

import pytest

from core.models.rule_catalog import PublishedRecord
from data_etl_app.scripts import assemble_prompts

_NAME = "industry_phrase_relationship_screening"
_KEY = "multi_stage/4_phrase_relationship_screening/industry_phrase_relationship_screening.txt"
_TEXT = "rendered prompt text\n"


@pytest.fixture
def rendered_on_disk(tmp_path, monkeypatch):
    """A catalog prompt whose rendered file on disk IS what the catalog renders,
    so the only thing `check` can still catch is the publish state."""
    (tmp_path / _KEY).parent.mkdir(parents=True)
    (tmp_path / _KEY).write_text(_TEXT, encoding="utf-8")
    monkeypatch.setattr(assemble_prompts, "ASSEMBLED_PROMPTS_DIR", tmp_path)
    monkeypatch.setattr(assemble_prompts, "_check_static", lambda only: [])
    monkeypatch.setattr(assemble_prompts, "_render_all", lambda only: {_NAME: (_KEY, _TEXT)})
    monkeypatch.setattr(assemble_prompts, "_reject_unknown_only", lambda only: None)
    return tmp_path


def _with_published(monkeypatch, published: PublishedRecord) -> None:
    monkeypatch.setattr(
        assemble_prompts,
        "load_all_catalogs",
        lambda: {_NAME: SimpleNamespace(published=published)},
    )


def _check() -> int:
    return assemble_prompts.cmd_check(argparse.Namespace(only=None))


def test_check_fails_when_the_catalog_render_is_not_what_is_published(
    rendered_on_disk, monkeypatch, caplog
):
    _with_published(
        monkeypatch,
        PublishedRecord(s3_version_id="v-old", rendered_sha256="0" * 64),
    )
    assert _check() == 1
    assert "UNPUBLISHED" in caplog.text
    assert _KEY in caplog.text


def test_check_fails_when_the_catalog_prompt_was_never_published(
    rendered_on_disk, monkeypatch, caplog
):
    _with_published(monkeypatch, PublishedRecord())
    assert _check() == 1
    assert "UNPINNED" in caplog.text


def test_check_passes_when_the_render_is_both_on_disk_and_published(
    rendered_on_disk, monkeypatch
):
    _with_published(
        monkeypatch,
        PublishedRecord(
            s3_version_id="v-current",
            rendered_sha256=assemble_prompts.rendered_sha256(_TEXT),
        ),
    )
    assert _check() == 0


def test_a_drifted_render_and_a_stale_publish_are_reported_once_not_twice(
    rendered_on_disk, monkeypatch, caplog
):
    """Both failures are independent and both get logged, but the prompt is
    named once in the summary — the count is prompts, not findings."""
    (rendered_on_disk / _KEY).write_text("hand-edited\n", encoding="utf-8")
    _with_published(
        monkeypatch,
        PublishedRecord(s3_version_id="v-old", rendered_sha256="0" * 64),
    )
    assert _check() == 1
    assert "DRIFTED" in caplog.text and "UNPUBLISHED" in caplog.text
    assert "1 prompt(s) are not what is published" in caplog.text
