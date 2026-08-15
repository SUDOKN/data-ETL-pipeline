from pure_utils.dict_diff import find_diffs


def test_reports_nested_differences_with_dotted_paths():
    a = {"outer": {"inner": 1}, "top": "x"}
    b = {"outer": {"inner": 2}, "top": "x"}

    assert find_diffs(a, b) == {"outer.inner": (1, 2)}


def test_exclusion_applies_at_every_depth():
    """The exclusion was top-level-only until 2026-08-13, which made it useless
    for extraction metadata: every per-stage node carries its own ``created_at``
    stamped from one process-start timestamp, so a resume in a fresh process
    showed a diff per stage and nothing real."""
    a = {"created_at": 1, "stage": {"created_at": 1, "prompt_version_id": "v1"}}
    b = {"created_at": 2, "stage": {"created_at": 2, "prompt_version_id": "v1"}}

    assert find_diffs(a, b, exclude={"created_at"}) == {}


def test_excluding_a_key_does_not_hide_its_siblings():
    a = {"stage": {"created_at": 1, "prompt_version_id": "v1"}}
    b = {"stage": {"created_at": 2, "prompt_version_id": "v2"}}

    assert find_diffs(a, b, exclude={"created_at"}) == {
        "stage.prompt_version_id": ("v1", "v2")
    }
