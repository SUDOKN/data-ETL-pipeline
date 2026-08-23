"""2026-08-22 (proposal P5, accepted): a trailing sub-window the line-respecting
divider leaves as a sliver merges into its predecessor instead of costing a
search request and a dummy mention request of its own."""

from core.models.chunking_strat import REMAINDER_MERGE_RATIO, merge_trailing_remainder


def test_a_trailing_sliver_merges_into_its_predecessor():
    assert merge_trailing_remainder([(0, 5000), (5000, 10000), (10000, 10386)]) == [(0, 5000), (5000, 10386)]


def test_a_trailing_window_at_or_above_the_ratio_stays():
    assert merge_trailing_remainder([(0, 5000), (5000, 6000)]) == [(0, 5000), (5000, 6000)]
    edge = int(REMAINDER_MERGE_RATIO * 5000)
    assert merge_trailing_remainder([(0, 5000), (5000, 5000 + edge)]) == [(0, 5000), (5000, 5000 + edge)]
    assert merge_trailing_remainder([(0, 5000), (5000, 5000 + edge - 1)]) == [(0, 5000 + edge - 1)]


def test_one_or_no_window_is_returned_unchanged_and_only_the_tail_is_considered():
    assert merge_trailing_remainder([]) == []
    assert merge_trailing_remainder([(0, 10)]) == [(0, 10)]
    # a sliver in the middle is not the divider's remainder; it stays
    assert merge_trailing_remainder([(0, 5000), (5000, 5100), (5100, 10000)]) == [(0, 5000), (5000, 5100), (5100, 10000)]
