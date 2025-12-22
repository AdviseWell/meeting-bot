from __future__ import annotations

from offline_pipeline import (
    Segment,
    _dedupe_repeated_segments,
    _segments_to_markdown,
)


def test_segments_to_markdown_groups_by_speaker_and_concatenates() -> None:
    segs = [
        Segment(start=0.0, end=1.0, speaker="Speaker A", text="Hello."),
        Segment(start=1.0, end=2.0, speaker="Speaker A", text="World."),
        Segment(start=2.0, end=3.0, speaker="Speaker B", text="Hi."),
        Segment(start=3.0, end=4.0, speaker="Speaker B", text="There."),
        Segment(start=4.0, end=5.0, speaker="Speaker A", text="Back."),
    ]

    md = _segments_to_markdown(segs)

    # Two A blocks because A->B->A.
    assert "**Speaker A:** Hello. World." in md
    assert "**Speaker B:** Hi. There." in md
    assert md.strip().endswith("**Speaker A:** Back.")


def test_dedupe_repeated_tail_segments_keeps_oldest() -> None:
    # Simulate the "last lines are repeated over and over" issue.
    segs = [
        Segment(start=0.0, end=1.0, speaker="Speaker A", text="Intro."),
        Segment(start=1.0, end=2.0, speaker="Speaker B", text="Body."),
        Segment(start=2.0, end=3.0, speaker="Speaker B", text="Tail."),
        Segment(start=3.0, end=4.0, speaker="Speaker B", text="Tail."),
        Segment(start=4.0, end=5.0, speaker="Speaker B", text="Tail."),
        Segment(start=5.0, end=6.0, speaker="Speaker B", text="Tail."),
    ]

    out = _dedupe_repeated_segments(segs, lookback=80)
    assert [s.text for s in out] == ["Intro.", "Body.", "Tail."]
