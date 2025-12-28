import re

from offline_pipeline import Segment, _segments_to_webvtt


def test_segments_to_webvtt_basic_format():
    segments = [
        Segment(start=0.0, end=1.234, text="Hello there", speaker="Speaker A"),
        Segment(start=61.0, end=62.0, text="Second cue", speaker=None),
    ]

    vtt = _segments_to_webvtt(segments)

    assert vtt.startswith("WEBVTT\n")
    assert "00:00:00.000 --> 00:00:01.234" in vtt
    assert "Speaker A: Hello there" in vtt
    assert "00:01:01.000 --> 00:01:02.000" in vtt
    assert "Second cue" in vtt

    # Ensure blank line between cues.
    assert re.search(
        r"00:00:00\.000 --> 00:00:01\.234\n.*\n\n2\n00:01:01\.000",
        vtt,
        re.S,
    )


def test_segments_to_webvtt_collapses_newlines():
    segments = [
        Segment(start=0.0, end=1.0, text="Line1\n\nLine2", speaker="S"),
    ]
    vtt = _segments_to_webvtt(segments)
    assert "S: Line1 Line2" in vtt
