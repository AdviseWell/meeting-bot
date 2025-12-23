from __future__ import annotations

from pathlib import Path

from firestore_persistence import persist_transcript_to_firestore


class _FakeFirestore:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def set_transcription(
        self,
        meeting_id: str,
        transcription_text: str,
    ) -> bool:
        self.calls.append((meeting_id, transcription_text))
        return True


def test_persist_transcript_to_firestore_reads_md(tmp_path: Path) -> None:
    md = tmp_path / "t.md"
    md.write_text("**Speaker A:** hello\n", encoding="utf-8")

    fake = _FakeFirestore()
    persist_transcript_to_firestore(
        firestore_client=fake,
        meeting_id="m1",
        markdown_path=str(md),
    )

    assert fake.calls
    mid, txt = fake.calls[0]
    assert mid == "m1"
    assert "Speaker A" in txt


def test_persist_transcript_to_firestore_skips_empty(tmp_path: Path) -> None:
    md = tmp_path / "t.md"
    md.write_text("\n", encoding="utf-8")

    fake = _FakeFirestore()
    persist_transcript_to_firestore(
        firestore_client=fake,
        meeting_id="m1",
        markdown_path=str(md),
    )

    assert fake.calls == []


def test_persist_transcript_to_firestore_skips_missing() -> None:
    fake = _FakeFirestore()
    persist_transcript_to_firestore(
        firestore_client=fake,
        meeting_id="m1",
        markdown_path="/does/not/exist.md",
    )

    assert fake.calls == []


def test_persist_transcript_to_firestore_requires_meeting_id(
    tmp_path: Path,
) -> None:
    md = tmp_path / "t.md"
    md.write_text("**Speaker A:** hello\n", encoding="utf-8")

    fake = _FakeFirestore()
    persist_transcript_to_firestore(
        firestore_client=fake,
        meeting_id="",
        markdown_path=str(md),
    )

    assert fake.calls == []
