from __future__ import annotations

from pathlib import Path
from typing import Protocol


class FirestoreTranscriptionWriter(Protocol):
    def set_transcription(
        self,
        meeting_id: str,
        transcription_text: str,
    ) -> bool: ...


def persist_transcript_to_firestore(
    *,
    firestore_client: FirestoreTranscriptionWriter,
    meeting_id: str,
    markdown_path: str | None,
    logger=None,
) -> None:
    """Best-effort Firestore persistence for offline transcripts.

    This is intentionally side-effect-tolerant: missing/empty input is a no-op,
    and Firestore failures shouldn't fail the overall job.
    """

    if not meeting_id:
        if logger is not None:
            logger.warning("Skipping Firestore persistence: missing meeting_id")
        return

    if not markdown_path:
        if logger is not None:
            logger.info("Skipping Firestore persistence: missing markdown_path")
        return

    md_path = Path(markdown_path)
    if not md_path.exists():
        if logger is not None:
            logger.info(
                "Skipping Firestore persistence: markdown missing: %s",
                markdown_path,
            )
        return

    md_text = md_path.read_text(encoding="utf-8").strip()
    if not md_text:
        if logger is not None:
            logger.info(
                "Skipping Firestore persistence: markdown empty: %s",
                markdown_path,
            )
        return

    try:
        firestore_client.set_transcription(meeting_id, md_text)
    except Exception as exc:  # noqa: BLE001
        if logger is not None:
            logger.exception("Failed to persist transcript to Firestore: %s", exc)
        return
