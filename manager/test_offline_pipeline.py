"""Lightweight smoke test for the offline pipeline.

This is intentionally minimal and CPU-only.
It does NOT require diarization model weights or whisper.cpp at import-time.

Run (locally):
  python3 -m manager.test_offline_pipeline

In CI we can run it as a smoke test in an image that has the prebaked
artifacts.
"""

from __future__ import annotations

from pathlib import Path


def main() -> int:
    # Import lazily to avoid importing heavy deps unless invoked.
    from offline_pipeline import transcribe_and_diarize_local_media

    fixture = Path(__file__).resolve().parent / "testdata" / "tone.wav"
    assert fixture.exists(), f"Missing fixture: {fixture}"

    out_dir = Path("/tmp/offline_pipeline_test")
    out_dir.mkdir(parents=True, exist_ok=True)

    # This will likely fail at runtime unless whisper.cpp assets are available.
    # That's okay: this file is primarily intended to run inside the built
    # manager image as a CI smoke test.
    txt_path, json_path = transcribe_and_diarize_local_media(
        input_path=fixture,
        out_dir=out_dir,
        meeting_id="fixture",
        diarize=False,
        language="en",
    )

    assert txt_path.exists(), "Expected txt output"
    assert json_path.exists(), "Expected json output"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
