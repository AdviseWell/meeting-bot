#!/usr/bin/env python3
"""Local transcription runner.

This script lets you run the existing Gemini-based transcription logic against a
recording in GCS (gs://...) without going through the meeting-bot or controller.

It intentionally reuses `manager/transcription_client.py` and (optionally)
`manager/storage_client.py` to generate a signed URL.

Example:
  ./scripts/transcribe_gcs.py \
    --gcs-uri gs://bucket/path/to/recording.webm \
    --meeting-id teams-abc123 \
    --out-dir /tmp/transcripts

Auth:
- Run on a GCP VM with IAM permissions, or provide service account creds via
  GOOGLE_APPLICATION_CREDENTIALS.

Notes:
- `TranscriptionClient.transcribe_audio()` currently expects an HTTPS URL.
  For gs:// inputs we generate a signed URL using StorageClient.
"""

from __future__ import annotations

import argparse
import os
import sys
import logging
from pathlib import Path
import datetime as _datetime
from datetime import datetime, timezone

# Ensure we can import from ./manager when running from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
UTC = getattr(_datetime, "UTC", timezone.utc)
MANAGER_DIR = REPO_ROOT / "manager"
sys.path.insert(0, str(MANAGER_DIR))

from transcription_client import TranscriptionClient  # noqa: E402


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("transcribe_gcs")


def _default_meeting_id_from_gcs_uri(gcs_uri: str) -> str:
    # Use the parent folder or filename stem as a stable-ish id
    # gs://bucket/.../teams-XYZ/recording.webm -> teams-XYZ
    parts = gcs_uri.split("/")
    if len(parts) >= 2:
        parent = parts[-2]
        if parent:
            return parent
    filename = parts[-1] if parts else "meeting"
    return filename.replace(".webm", "").replace(".mp4", "").replace(".m4a", "")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Transcribe a GCS recording using existing manager Gemini code"
    )
    parser.add_argument(
        "--gcs-uri",
        required=True,
        help="GCS URI to the recording, e.g. gs://bucket/path/recording.webm",
    )
    parser.add_argument(
        "--meeting-id", default=None, help="Meeting id used for output filenames"
    )
    parser.add_argument(
        "--out-dir",
        default=str(Path.cwd() / "transcripts"),
        help="Directory to write transcript files",
    )

    parser.add_argument("--language", default="en-AU")
    parser.add_argument("--no-diarization", action="store_true")
    parser.add_argument("--timestamps", action="store_true")
    parser.add_argument("--no-action-items", action="store_true")

    parser.add_argument(
        "--prefer-audio-only",
        action="store_true",
        help=(
            "If set, extract audio-only (OGG/Opus) locally before transcription. "
            "This can reduce input size and sometimes avoids chunking."
        ),
    )

    parser.add_argument(
        "--gemini-project",
        default=os.getenv("GEMINI_PROJECT_ID", "aw-gemini-api-central"),
    )
    parser.add_argument(
        "--gemini-region", default=os.getenv("GEMINI_REGION", "australia-southeast1")
    )

    parser.add_argument(
        "--signed-url-minutes",
        type=int,
        default=360,
        help="Signed URL expiry in minutes",
    )
    parser.add_argument(
        "--bucket",
        default=os.getenv("GCS_BUCKET"),
        help="GCS bucket name. If omitted, inferred from gs:// URI.",
    )

    args = parser.parse_args()

    meeting_id = args.meeting_id or _default_meeting_id_from_gcs_uri(args.gcs_uri)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Parse gs://bucket/object
    if not args.gcs_uri.startswith("gs://"):
        raise SystemExit("--gcs-uri must start with gs://")

    without_scheme = args.gcs_uri[len("gs://") :]
    bucket, _, blob_path = without_scheme.partition("/")
    if not bucket or not blob_path:
        raise SystemExit("Invalid GCS URI format; expected gs://bucket/path")

    if args.bucket and args.bucket != bucket:
        logger.warning(
            f"--bucket ({args.bucket}) differs from gs:// bucket ({bucket}); using gs:// bucket"
        )

    logger.info("Initializing TranscriptionClient...")
    tc = TranscriptionClient(project_id=args.gemini_project, region=args.gemini_region)

    logger.info("Starting transcription...")
    target_uri = args.gcs_uri
    if args.prefer_audio_only:
        # This requires ffmpeg/ffprobe. If it fails, fall back to original URI.
        try:
            import tempfile
            from io import BytesIO
            from pydub import AudioSegment

            logger.info("Downloading input from GCS for audio-only extraction...")
            raw = tc._download_gcs_uri(args.gcs_uri)  # reuse existing helper
            if raw:
                audio = AudioSegment.from_file(BytesIO(raw), format="webm")
                audio = audio.set_channels(1).set_frame_rate(16000)

                tmp_dir = Path(tempfile.gettempdir())
                out_path = tmp_dir / f"{meeting_id}_audio_only.ogg"
                audio.export(out_path, format="ogg", codec="libopus", bitrate="24k")

                logger.info(f"Audio-only extracted to {out_path}")
                target_uri = str(out_path)
        except Exception as e:
            logger.warning(f"Audio-only extraction failed; continuing: {e}")

    transcript_data = tc.transcribe_audio(
        audio_uri=target_uri,
        language_code=args.language,
        enable_speaker_diarization=(not args.no_diarization),
        enable_timestamps=args.timestamps,
        enable_action_items=(not args.no_action_items),
    )

    if not transcript_data:
        raise SystemExit(
            "Transcription failed (returned None). Check logs above for details."
        )

    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")

    # Always pass absolute paths so --out-dir is honored.
    # (The transcription client may otherwise write into its own working
    # directory, e.g. /tmp.)
    txt_path = (out_dir / f"{meeting_id}_{timestamp}.txt").resolve()
    json_path = (out_dir / f"{meeting_id}_{timestamp}.json").resolve()

    logger.info(f"Saving transcript to {txt_path} and {json_path}")
    tc.save_transcript(transcript_data, str(txt_path), format="txt")
    tc.save_transcript(transcript_data, str(json_path), format="json")

    logger.info("Done.")
    logger.info(f"Words: {transcript_data.get('word_count')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
