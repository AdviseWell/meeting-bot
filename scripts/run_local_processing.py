#!/usr/bin/env python3
"""Run the manager's conversion + storage pipeline locally.

This script is meant for fast iteration without redeploying Kubernetes jobs.

What it does:
- Gets a WEBM input either from:
  - a local file path, OR
  - GCS (gs://bucket/path or bucket+object), OR
  - a signed URL (https://...)
- Runs the same media conversion used by the manager (WEBM -> MP4 + M4A)
- Optionally runs the offline transcription pipeline
- Optionally uploads artifacts back to GCS
- Optionally writes the markdown transcript to Firestore

It intentionally does NOT attempt to join/monitor meetings.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional


def _parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError("Expected gs://bucket/object")
    rest = gs_uri[len("gs://"):]
    if "/" not in rest:
        raise ValueError("Expected gs://bucket/object")
    bucket, obj = rest.split("/", 1)
    return bucket, obj


def _download_to_path(
    *,
    input_kind: str,
    input_value: str,
    out_path: Path,
    gcs_bucket: Optional[str],
) -> None:
    """Download/copy input into out_path."""

    if input_kind == "file":
        src = Path(input_value)
        if not src.exists():
            raise FileNotFoundError(src)
        shutil.copy2(src, out_path)
        return

    if input_kind == "url":
        import requests  # type: ignore

        with requests.get(input_value, stream=True, timeout=120) as r:
            r.raise_for_status()
            with out_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return

    if input_kind == "gs":
        # Lazy import so local-only runs don't require gcloud libs.
        from google.cloud import storage  # type: ignore

        if input_value.startswith("gs://"):
            bucket_name, obj = _parse_gs_uri(input_value)
        else:
            if not gcs_bucket:
                raise ValueError(
                    "For non-gs:// inputs you must pass --gcs-bucket "
                    "(or set GCS_BUCKET)"
                )
            bucket_name, obj = gcs_bucket, input_value.lstrip("/")

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(obj)
        blob.download_to_filename(str(out_path))
        return

    raise ValueError(f"Unknown input kind: {input_kind}")


def main() -> int:
    # Ensure imports like `from media_converter import MediaConverter` work
    # when running from the repo without installing it as a package.
    repo_root = Path(__file__).resolve().parents[1]
    manager_dir = repo_root / "manager"
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    if str(manager_dir) not in sys.path:
        sys.path.insert(0, str(manager_dir))

    parser = argparse.ArgumentParser(
        description="Run meeting-bot conversion/transcription pipeline locally"
    )
    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Input WEBM source. Supported forms: "
            "(1) /path/to/file.webm "
            "(2) gs://bucket/path/to/recording.webm "
            "(3) GCS object path (requires --gcs-bucket) "
            "(4) https://signed-url"
        ),
    )
    parser.add_argument(
        "--meeting-id",
        required=True,
        help=(
            "Meeting doc id (used for transcript filenames / firestore writes)"
        ),
    )
    parser.add_argument(
        "--gcs-bucket",
        default=os.environ.get("GCS_BUCKET"),
        help="Default GCS bucket name (optional if --input is gs://...)",
    )
    parser.add_argument(
        "--gcs-dest-prefix",
        default=None,
        help=(
            "Destination prefix for uploads (e.g. recordings/sessions/<id>). "
            "If omitted, uploads are skipped unless --write-uploads is false."
        ),
    )
    parser.add_argument(
        "--firestore-database",
        default=os.environ.get("FIRESTORE_DATABASE", "(default)"),
    )
    parser.add_argument(
        "--firestore-org",
        default=os.environ.get("ORG_ID", "advisewell"),
        help="Firestore org id (default: advisewell)",
    )

    parser.add_argument(
        "--transcription-mode",
        choices=["offline", "none"],
        default=os.environ.get("TRANSCRIPTION_MODE", "offline")
        .strip()
        .lower(),
    )
    parser.add_argument(
        "--offline-language",
        default=os.environ.get("OFFLINE_TRANSCRIPTION_LANGUAGE", "en").strip(),
    )
    parser.add_argument(
        "--offline-max-speakers",
        type=int,
        default=int(os.environ.get("OFFLINE_MAX_SPEAKERS", "6")),
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Run conversion/transcription but skip uploads "
            "and firestore writes"
        ),
    )
    parser.add_argument(
        "--write-uploads",
        action="store_true",
        help=(
            "Upload artifacts to GCS "
            "(requires --gcs-dest-prefix and bucket creds)"
        ),
    )
    parser.add_argument(
        "--write-firestore",
        action="store_true",
        help="Write transcript markdown to Firestore (requires creds)",
    )

    args = parser.parse_args()

    input_value: str = args.input
    if input_value.startswith("http://") or input_value.startswith("https://"):
        input_kind = "url"
    elif input_value.startswith("gs://"):
        input_kind = "gs"
    else:
        input_kind = "file"

    # Workspace for heavy ffmpeg outputs.
    scratch_root = Path(os.environ.get("SCRATCH_DIR", "/tmp")).resolve()
    work_dir = scratch_root / "local-processing" / args.meeting_id
    work_dir.mkdir(parents=True, exist_ok=True)

    webm_path = work_dir / "recording.webm"
    _download_to_path(
        input_kind=input_kind,
        input_value=input_value,
        out_path=webm_path,
        gcs_bucket=args.gcs_bucket,
    )

    if not webm_path.exists() or webm_path.stat().st_size < 1000:
        raise RuntimeError(
            f"Downloaded WEBM is missing/too small: {webm_path}"
        )

    # Convert media (same converter as manager).
    from media_converter import MediaConverter  # type: ignore

    converter = MediaConverter()
    mp4_path_str, m4a_path_str = converter.convert(str(webm_path))

    mp4_path = Path(mp4_path_str) if mp4_path_str else None
    m4a_path = Path(m4a_path_str) if m4a_path_str else None

    transcript_md_path: Optional[Path] = None

    if args.transcription_mode == "offline":
        from offline_pipeline import (  # type: ignore
            transcribe_and_diarize_local_media,
        )

        local_input = (
            m4a_path if (m4a_path and m4a_path.exists()) else webm_path
        )
        out_dir = Path(tempfile.gettempdir())

        def _run(diarize: bool):
            return transcribe_and_diarize_local_media(
                input_path=Path(local_input),
                out_dir=out_dir,
                meeting_id=args.meeting_id,
                language=args.offline_language,
                diarize=diarize,
                max_speakers=args.offline_max_speakers,
            )

        try:
            txt_path, json_path = _run(diarize=True)
        except Exception:
            txt_path, json_path = _run(diarize=False)

        transcript_md_path = Path(str(txt_path)).with_suffix(".md")

    if args.dry_run:
        print("DRY RUN: skipping uploads and firestore writes")
        print(f"WEBM: {webm_path}")
        print(f"MP4: {mp4_path}")
        print(f"M4A: {m4a_path}")
        print(f"MD:  {transcript_md_path}")
        return 0

    if args.write_uploads:
        if not args.gcs_bucket:
            raise ValueError(
                "--write-uploads requires --gcs-bucket (or GCS_BUCKET)"
            )
        if not args.gcs_dest_prefix:
            raise ValueError("--write-uploads requires --gcs-dest-prefix")

        from storage_client import StorageClient  # type: ignore

        storage_client = StorageClient(args.gcs_bucket)

        dest_prefix = args.gcs_dest_prefix.rstrip("/")
        storage_client.upload_file(
            str(webm_path),
            f"{dest_prefix}/recording.webm",
        )
        if mp4_path and mp4_path.exists():
            storage_client.upload_file(
                str(mp4_path),
                f"{dest_prefix}/recording.mp4",
                content_type="video/mp4",
            )
        if m4a_path and m4a_path.exists():
            storage_client.upload_file(
                str(m4a_path),
                f"{dest_prefix}/recording.m4a",
                content_type="audio/mp4",
            )

    if (
        args.write_firestore
        and transcript_md_path
        and transcript_md_path.exists()
    ):
        # FirestoreClient currently has hardcoded org in document path; avoid
        # using it here so we can write to any org.
        from google.cloud import firestore  # type: ignore
        from firestore_persistence import (  # type: ignore
            persist_transcript_to_firestore,
        )

        class _FsWriter:
            def __init__(self, database: str, org_id: str):
                self.client = firestore.Client(database=database)
                self.org_id = org_id

            def set_transcription(
                self,
                meeting_id: str,
                transcription_text: str,
            ) -> bool:
                doc_ref = self.client.document(
                    f"organizations/{self.org_id}/meetings/{meeting_id}"
                )
                doc_ref.set({"transcription": transcription_text}, merge=True)
                return True

        md_text = transcript_md_path.read_text(encoding="utf-8")
        persist_transcript_to_firestore(
            firestore_client=_FsWriter(
                args.firestore_database,
                args.firestore_org,
            ),
            meeting_id=args.meeting_id,
            markdown_path=str(transcript_md_path),
            logger=None,
        )
        print(
            "Wrote transcription to Firestore "
            f"org={args.firestore_org} meeting={args.meeting_id} "
            f"({len(md_text)} chars)"
        )

    print("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
