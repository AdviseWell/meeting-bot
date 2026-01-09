#!/usr/bin/env python3
"""Meeting Bot Manager - Job Execution

This module is operational and includes long log lines and pipeline strings,
so we ignore line-length linting here.

# flake8: noqa: E501
# type: ignore

This manager processes a single meeting recording job:
1. Reads job details from environment variables
2. Initiates meeting join via meeting-bot API
3. Monitors meeting status
4. Uploads the original WEBM to GCS
5. Transcribes using the WEBM (optional)

Designed to run as a Kubernetes Job, spawned by the controller.
"""

import os
import sys
import logging
from typing import Dict, Optional
import tempfile
from pathlib import Path
from datetime import datetime, timezone

from meeting_monitor import MeetingMonitor
from metadata import load_meeting_metadata

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


def _scratch_root() -> str:
    """Prefer a PVC-backed scratch directory if mounted."""
    # /scratch is mounted by the controller when a RWX PVC is available.
    # Fall back to /tmp for local/dev.
    return "/scratch" if os.path.isdir("/scratch") else "/tmp"


def _should_cleanup_scratch() -> bool:
    """Whether to delete per-meeting scratch dirs after successful processing."""
    # Default on: keeps shared RWX PVC from filling up over time.
    return os.environ.get("CLEANUP_SCRATCH", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }


def _cleanup_meeting_scratch_dir(path: str) -> None:
    """Best-effort delete of the per-meeting scratch directory."""
    if not path:
        return

    abs_path = os.path.abspath(path)

    # Safety: only allow delete inside /scratch/meetings/<id>
    allowed_prefix = os.path.abspath("/scratch/meetings") + os.sep
    if not abs_path.startswith(allowed_prefix):
        logger.warning("Skipping scratch cleanup outside allowed prefix: %s", abs_path)
        return

    # Extra safety: don’t delete the root meetings dir.
    if abs_path.rstrip(os.sep) == os.path.abspath("/scratch/meetings"):
        logger.warning("Skipping scratch cleanup of root meetings dir")
        return

    try:
        import shutil

        shutil.rmtree(abs_path, ignore_errors=True)
        logger.info("Cleaned up scratch workspace: %s", abs_path)
    except Exception as e:
        logger.warning("Scratch cleanup failed (ignored): %s", e)


# Reduce noise from some verbose libraries
logging.getLogger("google.auth").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("google.cloud").setLevel(logging.INFO)


class MeetingManager:
    """Main manager for processing a single meeting recording job"""

    def __init__(self):
        # Required environment variables for the meeting job
        self.meeting_url = os.environ.get("MEETING_URL")
        self.meeting_id = os.environ.get("MEETING_ID")
        self.fs_meeting_id = os.environ.get(
            "FS_MEETING_ID"
        )  # Firestore-specific meeting ID
        self.user_id = (
            os.environ.get("USER_ID")
            or os.environ.get("user_id")
            or os.environ.get("FS_USER_ID")
            or os.environ.get("fs_user_id")
        )
        self.team_id = (
            os.environ.get("teamId")
            or os.environ.get("TEAMID")
            or os.environ.get("team_id")
            or os.environ.get("TEAM_ID")
            or ""
        )
        self.gcs_path = os.environ.get("GCS_PATH")

        # Storage layout is typically:
        #   recordings/<user_firebase_document_id>/<meeting_firebase_document_id>/...
        #
        # However, for session-dedupe runs we intentionally allow a canonical
        # shared prefix:
        #   recordings/sessions/<meeting_session_id>/...
        #
        # For backward compatibility we accept legacy values, but we normalize
        # into the canonical prefix as early as possible.
        explicit_gcs_path = (self.gcs_path or "").strip()
        is_canonical_session_path = explicit_gcs_path.startswith("recordings/sessions/")

        # USER_ID should never be blank for per-user recording jobs. Fail fast
        # here so we don't run an hours-long job only to be unable to locate
        # the recording volume directory at the end.
        if isinstance(self.user_id, str):
            self.user_id = self.user_id.strip()

        if not is_canonical_session_path and not self.user_id:
            raise ValueError(
                "Missing USER_ID for recording job. Refusing to start. "
                "Set USER_ID (or user_id/FS_USER_ID) in the manager env."
            )

        if is_canonical_session_path:
            # Respect explicit canonical session path as-is.
            self.gcs_path = explicit_gcs_path
        else:
            storage_meeting_id = self.fs_meeting_id or self.meeting_id
            if storage_meeting_id and self.user_id:
                self.gcs_path = f"recordings/{self.user_id}/{storage_meeting_id}"
            elif storage_meeting_id:
                # Backward-compatible prefix (meeting only) when user id is unknown.
                self.gcs_path = f"recordings/{storage_meeting_id}"
            elif self.gcs_path:
                # Last-resort fallback if FS_MEETING_ID/MEETING_ID are missing.
                # Keep existing behavior: accept bare ids.
                if (
                    not self.gcs_path.startswith("recordings/")
                    and "/" not in self.gcs_path
                ):
                    self.gcs_path = f"recordings/{self.gcs_path}"

        # Optional meeting metadata
        self.metadata = load_meeting_metadata(
            meeting_id=self.meeting_id,
            gcs_path=self.gcs_path,
        )

        # GCS and API configuration
        self.gcs_bucket = os.environ.get("GCS_BUCKET")
        self.firestore_database = os.environ.get(
            "FIRESTORE_DATABASE",
            "(default)",
        )
        self.meeting_bot_api = os.environ.get(
            "MEETING_BOT_API_URL", "http://localhost:3000"
        )

        # Validate required environment variables
        self._validate_config()

        # Clients and heavy deps are initialized lazily in process_meeting()
        # after config validation. This keeps startup fast and avoids import-time
        # failures in minimal environments.
        self.meeting_monitor = MeetingMonitor(self.meeting_bot_api)
        self.storage_client = None
        self.firestore_client = None

        # Transcription mode:
        # - offline (default): whisper.cpp + offline diarization
        # - gemini: use Gemini for transcription (requires cloud access)
        # - none: skip transcription entirely
        self.transcription_mode = (
            os.environ.get("TRANSCRIPTION_MODE", "offline").strip().lower()
        )
        self.transcription_client = None

        # Offline pipeline options (only used when TRANSCRIPTION_MODE=offline)
        self.offline_language = os.environ.get(
            "OFFLINE_TRANSCRIPTION_LANGUAGE", "en"
        ).strip()
        self.offline_max_speakers = int(os.environ.get("OFFLINE_MAX_SPEAKERS", "6"))

        logger.info("Transcription backend selected: %s", self.transcription_mode)

    def _init_clients(self) -> None:
        """Initialize optional clients that pull in heavier dependencies."""

        if self.storage_client and self.firestore_client:
            return

        from storage_client import StorageClient, FirestoreClient

        self.storage_client = StorageClient(self.gcs_bucket)
        self.firestore_client = FirestoreClient(self.firestore_database)

        if self.transcription_mode == "gemini" and self.transcription_client is None:
            from transcription_client import TranscriptionClient

            self.transcription_client = TranscriptionClient(
                project_id=os.environ.get(
                    "GEMINI_PROJECT_ID",
                    "aw-gemini-api-central",
                )
            )

    def _validate_config(self):
        """Validate required environment variables"""
        required_vars = {
            "MEETING_URL": self.meeting_url,
            "MEETING_ID": self.meeting_id,
            "GCS_PATH": self.gcs_path,
            "GCS_BUCKET": self.gcs_bucket,
        }

        missing = [k for k, v in required_vars.items() if not v]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    def process_meeting(self) -> bool:
        """
        Process the meeting recording job

        Returns:
            True if processing succeeded, False otherwise
        """
        try:
            # Only import and initialize heavy dependencies once we know the
            # configuration is valid.
            self._init_clients()

            logger.info(f"Processing meeting {self.meeting_id}")
            logger.info(f"Meeting URL: {self.meeting_url}")
            logger.info(f"Target GCS path: {self.gcs_path}")

            # Step 0: Wait for meeting-bot API to be ready
            logger.info("Step 0: Waiting for meeting-bot API to become ready...")
            if not self.meeting_monitor.wait_for_api_ready():
                logger.error("Meeting-bot API did not become ready in time")
                return False

            # Step 1: Join the meeting
            logger.info("Step 1: Joining meeting...")
            job_id = self.meeting_monitor.join_meeting(
                self.meeting_url,
                self.metadata,
            )
            if not job_id:
                logger.error("Failed to join meeting")
                return False

            logger.info(f"Successfully joined meeting with job ID: {job_id}")

            # Step 2: Monitor the meeting (check every 10 seconds)
            logger.info("Step 2: Monitoring meeting status...")
            recording_path = self.meeting_monitor.monitor_until_complete(
                job_id, self.metadata, check_interval=10
            )
            if not recording_path:
                logger.error("Meeting monitoring failed or no recording generated")
                return False

            logger.info(f"Meeting completed. Recording at: {recording_path}")

            # Use PVC-backed scratch space for any heavy processing. This avoids
            # GKE Autopilot ephemeral storage limits.
            scratch_root = _scratch_root()
            scratch_tmp = os.path.join(scratch_root, "tmp")
            os.makedirs(scratch_tmp, exist_ok=True)

            work_dir = os.path.join(scratch_root, "meetings", self.meeting_id)
            os.makedirs(work_dir, exist_ok=True)

            # Copy the WEBM into scratch so ffmpeg outputs (mp4/m4a) land on the
            # PVC rather than node ephemeral storage.
            work_webm = os.path.join(work_dir, "recording.webm")
            if os.path.abspath(recording_path) != os.path.abspath(work_webm):
                try:
                    logger.info("Copying recording into scratch workspace...")
                    import shutil

                    shutil.copy2(recording_path, work_webm)
                    recording_path = work_webm
                except Exception as copy_err:
                    logger.warning(
                        "Failed to copy recording into scratch; will process in-place: %s",
                        copy_err,
                    )

            # Step 2.5: Extract audio-only (preferred for transcription)
            # This reduces upload size and significantly improves chances of a
            # one-shot transcription staying within model input limits.
            audio_path = None
            mp4_path = None
            try:
                logger.info("Step 2.5: Extracting audio-only for transcription...")
                from media_converter import MediaConverter

                converter = MediaConverter()
                extracted_mp4, extracted_m4a = converter.convert(recording_path)
                if extracted_mp4 and os.path.exists(extracted_mp4):
                    mp4_path = extracted_mp4
                if extracted_m4a and os.path.exists(extracted_m4a):
                    audio_size = os.path.getsize(extracted_m4a)
                    logger.info(
                        "Extracted audio size: %s bytes (%.2f MB)",
                        audio_size,
                        audio_size / (1024 * 1024),
                    )
                    if audio_size >= 1000:
                        audio_path = extracted_m4a
                    else:
                        logger.warning(
                            "Extracted audio file is too small; "
                            "will fall back "
                            "to WEBM for transcription."
                        )
                else:
                    logger.warning(
                        "Audio extraction failed; will fall back to WEBM for "
                        "transcription."
                    )
            except Exception as audio_err:
                logger.warning(
                    "Audio extraction failed; continuing without it: %s",
                    audio_err,
                )

            # Step 3: Upload original WEBM file to GCS (required)
            logger.info("Step 3: Uploading original WEBM file to GCS...")
            if not os.path.exists(recording_path):
                logger.error(f"Recording file not found: {recording_path}")
                return False

            webm_size = os.path.getsize(recording_path)
            logger.info(
                "WEBM file size: %s bytes (%.2f MB)",
                webm_size,
                webm_size / (1024 * 1024),
            )
            if webm_size < 1000:
                logger.error(
                    "❌ WEBM file too small (%s bytes) - file is " "empty or corrupted",
                    webm_size,
                )
                return False

            webm_gcs_path = f"{self.gcs_path}/recording.webm"
            webm_uploaded = self.storage_client.upload_file(
                recording_path, webm_gcs_path
            )
            if not webm_uploaded:
                logger.error("Failed to upload original WEBM file")
                return False

            logger.info(
                "✅ Original WEBM uploaded to gs://%s/%s",
                self.gcs_bucket,
                webm_gcs_path,
            )

            # Step 3.1: Check for ad-hoc meeting and create/update if needed
            # Ad-hoc meetings occur when a user joins via Pub/Sub without
            # a pre-scheduled meeting document in Firestore.
            # Even if the meeting exists (created by frontend), it may be
            # missing the 'start' field which is required for UI display.
            if self.fs_meeting_id and self.team_id:
                meeting_data = self.firestore_client.get_meeting(
                    organization_id=self.team_id,
                    meeting_id=self.fs_meeting_id,
                )

                # Get recording duration to calculate meeting start time
                from media_converter import get_recording_duration_seconds

                duration_seconds = get_recording_duration_seconds(recording_path)

                # Fallback to MP4 if WEBM duration failed (common with streaming WEBM)
                if not duration_seconds and mp4_path and os.path.exists(mp4_path):
                    logger.info("Falling back to MP4 for duration check...")
                    duration_seconds = get_recording_duration_seconds(mp4_path)

                if not meeting_data:
                    # Meeting doesn't exist - create it
                    logger.info("Meeting document not found - creating ad-hoc meeting")

                    if duration_seconds:
                        from datetime import datetime, timezone, timedelta

                        # Calculate meeting start: current time - duration
                        now = datetime.now(timezone.utc)
                        start_time = now - timedelta(seconds=duration_seconds)
                        start_at = start_time.isoformat().replace("+00:00", "Z")

                        logger.info(
                            f"Creating ad-hoc meeting: duration={duration_seconds}s"
                            f" ({duration_seconds/60:.1f}min), start_at={start_at}"
                        )

                        # Create the ad-hoc meeting document
                        new_meeting_id = self.firestore_client.create_adhoc_meeting(
                            organization_id=self.team_id,
                            user_id=self.user_id,
                            meeting_url=self.meeting_url,
                            start_at=start_at,
                        )

                        if new_meeting_id:
                            # Update fs_meeting_id to use the new meeting
                            old_meeting_id = self.fs_meeting_id
                            self.fs_meeting_id = new_meeting_id
                            logger.info(
                                f"✅ Created ad-hoc meeting {new_meeting_id}"
                                f" (was {old_meeting_id})"
                            )

                            # Immediately update with end time and duration to meet schema requirements
                            # (create_adhoc_meeting only sets start and status=scheduled)
                            self.firestore_client.update_adhoc_meeting_times(
                                organization_id=self.team_id,
                                meeting_id=new_meeting_id,
                                start_time=start_time,
                                end_time=now,
                                duration_seconds=duration_seconds,
                            )
                        else:
                            logger.warning("Failed to create ad-hoc meeting document")
                    else:
                        logger.warning(
                            "Could not determine recording duration "
                            "- skipping ad-hoc meeting creation"
                        )
                elif meeting_data.get("source") == "ad_hoc" and not meeting_data.get(
                    "start"
                ):
                    # Ad-hoc meeting exists but is missing 'start' field
                    # This happens when the frontend creates the meeting
                    logger.info(
                        f"Ad-hoc meeting {self.fs_meeting_id} exists but missing "
                        "'start' field - updating"
                    )

                    if duration_seconds:
                        from datetime import datetime, timezone, timedelta

                        now = datetime.now(timezone.utc)
                        start_time = now - timedelta(seconds=duration_seconds)

                        # Update the meeting with the start time and end time
                        updated = self.firestore_client.update_adhoc_meeting_times(
                            organization_id=self.team_id,
                            meeting_id=self.fs_meeting_id,
                            start_time=start_time,
                            end_time=now,
                            duration_seconds=duration_seconds,
                        )

                        if updated:
                            logger.info(
                                f"✅ Updated ad-hoc meeting {self.fs_meeting_id} "
                                f"with start={start_time.isoformat()}, "
                                f"duration={duration_seconds}s"
                            )
                        else:
                            logger.warning(
                                f"Failed to update ad-hoc meeting {self.fs_meeting_id}"
                            )
                    else:
                        logger.warning(
                            "Could not determine recording duration "
                            "- skipping ad-hoc meeting update"
                        )
                else:
                    logger.debug(
                        f"Meeting {self.fs_meeting_id} already exists with "
                        f"source={meeting_data.get('source')}, "
                        f"start={meeting_data.get('start')}"
                    )

            # Step 3.25: Upload MP4 (optional) for browser fallback.
            if mp4_path and os.path.exists(mp4_path):
                logger.info("Step 3.25: Uploading MP4 fallback to GCS...")
                mp4_gcs_path = f"{self.gcs_path}/recording.mp4"
                mp4_uploaded = self.storage_client.upload_file(
                    mp4_path,
                    mp4_gcs_path,
                    content_type="video/mp4",
                )
                if mp4_uploaded:
                    logger.info(
                        "✅ MP4 uploaded to gs://%s/%s",
                        self.gcs_bucket,
                        mp4_gcs_path,
                    )
                else:
                    logger.warning("Failed to upload MP4 fallback; will ignore")

            # Step 3.5: Upload extracted audio (optional but preferred)
            audio_gcs_path = None
            if audio_path and os.path.exists(audio_path):
                logger.info("Step 3.5: Uploading extracted audio-only to GCS...")
                audio_gcs_path = f"{self.gcs_path}/recording.m4a"
                audio_uploaded = self.storage_client.upload_file(
                    audio_path,
                    audio_gcs_path,
                    content_type="audio/mp4",
                )
                if audio_uploaded:
                    logger.info(
                        "✅ Audio-only uploaded to "
                        f"gs://{self.gcs_bucket}/{audio_gcs_path}"
                    )
                else:
                    logger.warning("Failed to upload extracted audio; will ignore")
                    audio_gcs_path = None

            # Step 4: Transcribe (optional, non-fatal)
            # Prefer audio-only if available; fall back to WEBM.
            transcript_txt_path = None
            transcript_json_path = None
            transcript_md_path = None

            if self.transcription_mode == "none":
                logger.info("Step 4: Transcription skipped (TRANSCRIPTION_MODE=none)")
            elif self.transcription_mode == "offline":
                logger.info(
                    "Step 4: Transcribing offline with whisper.cpp + " "diarization..."
                )
                try:
                    # Prefer extracted audio when available.
                    local_input = audio_path or recording_path
                    out_dir = Path(tempfile.gettempdir())

                    def _run_offline(diarize: bool):
                        from offline_pipeline import transcribe_and_diarize_local_media

                        return transcribe_and_diarize_local_media(
                            input_path=Path(local_input),
                            out_dir=out_dir,
                            meeting_id=self.meeting_id,
                            language=self.offline_language,
                            diarize=diarize,
                            max_speakers=self.offline_max_speakers,
                        )

                    try:
                        txt_path, json_path = _run_offline(diarize=True)
                    except Exception as diar_err:
                        logger.warning(
                            "Diarization failed; retrying: %s",
                            diar_err,
                        )
                        txt_path, json_path = _run_offline(diarize=False)

                    # offline_pipeline also writes a markdown file next to the
                    # txt/json outputs using the same base name.
                    transcript_md_path = os.path.splitext(str(txt_path))[0] + ".md"

                    # Upload expects fixed filenames.
                    transcript_txt_path = str(txt_path)
                    transcript_json_path = str(json_path)
                    logger.info(
                        "✅ Offline transcription complete (%s, %s)",
                        transcript_txt_path,
                        transcript_json_path,
                    )

                    # Best-effort: persist offline transcript into Firestore.
                    # We write into the same `transcription` field used by the
                    # Gemini path.
                    try:
                        firestore_meeting_id = self.fs_meeting_id or self.meeting_id
                        from firestore_persistence import (
                            persist_transcript_to_firestore,
                        )

                        persist_transcript_to_firestore(
                            firestore_client=self.firestore_client,
                            meeting_id=firestore_meeting_id,
                            markdown_path=transcript_md_path,
                            logger=logger,
                        )
                    except Exception as firestore_err:
                        logger.exception(
                            "Error storing offline transcript in " "Firestore: %s",
                            firestore_err,
                        )
                except Exception as e:
                    logger.exception(
                        "Offline transcription failed (non-fatal): %s",
                        e,
                    )
            else:
                logger.info(
                    "Step 4: Transcribing with Gemini " "(audio-only preferred)..."
                )

            if self.transcription_mode == "gemini":
                try:
                    if not self.transcription_client:
                        raise RuntimeError(
                            "TRANSCRIPTION_MODE=gemini but Gemini client "
                            "was not initialized"
                        )
                    target_gcs_path = audio_gcs_path or webm_gcs_path
                    logger.info(
                        "Transcription target: gs://%s/%s",
                        self.gcs_bucket,
                        target_gcs_path,
                    )

                    # Generate signed URL for Gemini to access the file.
                    recording_url = self.storage_client.get_signed_url(
                        target_gcs_path,
                        expiration_minutes=360,
                    )

                    if recording_url:
                        logger.info(
                            "Generated signed URL for transcription: %s...",
                            recording_url[:50],
                        )
                        logger.info("Transcribing with Gemini...")

                        try:
                            transcript_data = (
                                self.transcription_client.transcribe_audio(
                                    audio_uri=recording_url,
                                    language_code="en-AU",
                                    enable_speaker_diarization=True,
                                    enable_timestamps=False,
                                    enable_action_items=True,
                                )
                            )

                            if transcript_data:
                                transcript_text = transcript_data.get("transcript", "")
                                if _is_sample_transcription(transcript_text):
                                    logger.warning(
                                        "⚠️  Transcription appears to be "
                                        "sample/demo "
                                        "text, not actual meeting content"
                                    )
                                    logger.warning(
                                        "This may indicate the WEBM contains "
                                        "no "
                                        "speech or audio capture failed"
                                    )

                                transcript_txt_path = os.path.join(
                                    tempfile.gettempdir(),
                                    f"{self.meeting_id}_transcript.txt",
                                )
                                self.transcription_client.save_transcript(
                                    transcript_data,
                                    transcript_txt_path,
                                    format="txt",
                                )

                                transcript_json_path = os.path.join(
                                    tempfile.gettempdir(),
                                    f"{self.meeting_id}_transcript.json",
                                )
                                self.transcription_client.save_transcript(
                                    transcript_data,
                                    transcript_json_path,
                                    format="json",
                                )

                                logger.info(
                                    "✅ Transcription complete! Words: %s",
                                    transcript_data.get("word_count"),
                                )

                                # Store transcription text in Firestore
                                try:
                                    transcription_text = transcript_data.get(
                                        "transcript", ""
                                    )
                                    if transcription_text:
                                        firestore_meeting_id = (
                                            self.fs_meeting_id or self.meeting_id
                                        )
                                        if firestore_meeting_id:
                                            firestore_stored = (
                                                self.firestore_client.set_transcription(
                                                    firestore_meeting_id,
                                                    transcription_text,
                                                )
                                            )
                                            if firestore_stored:
                                                logger.info(
                                                    "✅ Stored transcription " "for %s",
                                                    firestore_meeting_id,
                                                )
                                            else:
                                                logger.warning(
                                                    "Failed to store " "transcription"
                                                )
                                        else:
                                            logger.warning(
                                                "No meeting ID available for "
                                                "Firestore storage (neither "
                                                "FS_MEETING_ID nor MEETING_ID "
                                                "set)"
                                            )
                                    else:
                                        logger.warning(
                                            "No transcription text available "
                                            "to store in Firestore"
                                        )
                                except Exception as firestore_err:
                                    logger.exception(
                                        "Error storing transcription in "
                                        "Firestore: %s",
                                        firestore_err,
                                    )
                                    logger.warning(
                                        "Continuing despite Firestore storage "
                                        "failure"
                                    )
                            else:
                                logger.warning(
                                    "Transcription completed but no results " "returned"
                                )
                        finally:
                            for revoke_path in [target_gcs_path]:
                                try:
                                    self.storage_client.revoke_public_access(
                                        revoke_path
                                    )
                                except Exception as revoke_err:
                                    logger.debug(
                                        "Could not revoke public access "
                                        "(may not be public): %s",
                                        revoke_err,
                                    )
                    else:
                        logger.warning("Failed to generate signed URL for WEBM")

                except Exception as e:
                    logger.exception(f"Transcription failed (non-fatal): {e}")
                    logger.warning(
                        "Continuing with upload despite transcription failure"
                    )

            # Step 5: Upload transcripts if available
            logger.info("Step 5: Uploading transcripts to GCS (if available)...")
            transcript_txt_uploaded = False
            transcript_json_uploaded = False
            transcript_md_uploaded = False
            transcript_vtt_uploaded = False
            if transcript_txt_path and os.path.exists(transcript_txt_path):
                transcript_txt_uploaded = self.storage_client.upload_file(
                    transcript_txt_path, f"{self.gcs_path}/transcript.txt"
                )
            if transcript_json_path and os.path.exists(transcript_json_path):
                transcript_json_uploaded = self.storage_client.upload_file(
                    transcript_json_path, f"{self.gcs_path}/transcript.json"
                )
            if transcript_md_path and os.path.exists(transcript_md_path):
                transcript_md_uploaded = self.storage_client.upload_file(
                    transcript_md_path,
                    f"{self.gcs_path}/transcript.md",
                    content_type="text/markdown",
                )

            # offline_pipeline writes a .vtt file next to the txt/json outputs.
            if transcript_txt_path:
                transcript_vtt_path = os.path.splitext(transcript_txt_path)[0] + ".vtt"
                if os.path.exists(transcript_vtt_path):
                    transcript_vtt_uploaded = self.storage_client.upload_file(
                        transcript_vtt_path,
                        f"{self.gcs_path}/transcript.vtt",
                        content_type="text/vtt",
                    )

            logger.info(
                "Successfully uploaded recording to gs://%s/%s/",
                self.gcs_bucket,
                self.gcs_path,
            )
            if transcript_txt_uploaded and transcript_json_uploaded:
                logger.info("✅ Transcripts also uploaded successfully")
            if transcript_md_uploaded:
                logger.info("✅ Markdown transcript uploaded successfully")
            if transcript_vtt_uploaded:
                logger.info("✅ WebVTT subtitles uploaded successfully")

            # Cleanup local files (recording + transcripts)
            try:
                # Only delete the recording file when it lives in /scratch.
                # The source recording under /recordings is part of the shared
                # volume and we should not delete it here.
                if os.path.exists(recording_path) and os.path.abspath(
                    recording_path
                ).startswith(os.path.abspath("/scratch") + os.sep):
                    os.remove(recording_path)
                    logger.info(f"Cleaned up scratch file: {recording_path}")
            except Exception as cleanup_err:
                logger.warning(
                    f"Failed to cleanup file {recording_path}: {cleanup_err}"
                )

            if audio_path and os.path.exists(audio_path):
                try:
                    os.remove(audio_path)
                    logger.debug(f"Removed extracted audio: {audio_path}")
                except Exception as cleanup_err:
                    logger.debug(
                        "Failed to cleanup extracted audio %s: %s",
                        audio_path,
                        cleanup_err,
                    )

            if transcript_txt_path and os.path.exists(transcript_txt_path):
                os.remove(transcript_txt_path)
                logger.debug(
                    "Removed local transcript: %s",
                    transcript_txt_path,
                )
            if transcript_json_path and os.path.exists(transcript_json_path):
                os.remove(transcript_json_path)
                logger.debug(
                    "Removed local transcript: %s",
                    transcript_json_path,
                )
            if transcript_md_path and os.path.exists(transcript_md_path):
                os.remove(transcript_md_path)
                logger.debug(f"Removed local transcript: {transcript_md_path}")

            # Remove local VTT (if created).
            if transcript_txt_path:
                transcript_vtt_path = os.path.splitext(transcript_txt_path)[0] + ".vtt"
                if os.path.exists(transcript_vtt_path):
                    os.remove(transcript_vtt_path)
                    logger.debug(f"Removed local transcript: {transcript_vtt_path}")

            # Best-effort: delete the per-meeting scratch workspace.
            if _should_cleanup_scratch() and os.path.isdir("/scratch"):
                _cleanup_meeting_scratch_dir(
                    os.path.join("/scratch", "meetings", self.meeting_id)
                )

            # Session-dedupe support: mark the meeting session complete and
            # publish an artifact manifest so the controller can fan-out copies.
            try:
                self._mark_session_complete(
                    ok=True,
                    artifacts={
                        "recording_webm": webm_gcs_path,
                        "recording_mp4": (
                            f"{self.gcs_path}/recording.mp4" if mp4_path else None
                        ),
                        "recording_m4a": audio_gcs_path,
                        "transcript_txt": (
                            f"{self.gcs_path}/transcript.txt"
                            if transcript_txt_uploaded
                            else None
                        ),
                        "transcript_json": (
                            f"{self.gcs_path}/transcript.json"
                            if transcript_json_uploaded
                            else None
                        ),
                        "transcript_md": (
                            f"{self.gcs_path}/transcript.md"
                            if transcript_md_uploaded
                            else None
                        ),
                        "transcript_vtt": (
                            f"{self.gcs_path}/transcript.vtt"
                            if transcript_vtt_uploaded
                            else None
                        ),
                    },
                )
            except Exception as sess_err:
                logger.debug("Session completion update failed (ignored): %s", sess_err)

            return True

        except Exception as e:
            logger.exception(f"Error processing meeting: {e}")
            try:
                self._mark_session_complete(ok=False, artifacts=None)
            except Exception:
                pass
            return False

    def _mark_session_complete(self, *, ok: bool, artifacts: Optional[dict]) -> None:
        """Best-effort: update per-org session state when running in session mode."""

        if not (self.gcs_path or "").startswith("recordings/sessions/"):
            return

        session_id = self.fs_meeting_id or self.meeting_id
        if not session_id:
            return

        org_id = self.team_id or ""
        if not org_id:
            return

        # Use a lightweight Firestore client directly; the existing
        # FirestoreClient in storage_client.py is hard-coded to a specific org.
        from google.cloud import firestore

        db = firestore.Client(database=self.firestore_database)
        ref = (
            db.collection("organizations")
            .document(str(org_id))
            .collection("meeting_sessions")
            .document(str(session_id))
        )

        now = datetime.now(timezone.utc)
        payload: dict = {
            "status": "complete" if ok else "failed",
            "processed_at": now,
            "updated_at": now,
        }

        if artifacts is not None:
            payload["artifacts"] = {k: v for k, v in artifacts.items() if v}

        ref.set(payload, merge=True)

    def run(self):
        """Main run - process the meeting job"""
        logger.info("=" * 50)
        logger.info("Meeting Bot Manager starting...")
        logger.info("=" * 50)
        logger.info(f"Meeting ID: {self.meeting_id}")
        if self.fs_meeting_id:
            logger.info(f"Firestore Meeting ID: {self.fs_meeting_id}")
        logger.info(f"Meeting URL: {self.meeting_url}")
        logger.info(f"GCS Bucket: {self.gcs_bucket}")
        logger.info(f"GCS Path: {self.gcs_path}")
        logger.info(f"Meeting Bot API: {self.meeting_bot_api}")
        logger.info("=" * 50)

        exit_code = 0

        try:
            # Process the meeting
            success = self.process_meeting()

            if success:
                logger.info("=" * 50)
                logger.info("Processing completed successfully")
                logger.info("=" * 50)
                exit_code = 0
            else:
                logger.error("=" * 50)
                logger.error("Processing failed")
                logger.error("=" * 50)
                exit_code = 1

        finally:
            # ALWAYS trigger shutdown of meeting-bot, regardless of success or
            # failure
            logger.info("Triggering meeting-bot shutdown...")
            shutdown_success = self.meeting_monitor.shutdown()
            if shutdown_success:
                logger.info("Meeting-bot shutdown triggered successfully")
            else:
                logger.warning("Failed to trigger meeting-bot shutdown")

        return exit_code


def _is_sample_transcription(transcript_text: str) -> bool:
    """
    Check if transcription text appears to be sample/demo content

    Args:
        transcript_text: The transcription text to check

    Returns:
        True if it looks like sample text, False otherwise
    """
    if not transcript_text:
        return False

    # Common indicators of sample/demo text from the provided example
    sample_indicators = [
        "[Name Redacted]",
        "[Company Name Redacted]",
        "revolutionize how we interact with our customers",
        "360-degree view of each customer",
        "CRM system that integrates all our customer touchpoints",
        "phased rollout, starting with a pilot program in Q3",
        "sales and customer support departments",
        "customer satisfaction scores, response times, resolution rates",
        "I'm the director of operations",
        "new project that we're going to be launching",
        "Speaker 1 (*Male*):",
        "Speaker 2 (*Female*):",
    ]

    # Check if multiple sample indicators are present
    found_indicators = sum(
        1
        for indicator in sample_indicators
        if indicator.lower() in transcript_text.lower()
    )

    # Also check for generic business meeting patterns that suggest sample
    # content
    generic_patterns = [
        "thank you [name redacted]",
        "really excited about this new project",
        "director of operations",
        "customer touchpoints",
        "pilot program in q3",
    ]

    found_patterns = sum(
        1 for pattern in generic_patterns if pattern.lower() in transcript_text.lower()
    )

    # If we find 3+ indicators OR 2+ patterns, likely sample text
    return found_indicators >= 3 or found_patterns >= 2


def main():
    """Entry point"""
    manager = None
    exit_code = 1

    try:
        manager = MeetingManager()
        exit_code = manager.run()
    except Exception as e:
        logger.exception(f"Fatal error during initialization: {e}")
        exit_code = 1

        # Try to shutdown even if initialization failed partway through
        if manager and hasattr(manager, "meeting_monitor"):
            try:
                logger.info("Attempting meeting-bot shutdown after fatal error...")
                manager.meeting_monitor.shutdown()
            except Exception as shutdown_error:
                logger.error(
                    "Error during shutdown after fatal error: %s",
                    shutdown_error,
                )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
