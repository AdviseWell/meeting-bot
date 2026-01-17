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
import json
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

        # Session-based consolidation: if set, this job is for a deduplicated
        # meeting session and we must update the session status on completion.
        self.meeting_session_id = (
            os.environ.get("MEETING_SESSION_ID")
            or os.environ.get("meeting_session_id")
            or ""
        ).strip()
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
            logger.debug("Initializing clients...")
            self._init_clients()

            logger.info(f"Processing meeting {self.meeting_id}")
            logger.info(f"Meeting URL: {self.meeting_url}")
            logger.info(f"Target GCS path: {self.gcs_path}")

            # Step 0: Wait for meeting-bot API to be ready
            logger.info("Step 0: Waiting for meeting-bot API to become ready...")
            logger.debug("PRE-MEETING CHECK: Verifying meeting-bot API availability")
            if not self.meeting_monitor.wait_for_api_ready():
                logger.error("Meeting-bot API did not become ready in time")
                logger.debug("PRE-MEETING DECISION: Aborting - API not ready")
                return False

            logger.debug("PRE-MEETING DECISION: API ready, proceeding to join meeting")

            # Step 1: Join the meeting
            logger.info("Step 1: Joining meeting...")
            logger.debug("PRE-MEETING: Sending join request to meeting-bot API")
            logger.debug("Join parameters:")
            logger.debug("  URL: %s", self.meeting_url)
            logger.debug(
                "  Metadata: %s",
                (
                    json.dumps(self.metadata, indent=2, default=str)
                    if self.metadata
                    else "None"
                ),
            )

            job_id = self.meeting_monitor.join_meeting(
                self.meeting_url,
                self.metadata,
            )
            if not job_id:
                logger.error("Failed to join meeting")
                logger.debug(
                    "PRE-MEETING DECISION: Bot did not join - could indicate meeting not started, incorrect URL, or API issue"
                )
                return False

            logger.info(f"Successfully joined meeting with job ID: {job_id}")
            logger.debug("PRE-MEETING DECISION: Bot successfully joined meeting")

            # Enhanced logging for session claim
            session_id = self.fs_meeting_id or self.meeting_id
            logger.info(
                "SESSION_CLAIMED: session_id=%s, org_id=%s, meeting_url=%s, "
                "bot_job_id=%s",
                session_id[:16] if session_id else "unknown",
                self.team_id or "unknown",
                self.meeting_url[:50] if self.meeting_url else "unknown",
                job_id,
            )

            # Step 2: Monitor the meeting (check every 10 seconds)
            logger.info("Step 2: Monitoring meeting status...")
            logger.debug("DURING MEETING: Starting monitoring loop")
            recording_path = self.meeting_monitor.monitor_until_complete(
                job_id, self.metadata, check_interval=10
            )
            if not recording_path:
                logger.error("Meeting monitoring failed or no recording generated")
                logger.debug(
                    "POST-MEETING DECISION: No recording file - meeting may have ended prematurely or recording failed"
                )
                return False

            logger.info(f"Meeting completed. Recording at: {recording_path}")
            logger.debug("POST-MEETING: Recording file created successfully")
            logger.debug("Recording path: %s", recording_path)

            # Enhanced logging for recording complete
            session_id = self.fs_meeting_id or self.meeting_id
            logger.info(
                "RECORDING_COMPLETE: session_id=%s, org_id=%s, " "recording_path=%s",
                session_id[:16] if session_id else "unknown",
                self.team_id or "unknown",
                recording_path,
            )

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
            logger.debug("POST-MEETING: Verifying recording file exists")
            if not os.path.exists(recording_path):
                logger.error(f"Recording file not found: {recording_path}")
                logger.debug(
                    "POST-MEETING DECISION: Recording file missing - cannot proceed"
                )
                return False

            webm_size = os.path.getsize(recording_path)
            logger.info(
                "WEBM file size: %s bytes (%.2f MB)",
                webm_size,
                webm_size / (1024 * 1024),
            )
            logger.debug("POST-MEETING: Validating file size")
            if webm_size < 1000:
                logger.error(
                    "❌ WEBM file too small (%s bytes) - file is " "empty or corrupted",
                    webm_size,
                )
                logger.debug("POST-MEETING DECISION: File too small, likely corrupted")
                return False

            logger.debug("POST-MEETING: Uploading to GCS path: %s", self.gcs_path)
            webm_gcs_path = f"{self.gcs_path}/recording.webm"
            webm_uploaded = self.storage_client.upload_file(
                recording_path, webm_gcs_path
            )
            if not webm_uploaded:
                logger.error("Failed to upload original WEBM file")
                logger.debug(
                    "POST-MEETING DECISION: Upload failed - storage issue or permissions"
                )
                return False

            logger.info(
                "✅ Original WEBM uploaded to gs://%s/%s",
                self.gcs_bucket,
                webm_gcs_path,
            )
            logger.debug("POST-MEETING: WEBM upload successful")

            # Step 3.1: Check for ad-hoc meeting and create/update if needed
            # Ad-hoc meetings occur when a user joins via Pub/Sub without
            # a pre-scheduled meeting document in Firestore.
            # Even if the meeting exists (created by frontend), it may be
            # missing the 'start' field which is required for UI display.
            logger.debug("POST-MEETING: Checking for ad-hoc meeting requirements")
            if self.fs_meeting_id and self.team_id:
                logger.debug("Fetching meeting document from Firestore...")
                meeting_data = self.firestore_client.get_meeting(
                    organization_id=self.team_id,
                    meeting_id=self.fs_meeting_id,
                )
                logger.debug(
                    "Meeting data from Firestore: %s",
                    (
                        json.dumps(meeting_data, indent=2, default=str)
                        if meeting_data
                        else "None"
                    ),
                )

                # Get recording duration to calculate meeting start time
                from media_converter import get_recording_duration_seconds

                logger.debug("Calculating recording duration...")
                duration_seconds = get_recording_duration_seconds(recording_path)

                # Fallback to MP4 if WEBM duration failed (common with streaming WEBM)
                if not duration_seconds and mp4_path and os.path.exists(mp4_path):
                    logger.info("Falling back to MP4 for duration check...")
                    duration_seconds = get_recording_duration_seconds(mp4_path)

                logger.debug("Recording duration: %s seconds", duration_seconds)

                if not meeting_data:
                    # Meeting doesn't exist - create it
                    logger.info("Meeting document not found - creating ad-hoc meeting")
                    logger.debug(
                        "POST-MEETING DECISION: Creating ad-hoc meeting document"
                    )

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
                        logger.debug("Ad-hoc meeting details:")
                        logger.debug("  organization_id: %s", self.team_id)
                        logger.debug("  user_id: %s", self.user_id)
                        logger.debug("  meeting_url: %s", self.meeting_url)
                        logger.debug("  start_at: %s", start_at)

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

            logger.debug("POST-MEETING: Starting transcription process")
            logger.debug("Transcription mode: %s", self.transcription_mode)

            if self.transcription_mode == "none":
                logger.info("Step 4: Transcription skipped (TRANSCRIPTION_MODE=none)")
                logger.debug("POST-MEETING DECISION: Skipping transcription")
            elif self.transcription_mode == "offline":
                logger.info(
                    "Step 4: Transcribing offline with whisper.cpp + " "diarization..."
                )
                logger.debug("POST-MEETING: Using offline transcription (whisper.cpp)")
                try:
                    # Prefer extracted audio when available.
                    local_input = audio_path or recording_path
                    logger.debug("Transcription input file: %s", local_input)
                    logger.debug("Transcription language: %s", self.offline_language)
                    logger.debug("Max speakers: %d", self.offline_max_speakers)

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
                    logger.debug(
                        "POST-MEETING: Transcription files generated successfully"
                    )

                    # Best-effort: persist offline transcript into Firestore.
                    # We write into the same `transcription` field used by the
                    # Gemini path.
                    logger.debug("POST-MEETING: Persisting transcript to Firestore")
                    try:
                        firestore_meeting_id = self.fs_meeting_id or self.meeting_id
                        logger.debug("Firestore meeting ID: %s", firestore_meeting_id)
                        from firestore_persistence import (
                            persist_transcript_to_firestore,
                        )

                        persist_transcript_to_firestore(
                            firestore_client=self.firestore_client,
                            meeting_id=firestore_meeting_id,
                            markdown_path=transcript_md_path,
                            logger=logger,
                        )
                        logger.debug(
                            "POST-MEETING: Transcript persisted to Firestore successfully"
                        )
                    except Exception as firestore_err:
                        logger.exception(
                            "Error storing offline transcript in " "Firestore: %s",
                            firestore_err,
                        )
                        logger.debug(
                            "POST-MEETING: Failed to persist transcript to Firestore (non-fatal)"
                        )
                except Exception as e:
                    logger.exception(
                        "Offline transcription failed (non-fatal): %s",
                        e,
                    )
                    logger.debug(
                        "POST-MEETING: Transcription failed (continuing without it)"
                    )
            else:
                logger.info(
                    "Step 4: Transcribing with Gemini " "(audio-only preferred)..."
                )
                logger.debug("POST-MEETING: Using Gemini transcription")

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
            logger.debug("POST-MEETING: Checking for transcript files to upload")
            logger.debug("  transcript_txt_path: %s", transcript_txt_path)
            logger.debug("  transcript_json_path: %s", transcript_json_path)
            logger.debug("  transcript_md_path: %s", transcript_md_path)

            transcript_txt_uploaded = False
            transcript_json_uploaded = False
            transcript_md_uploaded = False
            transcript_vtt_uploaded = False

            if transcript_txt_path and os.path.exists(transcript_txt_path):
                logger.debug("Uploading transcript.txt...")
                transcript_txt_uploaded = self.storage_client.upload_file(
                    transcript_txt_path, f"{self.gcs_path}/transcript.txt"
                )
                logger.debug("  transcript.txt uploaded: %s", transcript_txt_uploaded)

            if transcript_json_path and os.path.exists(transcript_json_path):
                logger.debug("Uploading transcript.json...")
                transcript_json_uploaded = self.storage_client.upload_file(
                    transcript_json_path, f"{self.gcs_path}/transcript.json"
                )
                logger.debug("  transcript.json uploaded: %s", transcript_json_uploaded)

            if transcript_md_path and os.path.exists(transcript_md_path):
                logger.debug("Uploading transcript.md...")
                transcript_md_uploaded = self.storage_client.upload_file(
                    transcript_md_path,
                    f"{self.gcs_path}/transcript.md",
                    content_type="text/markdown",
                )
                logger.debug("  transcript.md uploaded: %s", transcript_md_uploaded)

            # offline_pipeline writes a .vtt file next to the txt/json outputs.
            if transcript_txt_path:
                transcript_vtt_path = os.path.splitext(transcript_txt_path)[0] + ".vtt"
                if os.path.exists(transcript_vtt_path):
                    logger.debug("Uploading transcript.vtt...")
                    transcript_vtt_uploaded = self.storage_client.upload_file(
                        transcript_vtt_path,
                        f"{self.gcs_path}/transcript.vtt",
                        content_type="text/vtt",
                    )
                    logger.debug(
                        "  transcript.vtt uploaded: %s", transcript_vtt_uploaded
                    )

            logger.debug("POST-MEETING: Transcript upload summary:")
            logger.debug(
                "  TXT: %s, JSON: %s, MD: %s, VTT: %s",
                transcript_txt_uploaded,
                transcript_json_uploaded,
                transcript_md_uploaded,
                transcript_vtt_uploaded,
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
            logger.debug("=" * 80)
            logger.debug("POST-MEETING: Marking session complete")
            logger.debug("=" * 80)

            artifacts_manifest = {
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
                    f"{self.gcs_path}/transcript.md" if transcript_md_uploaded else None
                ),
                "transcript_vtt": (
                    f"{self.gcs_path}/transcript.vtt"
                    if transcript_vtt_uploaded
                    else None
                ),
            }

            logger.debug("Artifacts manifest:")
            logger.debug(json.dumps(artifacts_manifest, indent=2, default=str))
            logger.debug(
                "FANOUT: These artifacts will be copied to all session subscribers"
            )

            # Update meeting document with bot_status and artifacts (for K8s dedup fanout)
            try:
                self._mark_meeting_complete(ok=True, artifacts=artifacts_manifest)
            except Exception as meet_err:
                logger.warning("Meeting completion update failed: %s", meet_err)

            # Also update session document (for backwards compatibility)
            try:
                self._mark_session_complete(ok=True, artifacts=artifacts_manifest)
            except Exception as sess_err:
                logger.debug("Session completion update failed (ignored): %s", sess_err)

            return True

        except Exception as e:
            logger.exception(f"Error processing meeting: {e}")
            # Mark both meeting and session as failed
            try:
                self._mark_meeting_complete(ok=False, artifacts=None)
            except Exception:
                pass
            try:
                self._mark_session_complete(ok=False, artifacts=None)
            except Exception:
                pass
            return False

    def _mark_session_complete(self, *, ok: bool, artifacts: Optional[dict]) -> None:
        """Best-effort: update per-org session state when running in session mode.

        Session mode is detected by the presence of MEETING_SESSION_ID env var.
        """

        # Session mode is identified by meeting_session_id, not GCS path
        if not self.meeting_session_id:
            return

        session_id = self.meeting_session_id
        org_id = self.team_id or ""
        if not org_id:
            logger.warning(
                "SESSION_COMPLETE_SKIPPED: session_id=%s, reason=missing_org_id",
                session_id[:16] if session_id else "unknown",
            )
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
        new_status = "complete" if ok else "failed"
        payload: dict = {
            "status": new_status,
            "processed_at": now,
            "updated_at": now,
        }

        if artifacts is not None:
            payload["artifacts"] = {k: v for k, v in artifacts.items() if v}

        # Enhanced logging for session status change
        logger.info(
            "SESSION_STATUS_CHANGE: session_id=%s, org_id=%s, "
            "to_status=%s, trigger=recording_%s, artifact_count=%d",
            session_id[:16] if session_id else "unknown",
            org_id,
            new_status,
            "complete" if ok else "failed",
            len(payload.get("artifacts", {})),
        )

        ref.set(payload, merge=True)

    def _mark_meeting_complete(self, *, ok: bool, artifacts: Optional[dict]) -> None:
        """Update the meeting document with bot_status and artifacts for fanout.

        This is required for the K8s-based deduplication approach where the
        controller queries meetings by bot_status='complete' to trigger fanout.
        """
        # Need org_id and fs_meeting_id to locate the meeting document
        org_id = self.team_id or ""
        meeting_id = self.fs_meeting_id or ""

        if not org_id or not meeting_id:
            logger.debug(
                "MEETING_COMPLETE_SKIPPED: reason=missing_org_or_meeting_id, "
                "org_id=%s, meeting_id=%s",
                org_id or "missing",
                meeting_id or "missing",
            )
            return

        from google.cloud import firestore

        db = firestore.Client(database=self.firestore_database)
        meeting_ref = (
            db.collection("organizations")
            .document(str(org_id))
            .collection("meetings")
            .document(str(meeting_id))
        )

        now = datetime.now(timezone.utc)
        new_status = "complete" if ok else "failed"

        payload: dict = {
            "bot_status": new_status,
            "bot_completed_at": now,
            "updated_at": now,
        }

        # Add artifacts and recording_url for fanout
        if artifacts is not None:
            clean_artifacts = {k: v for k, v in artifacts.items() if v}
            payload["artifacts"] = clean_artifacts

            # Set recording_url from webm path if available
            webm_path = clean_artifacts.get("recording_webm")
            if webm_path:
                payload["recording_url"] = f"gs://{self.gcs_bucket}/{webm_path}"

        logger.info(
            "MEETING_STATUS_CHANGE: meeting_id=%s, org_id=%s, "
            "bot_status=%s, artifact_count=%d",
            meeting_id,
            org_id,
            new_status,
            len(payload.get("artifacts", {})),
        )

        try:
            meeting_ref.set(payload, merge=True)
            logger.debug("Meeting document updated with bot_status=%s", new_status)
        except Exception as e:
            logger.warning("Failed to update meeting document: %s", e)

    def run(self):
        """Main run - process the meeting job"""
        logger.info("=" * 50)
        logger.info("Meeting Bot Manager starting...")
        logger.info("=" * 50)
        logger.debug("ENVIRONMENT VARIABLES:")
        logger.debug("  MEETING_ID: %s", self.meeting_id)
        logger.debug("  FS_MEETING_ID: %s", self.fs_meeting_id)
        logger.debug("  MEETING_SESSION_ID: %s", self.meeting_session_id)
        logger.debug("  MEETING_URL: %s", self.meeting_url)
        logger.debug("  USER_ID: %s", self.user_id)
        logger.debug("  TEAM_ID: %s", self.team_id)
        logger.debug("  GCS_BUCKET: %s", self.gcs_bucket)
        logger.debug("  GCS_PATH: %s", self.gcs_path)
        logger.debug("  MEETING_BOT_API_URL: %s", self.meeting_bot_api)
        logger.debug("  TRANSCRIPTION_MODE: %s", self.transcription_mode)
        logger.debug("  FIRESTORE_DATABASE: %s", self.firestore_database)

        # Log session mode detection
        if self.meeting_session_id:
            logger.info(
                "SESSION_MODE_DETECTED: session_id=%s, org_id=%s - "
                "Will update session status on completion for fanout",
                self.meeting_session_id[:16] if self.meeting_session_id else "unknown",
                self.team_id or "unknown",
            )

        logger.info(f"Meeting ID: {self.meeting_id}")
        if self.fs_meeting_id:
            logger.info(f"Firestore Meeting ID: {self.fs_meeting_id}")
        logger.info(f"Meeting URL: {self.meeting_url}")
        logger.info(f"GCS Bucket: {self.gcs_bucket}")
        logger.info(f"GCS Path: {self.gcs_path}")
        logger.info(f"Meeting Bot API: {self.meeting_bot_api}")

        if self.metadata:
            logger.debug(
                "Meeting metadata: %s", json.dumps(self.metadata, indent=2, default=str)
            )

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
