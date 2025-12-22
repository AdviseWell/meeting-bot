#!/usr/bin/env python3
"""

# type: ignore
Meeting Bot Manager - Job Execution

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
from typing import Dict
import tempfile
from pathlib import Path

from meeting_monitor import MeetingMonitor
from storage_client import StorageClient, FirestoreClient
from transcription_client import TranscriptionClient
from meeting_utils import auto_generate_missing_fields
from media_converter import MediaConverter
from offline_pipeline import transcribe_and_diarize_local_media

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

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
        self.gcs_path = os.environ.get("GCS_PATH")

        # Optional meeting metadata
        self.metadata = self._load_metadata()

        # GCS and API configuration
        self.gcs_bucket = os.environ.get("GCS_BUCKET")
        self.firestore_database = os.environ.get("FIRESTORE_DATABASE", "(default)")
        self.meeting_bot_api = os.environ.get(
            "MEETING_BOT_API_URL", "http://localhost:3000"
        )

        # Validate required environment variables
        self._validate_config()

        # Initialize clients
        self.meeting_monitor = MeetingMonitor(self.meeting_bot_api)
        self.storage_client = StorageClient(self.gcs_bucket)
        self.firestore_client = FirestoreClient(self.firestore_database)

        # Transcription mode:
        # - offline (default): whisper.cpp + offline diarization
        # - gemini: use Gemini for transcription (requires cloud access)
        # - none: skip transcription entirely
        self.transcription_mode = (
            os.environ.get("TRANSCRIPTION_MODE", "offline").strip().lower()
        )

        # Only initialize Gemini client when explicitly requested. This avoids
        # accidental cloud usage when the intention is to run fully offline.
        self.transcription_client = None
        if self.transcription_mode == "gemini":
            self.transcription_client = TranscriptionClient(
                project_id=os.environ.get(
                    "GEMINI_PROJECT_ID", "aw-gemini-api-central"
                )
            )

        # Offline pipeline options (only used when TRANSCRIPTION_MODE=offline)
        self.offline_language = os.environ.get(
            "OFFLINE_TRANSCRIPTION_LANGUAGE", "en"
        ).strip()
        self.offline_max_speakers = int(
            os.environ.get("OFFLINE_MAX_SPEAKERS", "6")
        )

        logger.info(
            "Transcription backend selected: %s",
            self.transcription_mode,
        )

    def _load_metadata(self) -> Dict:
        """Load metadata from environment variables for meeting-bot API"""
        metadata = {}

        # Core meeting fields
        if self.meeting_id:
            metadata["meeting_id"] = self.meeting_id
        if self.gcs_path:
            metadata["gcs_path"] = self.gcs_path

        # Get optional fields from environment (support both snake_case and camelCase)
        bearer_token = (
            os.environ.get("BEARERTOKEN")
            or os.environ.get("BEARER_TOKEN")
            or os.environ.get("bearer_token")
        )
        user_id = (
            os.environ.get("USERID")
            or os.environ.get("USER_ID")
            or os.environ.get("user_id")
        )
        bot_id = (
            os.environ.get("BOTID")
            or os.environ.get("BOT_ID")
            or os.environ.get("bot_id")
        )
        event_id = (
            os.environ.get("EVENTID")
            or os.environ.get("EVENT_ID")
            or os.environ.get("event_id")
        )

        # Auto-generate missing fields if meeting URL is available
        if self.meeting_url:
            try:
                auto_generated = auto_generate_missing_fields(
                    url=self.meeting_url,
                    bearer_token=bearer_token,
                    user_id=user_id,
                    bot_id=bot_id,
                    event_id=event_id,
                )

                # Use auto-generated values
                metadata["bearerToken"] = auto_generated["bearer_token"]
                metadata["userId"] = auto_generated["user_id"]
                metadata["botId"] = auto_generated["bot_id"]

                # Also update meeting_id if it wasn't provided
                if not self.meeting_id:
                    metadata["meeting_id"] = auto_generated["meeting_id"]

                logger.info(
                    f"Auto-generated fields: userId={metadata['userId']}, botId={metadata['botId']}, bearerToken={'***' if metadata['bearerToken'] else 'NONE'}"
                )
            except Exception as e:
                logger.warning(
                    f"Could not auto-generate fields: {e}. Using fallback values."
                )
                # Fallback to original behavior
                metadata["bearerToken"] = bearer_token or ""
                metadata["userId"] = user_id or "system"
                metadata["botId"] = bot_id or event_id or self.meeting_id
        else:
            # No meeting URL, use provided values or fallbacks
            metadata["bearerToken"] = bearer_token or ""
            metadata["userId"] = user_id or "system"
            metadata["botId"] = bot_id or event_id or self.meeting_id

        # teamId (required) - fallback to meeting_id
        metadata["teamId"] = (
            os.environ.get("TEAMID")
            or os.environ.get("TEAM_ID")
            or os.environ.get("team_id")
            or self.meeting_id
        )

        # timezone (required) - fallback to UTC
        metadata["timezone"] = (
            os.environ.get("TIMEZONE") or os.environ.get("timezone") or "UTC"
        )

        # name (required) - fallback to 'Meeting Bot'
        metadata["name"] = (
            os.environ.get("NAME")
            or os.environ.get("name")
            or os.environ.get("BOT_NAME")
            or "Meeting Bot"
        )

        # Optional meeting metadata
        if os.environ.get("MEETING_TITLE"):
            metadata["meeting_title"] = os.environ.get("MEETING_TITLE")
        if os.environ.get("MEETING_ORGANIZER"):
            metadata["meeting_organizer"] = os.environ.get("MEETING_ORGANIZER")
        if os.environ.get("MEETING_START_TIME"):
            metadata["meeting_start_time"] = os.environ.get("MEETING_START_TIME")

        return metadata

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
            job_id = self.meeting_monitor.join_meeting(self.meeting_url, self.metadata)
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

            # Step 2.5: Extract audio-only (preferred for transcription)
            # This reduces upload size and significantly improves chances of a
            # one-shot transcription staying within model input limits.
            audio_path = None
            try:
                logger.info("Step 2.5: Extracting audio-only for transcription...")
                converter = MediaConverter()
                _, extracted_m4a = converter.convert(recording_path)
                if extracted_m4a and os.path.exists(extracted_m4a):
                    audio_size = os.path.getsize(extracted_m4a)
                    logger.info(
                        "Extracted audio size: "
                        f"{audio_size} bytes ({audio_size / (1024 * 1024):.2f} MB)"
                    )
                    if audio_size >= 1000:
                        audio_path = extracted_m4a
                    else:
                        logger.warning(
                            "Extracted audio file is too small; will fall back "
                            "to WEBM for transcription."
                        )
                else:
                    logger.warning(
                        "Audio extraction failed; will fall back to WEBM for "
                        "transcription."
                    )
            except Exception as audio_err:
                logger.warning(
                    f"Audio extraction failed; continuing without it: {audio_err}"
                )

            # Step 3: Upload original WEBM file to GCS (required)
            logger.info("Step 3: Uploading original WEBM file to GCS...")
            if not os.path.exists(recording_path):
                logger.error(f"Recording file not found: {recording_path}")
                return False

            webm_size = os.path.getsize(recording_path)
            logger.info(
                f"WEBM file size: {webm_size} bytes ({webm_size / (1024 * 1024):.2f} MB)"
            )
            if webm_size < 1000:
                logger.error(
                    f"❌ WEBM file too small ({webm_size} bytes) - file is empty or corrupted"
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
                f"✅ Original WEBM uploaded to gs://{self.gcs_bucket}/{webm_gcs_path}"
            )

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

            if self.transcription_mode == "none":
                logger.info("Step 4: Transcription skipped (TRANSCRIPTION_MODE=none)")
            elif self.transcription_mode == "offline":
                logger.info(
                    "Step 4: Transcribing offline with whisper.cpp + diarization..."
                )
                try:
                    # Prefer extracted audio when available.
                    local_input = audio_path or recording_path
                    out_dir = Path(tempfile.gettempdir())

                    txt_path, json_path = transcribe_and_diarize_local_media(
                        input_path=Path(local_input),
                        out_dir=out_dir,
                        meeting_id=self.meeting_id,
                        language=self.offline_language,
                        diarize=True,
                        max_speakers=self.offline_max_speakers,
                    )

                    # Upload expects fixed filenames.
                    transcript_txt_path = str(txt_path)
                    transcript_json_path = str(json_path)
                    logger.info(
                        "✅ Offline transcription complete (%s, %s)",
                        transcript_txt_path,
                        transcript_json_path,
                    )
                except Exception as e:
                    logger.exception("Offline transcription failed (non-fatal): %s", e)
            else:
                logger.info(
                    "Step 4: Transcribing with Gemini (audio-only preferred)..."
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
                        f"Transcription target: gs://{self.gcs_bucket}/{target_gcs_path}"
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
                                        "⚠️  Transcription appears to be sample/demo text, not actual meeting content"
                                    )
                                    logger.warning(
                                        "This may indicate the WEBM contains no speech or audio capture failed"
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
                                                    "✅ Transcription stored in Firestore for meeting: %s",
                                                    firestore_meeting_id,
                                                )
                                            else:
                                                logger.warning(
                                                    "Failed to store transcription in Firestore"
                                                )
                                        else:
                                            logger.warning(
                                                "No meeting ID available for Firestore storage (neither FS_MEETING_ID nor MEETING_ID set)"
                                            )
                                    else:
                                        logger.warning(
                                            "No transcription text available to store in Firestore"
                                        )
                                except Exception as firestore_err:
                                    logger.exception(
                                        "Error storing transcription in Firestore: %s",
                                        firestore_err,
                                    )
                                    logger.warning(
                                        "Continuing despite Firestore storage failure"
                                    )
                            else:
                                logger.warning(
                                    "Transcription completed but no results returned"
                                )
                        finally:
                            for revoke_path in [target_gcs_path]:
                                try:
                                    self.storage_client.revoke_public_access(
                                        revoke_path
                                    )
                                except Exception as revoke_err:
                                    logger.debug(
                                        "Could not revoke public access (may not be public): %s",
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
            if transcript_txt_path and os.path.exists(transcript_txt_path):
                transcript_txt_uploaded = self.storage_client.upload_file(
                    transcript_txt_path, f"{self.gcs_path}/transcript.txt"
                )
            if transcript_json_path and os.path.exists(transcript_json_path):
                transcript_json_uploaded = self.storage_client.upload_file(
                    transcript_json_path, f"{self.gcs_path}/transcript.json"
                )

            logger.info(
                f"Successfully uploaded recording to gs://{self.gcs_bucket}/{self.gcs_path}/"
            )
            if transcript_txt_uploaded and transcript_json_uploaded:
                logger.info("✅ Transcripts also uploaded successfully")

            # Cleanup local files (recording + transcripts)
            try:
                if os.path.exists(recording_path):
                    os.remove(recording_path)
                    logger.info(f"Cleaned up file: {recording_path}")
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
                        f"Failed to cleanup extracted audio {audio_path}: {cleanup_err}"
                    )

            if transcript_txt_path and os.path.exists(transcript_txt_path):
                os.remove(transcript_txt_path)
                logger.debug(f"Removed local transcript: {transcript_txt_path}")
            if transcript_json_path and os.path.exists(transcript_json_path):
                os.remove(transcript_json_path)
                logger.debug(f"Removed local transcript: {transcript_json_path}")

            return True

        except Exception as e:
            logger.exception(f"Error processing meeting: {e}")
            return False

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
            # ALWAYS trigger shutdown of meeting-bot, regardless of success or failure
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

    # Also check for generic business meeting patterns that suggest sample content
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
                    f"Error during shutdown after fatal error: {shutdown_error}"
                )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
