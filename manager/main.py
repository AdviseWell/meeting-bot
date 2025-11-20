#!/usr/bin/env python3
"""
Meeting Bot Manager - Job Execution

This manager processes a single meeting recording job:
1. Reads job details from environment variables
2. Initiates meeting join via meeting-bot API
3. Monitors meeting status
4. Converts recordings (MP4 + M4A)
5. Uploads to GCS

Designed to run as a Kubernetes Job, spawned by the controller.
"""

import os
import sys
import time
import logging
from typing import Optional, Dict

from meeting_monitor import MeetingMonitor
from media_converter import MediaConverter
from storage_client import StorageClient, FirestoreClient
from transcription_client import TranscriptionClient
from meeting_utils import auto_generate_missing_fields

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
        self.fs_meeting_id = os.environ.get("FS_MEETING_ID")  # Firestore-specific meeting ID
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
        self.media_converter = MediaConverter()
        self.storage_client = StorageClient(self.gcs_bucket)
        self.firestore_client = FirestoreClient(self.firestore_database)
        self.transcription_client = TranscriptionClient(
            project_id="aw-gemini-api-central"
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
                    event_id=event_id
                )
                
                # Use auto-generated values
                metadata["bearerToken"] = auto_generated["bearer_token"]
                metadata["userId"] = auto_generated["user_id"]
                metadata["botId"] = auto_generated["bot_id"]
                
                # Also update meeting_id if it wasn't provided
                if not self.meeting_id:
                    metadata["meeting_id"] = auto_generated["meeting_id"]
                    
                logger.info(f"Auto-generated fields: userId={metadata['userId']}, botId={metadata['botId']}, bearerToken={'***' if metadata['bearerToken'] else 'NONE'}")
            except Exception as e:
                logger.warning(f"Could not auto-generate fields: {e}. Using fallback values.")
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

            # Step 3: Upload original WEBM file to GCS (before conversion)
            logger.info("Step 3: Uploading original WEBM file to GCS...")
            webm_uploaded = self.storage_client.upload_file(
                recording_path, f"{self.gcs_path}/recording.webm"
            )
            if webm_uploaded:
                logger.info(f"✅ Original WEBM uploaded to gs://{self.gcs_bucket}/{self.gcs_path}/recording.webm")
            else:
                logger.warning("Failed to upload original WEBM file (non-fatal)")

            # Step 4: Convert media files
            logger.info("Step 4: Converting media files...")
            mp4_path, m4a_path = self.media_converter.convert(recording_path)
            if not mp4_path or not m4a_path:
                logger.error("Media conversion failed")
                return False

            logger.info(f"Conversion complete - MP4: {mp4_path}, M4A: {m4a_path}")

            # Step 5: Transcribe audio using Gemini (optional, non-fatal)
            logger.info("Step 5: Transcribing audio with Gemini...")
            transcript_txt_path = None
            transcript_json_path = None

            try:
                # Validate M4A file before upload
                if not os.path.exists(m4a_path):
                    logger.error(f"M4A file not found: {m4a_path}")
                    return False

                m4a_size = os.path.getsize(m4a_path)
                logger.info(f"M4A file size: {m4a_size} bytes ({m4a_size / (1024 * 1024):.2f} MB)")

                # Validate file size is reasonable for a meeting recording
                # Typical sizes: 1min ~500KB-2MB, 5min ~2-10MB, 30min ~10-50MB
                if m4a_size < 1000:  # Less than 1KB is definitely empty
                    logger.error(f"❌ M4A file too small ({m4a_size} bytes) - file is empty or corrupted")
                    logger.error("This indicates the meeting bot did not capture any audio")
                    logger.error("Check meeting-bot logs for recording errors")
                    return False
                
                if m4a_size < 100000:  # Less than 100KB (~10 seconds of audio)
                    logger.warning(f"⚠️  M4A file is very small ({m4a_size} bytes / {m4a_size / 1024:.1f} KB)")
                    logger.warning("This is unusually small for a meeting recording")
                    logger.warning("Expected: >500KB for 1 minute, >2MB for 5 minutes")
                    logger.warning("The recording may be incomplete or contain no actual audio")
                    logger.warning("Proceeding with transcription, but results may be poor...")

                # Verify audio content using pydub
                try:
                    from pydub import AudioSegment
                    audio = AudioSegment.from_file(m4a_path, format="m4a")
                    duration_ms = len(audio)
                    duration_sec = duration_ms / 1000
                    duration_min = duration_sec / 60
                    
                    logger.info(f"📊 Audio file duration: {duration_min:.2f} minutes ({duration_sec:.1f} seconds)")
                    
                    # Check if duration is reasonable
                    if duration_sec < 5:
                        logger.error(f"❌ Audio duration is too short ({duration_sec:.1f}s)")
                        logger.error("Recording appears to be nearly empty - likely no meeting audio was captured")
                        logger.error("This will result in sample/placeholder text from Gemini")
                        # Continue anyway to see what happens
                    
                    # Check for silent audio (all samples near zero)
                    max_amplitude = audio.max
                    if max_amplitude < 100:  # Very quiet audio
                        logger.warning(f"⚠️  Audio appears to be silent or nearly silent (max amplitude: {max_amplitude})")
                        logger.warning("The recording may not contain actual speech")
                        
                except ImportError:
                    logger.warning("pydub not available - skipping audio content validation")
                except Exception as e:
                    logger.warning(f"Could not analyze audio file: {e}")

                # Upload the M4A file to GCS first
                m4a_gcs_path = f"{self.gcs_path}/audio.m4a"
                m4a_uploaded = self.storage_client.upload_file(m4a_path, m4a_gcs_path)

                if m4a_uploaded:
                    # Generate signed URL for Gemini to access audio
                    # Uses IAM-based signing to keep files private
                    # Falls back to public URL if IAM permissions missing
                    audio_url = self.storage_client.get_signed_url(
                        m4a_gcs_path, expiration_minutes=120  # 2 hours
                    )

                    if audio_url:
                        logger.info(f"Generated signed URL for audio: {audio_url[:50]}...")
                        logger.info("Transcribing audio with Gemini...")

                        try:
                            # Transcribe the audio with speaker diarization
                            transcript_data = (
                                self.transcription_client.transcribe_audio(
                                    audio_uri=audio_url,
                                    language_code="en-AU",  # Australian
                                    enable_speaker_diarization=True,
                                    enable_timestamps=False,
                                    enable_action_items=True,
                                )
                            )

                            if transcript_data:
                                # Check if transcription looks like sample/demo text
                                transcript_text = transcript_data.get("transcript", "")
                                if _is_sample_transcription(transcript_text):
                                    logger.warning("⚠️  Transcription appears to be sample/demo text, not actual meeting content")
                                    logger.warning("This may indicate the audio file was not processed correctly")
                                    # Continue anyway - better to have sample text than no text

                                import tempfile

                                # Save transcript as TXT
                                transcript_txt_path = os.path.join(
                                    tempfile.gettempdir(),
                                    f"{self.meeting_id}_transcript.txt",
                                )
                                self.transcription_client.save_transcript(
                                    transcript_data,
                                    transcript_txt_path,
                                    format="txt",
                                )

                                # Save transcript as JSON
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
                                    f"✅ Transcription complete! "
                                    f"Words: {transcript_data['word_count']}"
                                )

                                # Store transcription text in Firestore
                                try:
                                    transcription_text = transcript_data.get("transcript", "")
                                    if transcription_text:
                                        # Use FS_MEETING_ID if provided, otherwise fall back to MEETING_ID
                                        firestore_meeting_id = self.fs_meeting_id or self.meeting_id
                                        if firestore_meeting_id:
                                            firestore_stored = self.firestore_client.set_transcription(
                                                firestore_meeting_id, transcription_text
                                            )
                                            if firestore_stored:
                                                logger.info(
                                                    f"✅ Transcription stored in Firestore for meeting: {firestore_meeting_id}"
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
                                    logger.exception(f"Error storing transcription in Firestore: {firestore_err}")
                                    logger.warning("Continuing despite Firestore storage failure")

                            else:
                                logger.warning(
                                    "Transcription completed but no results " "returned"
                                )
                        finally:
                            # Always try to revoke public access
                            # (in case fallback made it public)
                            try:
                                self.storage_client.revoke_public_access(m4a_gcs_path)
                            except Exception as revoke_err:
                                logger.debug(
                                    f"Could not revoke public access "
                                    f"(may not be public): {revoke_err}"
                                )
                    else:
                        logger.warning("Failed to generate signed URL for audio file")
                else:
                    logger.warning(
                        "Failed to upload M4A to GCS, " "skipping transcription step"
                    )

            except Exception as e:
                logger.exception(f"Transcription failed (non-fatal): {e}")
                logger.warning("Continuing with upload despite transcription failure")

            # Step 6: Upload remaining files to GCS
            logger.info("Step 6: Uploading remaining files to GCS...")

            # Upload video
            mp4_uploaded = self.storage_client.upload_file(
                mp4_path, f"{self.gcs_path}/video.mp4"
            )

            # Upload audio (if not already uploaded for transcription)
            if not m4a_uploaded:
                m4a_uploaded = self.storage_client.upload_file(
                    m4a_path, f"{self.gcs_path}/audio.m4a"
                )

            # Upload transcripts if available
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

            if mp4_uploaded and m4a_uploaded:
                logger.info(
                    f"Successfully uploaded files to gs://{self.gcs_bucket}/{self.gcs_path}/"
                )
                if transcript_txt_uploaded and transcript_json_uploaded:
                    logger.info(f"✅ Transcripts also uploaded successfully")

                # Cleanup local files
                self.media_converter.cleanup(recording_path, mp4_path, m4a_path)

                # Cleanup transcript files
                if transcript_txt_path and os.path.exists(transcript_txt_path):
                    os.remove(transcript_txt_path)
                    logger.debug(f"Removed local transcript: {transcript_txt_path}")
                if transcript_json_path and os.path.exists(transcript_json_path):
                    os.remove(transcript_json_path)
                    logger.debug(f"Removed local transcript: {transcript_json_path}")

                return True
            else:
                logger.error("Failed to upload one or more files to GCS")
                return False

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
        "Speaker 2 (*Female*):"
    ]

    # Check if multiple sample indicators are present
    found_indicators = sum(1 for indicator in sample_indicators if indicator.lower() in transcript_text.lower())

    # Also check for generic business meeting patterns that suggest sample content
    generic_patterns = [
        "thank you [name redacted]",
        "really excited about this new project",
        "director of operations",
        "customer touchpoints",
        "pilot program in q3"
    ]

    found_patterns = sum(1 for pattern in generic_patterns if pattern.lower() in transcript_text.lower())

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
