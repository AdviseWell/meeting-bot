"""
Transcription Client - Uses Google Chirp 3 for speech-to-text transcription
"""

import os
import time
import logging
from typing import Optional, Dict
from google.cloud import speech_v2
from google.cloud.speech_v2 import SpeechClient
from google.cloud.speech_v2.types import cloud_speech
from google.api_core.client_options import ClientOptions

logger = logging.getLogger(__name__)


class TranscriptionClient:
    """Client for transcribing audio files using Google Chirp 3"""

    def __init__(
        self,
        project_id: str = "aw-gemini-api-central",
        region: str = "asia-southeast1",  # Singapore - closest to Sydney
    ):
        """
        Initialize the transcription client

        Args:
            project_id: Google Cloud project ID for AI workloads
            region: GCP region for Chirp 3 (must be regional, not global)
        """
        self.project_id = project_id
        self.region = region
        self.client = None

        try:
            # Chirp 3 requires a regional endpoint, not global
            client_options = ClientOptions(
                api_endpoint=f"{region}-speech.googleapis.com"
            )
            self.client = SpeechClient(client_options=client_options)
            logger.info(
                f"Initialized Chirp 3 transcription client for project: {project_id}, region: {region}"
            )
        except Exception as e:
            logger.error(f"Failed to initialize transcription client: {e}")
            raise

    def transcribe_audio(
        self,
        audio_uri: str,
        language_code: str = "en-US",
        enable_automatic_punctuation: bool = True,
    ) -> Optional[Dict]:
        """
        Transcribe an audio file using Chirp 3

        Args:
            audio_uri: GCS URI of the audio file (gs://bucket/file.aac)
            language_code: Language code (default: en-US)
            enable_automatic_punctuation: Add punctuation to transcript

        Returns:
            Dictionary with transcript and metadata, or None if failed

        Note:
            Chirp 3 with default recognizer supports automatic punctuation
            but does not support word timestamps or speaker diarization.
        """
        try:
            logger.info(f"Starting Chirp 3 transcription for: {audio_uri}")

            # Configure recognition request
            # Note: Chirp 3 with default recognizer has limitations:
            # - Does NOT support enable_word_time_offsets
            # - Does NOT support diarization_config
            # For these features, a custom recognizer must be created
            config = cloud_speech.RecognitionConfig(
                auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
                language_codes=[language_code],
                model="chirp_3",
                features=cloud_speech.RecognitionFeatures(
                    enable_automatic_punctuation=enable_automatic_punctuation,
                    # Chirp 3 does not support these features with default recognizer
                    # enable_word_time_offsets=True,
                    # diarization_config=None,
                ),
            )  # Set up the audio source
            file_metadata = cloud_speech.BatchRecognizeFileMetadata(
                uri=audio_uri,
            )

            # Create the recognition request (use regional location, not global)
            request = cloud_speech.BatchRecognizeRequest(
                recognizer=f"projects/{self.project_id}/locations/{self.region}/recognizers/_",
                config=config,
                files=[file_metadata],
                recognition_output_config=cloud_speech.RecognitionOutputConfig(
                    inline_response_config=cloud_speech.InlineOutputConfig(),
                ),
            )

            # Start the transcription operation
            logger.info("Submitting transcription request to Chirp 3...")
            operation = self.client.batch_recognize(request=request)

            logger.info("Waiting for transcription to complete...")
            response = operation.result(timeout=3600)  # 1 hour timeout

            # Process the results
            transcript_data = self._process_response(response)

            if transcript_data:
                logger.info(
                    f"✅ Transcription complete! {transcript_data['word_count']} words transcribed"
                )
                return transcript_data
            else:
                logger.warning("Transcription completed but no results found")
                return None

        except Exception as e:
            logger.exception(f"Error during transcription: {e}")
            return None

    def _process_response(self, response) -> Optional[Dict]:
        """
        Process the transcription response and extract structured data

        Args:
            response: BatchRecognizeResponse from Chirp 3

        Returns:
            Dictionary with transcript, segments, and metadata
        """
        try:
            full_transcript = []
            segments = []
            total_words = 0
            total_duration = 0.0

            # Process each file result (should be just one)
            for file_result in response.results.values():
                for result in file_result.transcript.results:
                    if not result.alternatives:
                        continue

                    # Get the best alternative
                    alternative = result.alternatives[0]

                    # Extract transcript text
                    transcript_text = alternative.transcript.strip()
                    if transcript_text:
                        full_transcript.append(transcript_text)

                    # Count words (approximate)
                    total_words += len(transcript_text.split())

                    # Create segment
                    segment = {
                        "transcript": transcript_text,
                        "confidence": (
                            alternative.confidence
                            if hasattr(alternative, "confidence")
                            else None
                        ),
                        "language_code": (
                            result.language_code
                            if hasattr(result, "language_code")
                            else None
                        ),
                    }

                    segments.append(segment)

            # Combine all transcript parts
            combined_transcript = " ".join(full_transcript)

            result = {
                "transcript": combined_transcript,
                "segments": segments,
                "word_count": total_words,
                "duration_seconds": total_duration,
            }

            return result

        except Exception as e:
            logger.exception(f"Error processing transcription response: {e}")
            return None

    def save_transcript(
        self, transcript_data: Dict, output_path: str, format: str = "txt"
    ) -> bool:
        """
        Save transcript to a file

        Args:
            transcript_data: Transcript data from transcribe_audio()
            output_path: Local file path to save to
            format: Output format ('txt', 'json', or 'srt')

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Saving transcript to: {output_path}")

            if format == "txt":
                # Simple text format
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(transcript_data["transcript"])
                    f.write("\n\n")
                    f.write("--- Metadata ---\n")
                    f.write(f"Words: {transcript_data['word_count']}\n")
                    f.write(f"Duration: {transcript_data['duration_seconds']:.2f}s\n")

            elif format == "json":
                # JSON format with all details
                import json

                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(transcript_data, f, indent=2, ensure_ascii=False)

            elif format == "srt":
                # SRT subtitle format (simplified)
                with open(output_path, "w", encoding="utf-8") as f:
                    for idx, segment in enumerate(transcript_data["segments"], 1):
                        f.write(f"{idx}\n")
                        f.write(f"00:00:00,000 --> 00:00:00,000\n")  # Simplified timing
                        f.write(f"{segment['transcript']}\n\n")

            else:
                logger.error(f"Unsupported format: {format}")
                return False

            logger.info(f"✅ Transcript saved successfully")
            return True

        except Exception as e:
            logger.exception(f"Error saving transcript: {e}")
            return False
