"""
Audio Transcription Client - Uses Google Gemini for speech-to-text transcription

This module provides audio transcription capabilities using Google's Gemini models
via the GenAI API. It supports direct transcription with speaker diarization,
timestamps, and action items extraction.
"""

import os
import time
import logging
import json
from typing import Optional, Dict, Any, List, Tuple
from google import genai
from google.genai.types import HttpOptions, Part
from io import BytesIO
import requests

logger = logging.getLogger(__name__)


class TranscriptionClient:
    """Client for transcribing audio files using Google Gemini"""

    def __init__(
        self,
        project_id: str = "aw-gemini-api-central",
        region: str = "australia-southeast1",  # Sydney, Australia
    ):
        """
        Initialize the transcription client

        Args:
            project_id: Google Cloud project ID for AI workloads
            region: GCP location for Vertex AI (default: Sydney)
        """
        self.project_id = project_id
        self.region = region
        self.client = None

        try:
            self.client = genai.Client(
                vertexai=True,
                project=project_id,
                location=region,
                http_options=HttpOptions(
                    api_version="v1",
                    timeout=1800000,  # 30 minutes in milliseconds
                    headers={"Connection": "close"},  # Prevent stale connections
                ),
            )
            logger.info(
                f"Initialized Gemini transcription client for project: {project_id}, region: {region}"
            )
        except Exception as e:
            logger.error(f"Failed to initialize transcription client: {e}")
            raise

    def transcribe_audio(
        self,
        audio_uri: str,
        language_code: str = "en-AU",
        enable_speaker_diarization: bool = True,
        enable_timestamps: bool = False,
        enable_action_items: bool = True,
    ) -> Optional[Dict]:
        """
        Transcribe an audio file using Gemini

        Args:
            audio_uri: HTTP/HTTPS URL or GCS URI of the audio file
            language_code: Language code (default: en-AU for Australian English)
            enable_speaker_diarization: Enable speaker identification
            enable_timestamps: Enable timestamped transcript
            enable_action_items: Extract action items from the conversation

        Returns:
            Dictionary with transcript and metadata, or None if failed
        """
        try:
            logger.info(f"Starting Gemini transcription for: {audio_uri}")

            # Download audio file
            if audio_uri.startswith("gs://"):
                # For GCS URIs, convert to signed URL or download via GCS client
                logger.error(
                    "Direct GCS URIs not supported - please provide signed URL"
                )
                return None

            logger.info("Downloading audio file...")
            response = requests.get(audio_uri, timeout=60)
            response.raise_for_status()
            audio_bytes = response.content

            # Detect MIME type from URL or default to m4a
            mime_type = "audio/mp4"  # M4A files use audio/mp4 MIME type
            if audio_uri.endswith(".mp3"):
                mime_type = "audio/mpeg"
            elif audio_uri.endswith(".wav"):
                mime_type = "audio/wav"
            elif audio_uri.endswith(".ogg"):
                mime_type = "audio/ogg"
            elif audio_uri.endswith(".webm"):
                mime_type = "video/webm"  # WebM can contain both audio and video

            logger.info(
                f"Downloaded {len(audio_bytes)} bytes ({len(audio_bytes) / (1024 * 1024):.2f} MB)"
            )

            # Validate audio file has content
            if len(audio_bytes) < 1000:  # Less than 1KB is likely empty/corrupted
                logger.error(
                    f"Audio file too small ({len(audio_bytes)} bytes) - likely empty or corrupted"
                )
                return None

            # Convert to optimized format
            audio_bytes, optimized_mime_type = self._convert_to_optimized_format(
                audio_bytes, mime_type
            )

            # Validate converted audio
            if len(audio_bytes) < 1000:
                logger.error(
                    f"Converted audio file too small ({len(audio_bytes)} bytes) - conversion failed"
                )
                return None

            # Build transcription options
            options = {
                "fullTranscript": True,  # Always get full transcript
                "timestamps": enable_timestamps,
                "speakerIdentification": enable_speaker_diarization,
                "summary": False,  # Can be enabled if needed
                "actionItems": enable_action_items,
                "customPrompt": None,
            }

            # Build prompt
            prompt = self._build_transcription_prompt(options, language_code)

            logger.info(f"Sending transcription request with options: {options}")
            logger.debug(f"Transcription prompt: {prompt[:200]}...")

            logger.info("Sending audio to Gemini for transcription...")

            # Create audio part
            audio_part = Part.from_bytes(
                data=audio_bytes, mime_type=optimized_mime_type
            )

            # Generate transcription with Gemini
            start_time = time.time()
            gemini_response = self.client.models.generate_content(
                model="gemini-2.5-flash",  # Latest high-performance model
                contents=[prompt, audio_part],
                config={
                    "temperature": 0.1,  # Low temperature for accuracy
                    "max_output_tokens": 65536,  # Max for Gemini 2.5 Flash
                    "response_mime_type": "text/plain",
                },
            )

            processing_time_ms = int((time.time() - start_time) * 1000)

            # Debug: Log Gemini response details
            logger.info(f"Gemini response received in {processing_time_ms}ms")
            logger.debug(
                f"Gemini response text (first 500 chars): {gemini_response.text[:500]}"
            )

            # Extract and parse response
            transcript_text = gemini_response.text

            # Check for no speech detected
            if transcript_text.strip().upper() == "NO SPEECH DETECTED":
                logger.warning("Gemini detected no speech in the audio file")
                return None

            # Check for common sample text patterns that indicate Gemini generated placeholder content
            sample_patterns = [
                "just to confirm, we're going with the revised plan",
                "i've already circulated the updated document",
                "are we on track to get those approved",
                "what about the marketing materials",
                "speaker 1 (male):",
                "speaker 2 (female):",
            ]

            found_sample_patterns = sum(
                1
                for pattern in sample_patterns
                if pattern.lower() in transcript_text.lower()
            )

            if found_sample_patterns >= 2:
                logger.error("❌ SAMPLE TEXT DETECTED IN TRANSCRIPTION!")
                logger.error(
                    "Gemini returned sample/placeholder text instead of actual transcription"
                )
                logger.error("This typically means:")
                logger.error("  1. The audio file is empty or nearly empty")
                logger.error("  2. The audio file contains no discernible speech")
                logger.error("  3. The audio file is corrupted or invalid")
                logger.error("  4. The meeting bot failed to capture actual audio")
                logger.error("")
                logger.error(
                    "ACTION REQUIRED: Check the meeting bot recording process!"
                )
                logger.error("The M4A file likely contains no actual meeting audio.")

            sections = self._parse_transcription_sections(transcript_text, options)

            # Build result in expected format
            result = {
                "transcript": sections.get("fullTranscript", transcript_text),
                "segments": [],  # Gemini doesn't provide segments like Chirp
                "word_count": len(transcript_text.split()),
                "duration_seconds": 0.0,  # Not available from Gemini
                "processing_time_ms": processing_time_ms,
                "model": "gemini-2.5-flash",
                "sections": sections,  # Include all parsed sections
            }

            logger.info(
                f"✅ Transcription complete! {result['word_count']} words transcribed"
            )
            return result

        except Exception as e:
            logger.exception(f"Error during transcription: {e}")
            return None

    def _convert_to_optimized_format(
        self, audio_bytes: bytes, mime_type: str
    ) -> Tuple[bytes, str]:
        """
        Convert audio to optimized format for Gemini transcription

        Converts to OGG/Opus with speech-optimized settings to reduce file size
        by 60-70% without quality loss.
        """
        try:
            from pydub import AudioSegment

            format_map = {
                "audio/mpeg": "mp3",
                "audio/mp3": "mp3",
                "audio/wav": "wav",
                "audio/flac": "flac",
                "audio/mp4": "m4a",
                "audio/m4a": "m4a",
                "audio/ogg": "ogg",
                "audio/webm": "webm",
                "video/webm": "webm",  # WebM files (audio+video)
            }

            audio_format = format_map.get(mime_type, "mp3")
            original_size = len(audio_bytes)

            logger.info(
                f"Converting audio from {audio_format} ({mime_type}) to optimized format"
            )
            logger.info(
                f"Original audio size: {original_size} bytes ({original_size / (1024 * 1024):.2f} MB)"
            )

            # Load audio
            audio = AudioSegment.from_file(BytesIO(audio_bytes), format=audio_format)

            # Convert to mono, 16kHz, and export as OGG/Opus
            audio = audio.set_channels(1)
            audio = audio.set_frame_rate(16000)

            output = BytesIO()
            audio.export(output, format="ogg", codec="libopus", bitrate="48k")

            converted_bytes = output.getvalue()
            size_reduction = (
                (original_size - len(converted_bytes)) / original_size
            ) * 100

            logger.info(
                f"Audio conversion complete: {original_size / (1024 * 1024):.2f} MB → "
                f"{len(converted_bytes) / (1024 * 1024):.2f} MB "
                f"({size_reduction:.1f}% reduction)"
            )

            return converted_bytes, "audio/ogg"

        except ImportError:
            logger.warning("pydub not installed - using original audio format")
            return audio_bytes, mime_type
        except Exception as e:
            logger.warning(f"Audio conversion failed, using original: {e}")
            return audio_bytes, mime_type

    def _build_transcription_prompt(
        self, options: Dict[str, Any], spelling_preference: str = "en-AU"
    ) -> str:
        """Build transcription prompt based on options"""
        prompt_parts = [
            "You are a professional transcription service. Your task is to transcribe the SPEECH content from the provided AUDIO file.",
            "IMPORTANT: Only transcribe what you actually hear in the audio. Do NOT generate sample text, placeholder content, or fictional conversations.",
            "If you cannot clearly hear speech in the audio, respond with 'NO SPEECH DETECTED' only.",
            "",
            "Please analyze the following audio file and provide:",
        ]

        section_number = 1

        if options.get("fullTranscript"):
            prompt_parts.append(
                f"{section_number}. A complete word-for-word transcript of the ACTUAL SPEECH in the audio file"
            )
            section_number += 1

        if options.get("timestamps"):
            prompt_parts.append(
                f"{section_number}. A timestamped transcript with MM:SS markers at regular intervals"
            )
            section_number += 1

        if options.get("speakerIdentification"):
            prompt_parts.extend(
                [
                    f"{section_number}. Speaker diarization with the following requirements:",
                    "   - Identify different speakers and detect their voice characteristics",
                    "   - Use descriptive labels: 'Speaker 1 (*Male*):', 'Speaker 2 (*Female*):'",
                    "   - Start speaker identification from the VERY FIRST WORDS of the audio",
                    "   - Format EVERY speaker change as a new paragraph with speaker label",
                    "   - Maintain consistent speaker labels throughout the ENTIRE audio",
                ]
            )
            section_number += 1

        if options.get("summary"):
            prompt_parts.append(
                f"{section_number}. A concise summary of the key points discussed"
            )
            section_number += 1

        if options.get("actionItems"):
            prompt_parts.append(
                f"{section_number}. A list of action items, tasks, or decisions mentioned"
            )
            section_number += 1

        if options.get("customPrompt"):
            prompt_parts.append(
                f"{section_number}. Custom Analysis: {options['customPrompt']}"
            )
            section_number += 1

        prompt_parts.extend(
            [
                "\nFormatting Guidelines:",
                "- Use clear Markdown section headers (##) for each requested section",
                "- Maintain consistent formatting throughout",
                "- Start new paragraphs for speaker changes or topic shifts",
                "\nQuality Requirements:",
                "- Be accurate and thorough in your transcription",
                "- Maintain consistency in all labels and formatting",
            ]
        )

        return "\n".join(prompt_parts)

    def _parse_transcription_sections(
        self, text: str, options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Parse transcription response into sections"""
        sections = {}
        current_section = None
        current_content = []

        for line in text.split("\n"):
            if line.startswith("##"):
                # Save previous section
                if current_section and current_content:
                    sections[current_section] = "\n".join(current_content).strip()

                # Start new section
                header = line.replace("##", "").strip().lower()
                current_content = []

                if "full transcript" in header or "transcript" in header:
                    current_section = "fullTranscript"
                elif "timestamp" in header:
                    current_section = "timestampedTranscript"
                elif "speaker" in header or "diarization" in header:
                    current_section = "speakerDiarization"
                elif "summary" in header:
                    current_section = "summary"
                elif "action" in header:
                    current_section = "actionItems"
                else:
                    current_section = None
            else:
                if current_section:
                    current_content.append(line)

        # Save last section
        if current_section and current_content:
            sections[current_section] = "\n".join(current_content).strip()

        # Parse action items into list
        if "actionItems" in sections:
            action_text = sections["actionItems"]
            items = []
            for line in action_text.split("\n"):
                line = line.strip()
                if line and (
                    line[0].isdigit() or line.startswith("-") or line.startswith("•")
                ):
                    item = line.lstrip("0123456789.-•").strip()
                    if item:
                        items.append(item)
            if items:
                sections["actionItems"] = items

        # If no sections parsed, use entire text
        if not sections and options.get("fullTranscript"):
            sections["fullTranscript"] = text

        return sections

    def save_transcript(
        self, transcript_data: Dict, output_path: str, format: str = "txt"
    ) -> bool:
        """
        Save transcript to a file

        Args:
            transcript_data: Transcript data from transcribe_audio()
            output_path: Local file path to save to
            format: Output format ('txt', 'json')

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Saving transcript to: {output_path}")

            if format == "txt":
                with open(output_path, "w", encoding="utf-8") as f:
                    # Write main transcript
                    f.write(transcript_data["transcript"])
                    f.write("\n\n")

                    # Write metadata
                    f.write("--- Metadata ---\n")
                    f.write(f"Words: {transcript_data['word_count']}\n")
                    f.write(
                        f"Processing Time: {transcript_data.get('processing_time_ms', 0) / 1000:.2f}s\n"
                    )
                    f.write(f"Model: {transcript_data.get('model', 'N/A')}\n")

                    # Write additional sections if available
                    if "sections" in transcript_data:
                        sections = transcript_data["sections"]

                        if "speakerDiarization" in sections:
                            f.write("\n\n--- Speaker Diarization ---\n")
                            f.write(sections["speakerDiarization"])

                        if "actionItems" in sections:
                            f.write("\n\n--- Action Items ---\n")
                            items = sections["actionItems"]
                            if isinstance(items, list):
                                for item in items:
                                    f.write(f"- {item}\n")
                            else:
                                f.write(items)

            elif format == "json":
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(transcript_data, f, indent=2, ensure_ascii=False)

            else:
                logger.error(f"Unsupported format: {format}")
                return False

            logger.info(f"✅ Transcript saved successfully")
            return True

        except Exception as e:
            logger.exception(f"Error saving transcript: {e}")
            return False
