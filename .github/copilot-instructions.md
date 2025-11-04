# GitHub Copilot Instructions for meeting-bot

## General Guidelines

- **Never write large scripts directly into terminal commands**: Break complex operations into separate script files instead of embedding them in `run_in_terminal` commands
- When working with Python scripts that are more than 10-15 lines, create a separate `.py` file rather than using inline Python with `-c`
- Use `create_file` or `replace_string_in_file` tools to create/modify script files, then execute them via terminal

## Code Style

- Follow PEP 8 style guidelines for Python code
- Maximum line length: 79 characters
- Use descriptive variable names
- Add type hints to function signatures
- Include docstrings for classes and functions

## Project Structure

- **controller/**: Python-based meeting controller service
- **manager/**: Python-based meeting manager service  
- **src/**: TypeScript/Node.js bot implementation
- **scripts/**: Build and deployment scripts

## Testing

- Always test changes in isolation before integrating
- Use descriptive test file names (e.g., `test_transcription.py`)
- Clean up test files and artifacts after validation

## Google Cloud Services

- Default project: `aw-gemini-api-central`
- Default region: `asia-southeast1`
- GCS bucket: `advisewell-firebase-development`
- Always check IAM permissions when working with Google Cloud APIs

## Transcription Services

- Using Google Cloud Speech-to-Text V2 API
- **Chirp 3 model** with default recognizer limitations:
  - ✅ Automatic punctuation
  - ❌ No `enable_word_time_offsets` (word timestamps)
  - ❌ No `diarization_config` (speaker diarization)
- **latest_long model** limitations:
  - ✅ Automatic punctuation  
  - ✅ Word timestamps (`enable_word_time_offsets`)
  - ❌ No speaker diarization in BatchRecognize API
- **BatchRecognize API** (Speech V2) does NOT support speaker diarization
- For speaker diarization, would need to use Speech V1 API or streaming/sync recognition (with file size/duration limits)

## Docker & Deployment

- Multi-container setup with controller, manager, and production services
- Use provided build scripts in `scripts/` directory
- Check `DEPLOYMENT.md` for deployment procedures
