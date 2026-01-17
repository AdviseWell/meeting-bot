# Meeting Bot Logging Enhancements

This document describes the comprehensive DEBUG logging added to the meeting bot system to help diagnose:
- Meetings where the bot didn't join but should have
- Recipients that should have received transcripts and files but didn't

## Overview

All new logs are at **DEBUG** level and provide verbose details about:
1. Pub/Sub messages received (JSON)
2. Scheduled meetings discovered (JSON)
3. Deduplication decisions
4. Recipients of files (fanout process)
5. Logic and decisions before/during/after meetings

## Controller Logging (`controller/main.py`)

### 1. Pub/Sub Message Reception
**Location:** `_pubsub_callback()`

Logs every incoming Pub/Sub message with full details:
```
DEBUG: PUB/SUB MESSAGE RECEIVED
DEBUG: Message ID: <message_id>
DEBUG: Full message data: <JSON payload>
DEBUG: Meeting ID: <meeting_id>
DEBUG: Organization ID: <org_id>
```

**Captures:**
- Complete message payload
- Meeting and organization identifiers
- Decision whether to use scheduled meeting flow or legacy flow

### 2. Scheduled Meeting Discovery
**Location:** `_query_meetings_needing_bots()`

Logs the discovery process for scheduled meetings:
```
DEBUG: SCHEDULED MEETING DISCOVERY
DEBUG: Query mode: collection/collection_group
DEBUG: Querying meetings: path=<path>, status=<status>
```

For each meeting found:
```
DEBUG: Evaluating meeting <meeting_id>:
DEBUG:   Meeting data: <JSON>
DEBUG:   SKIP: Already has bot_instance_id / Missing meeting_url
DEBUG:   CANDIDATE: Meeting needs bot (url=<url>)
```

**Captures:**
- Total meetings scanned
- Meetings skipped (with reasons)
- Meetings selected as candidates
- Full meeting document data for each candidate

### 3. Session Deduplication Logic
**Location:** `_try_create_or_update_session_for_meeting()`

Detailed logging of the deduplication algorithm:
```
DEBUG: SESSION DEDUPLICATION LOGIC
DEBUG: Meeting ID: <meeting_id>
DEBUG: Meeting data: <JSON>
DEBUG: Meeting URL: <url>
DEBUG: Organization ID: <org_id>
DEBUG: User ID: <user_id>
DEBUG: Computed session ID (SHA256 of org+URL): <session_id>
```

**Decision logging:**
```
DEBUG: DEDUPLICATION DECISION: No meeting URL found, cannot deduplicate
DEBUG: DEDUPLICATION DECISION: New session created for org=<org_id>, url=<url>
DEBUG: DEDUPLICATION DECISION: Existing session found - bot will be shared
```

**Subscriber tracking:**
```
DEBUG: FANOUT RECIPIENT: User <user_id> will receive copies via fanout
DEBUG: FANOUT RECIPIENT: User <user_id> already subscribed
```

**Captures:**
- Why deduplication succeeded or failed
- Session ID computation
- Whether a new session was created or existing one reused
- All subscribers who will receive files

### 4. Fanout Process (File Distribution)
**Location:** `_fanout_meeting_session_artifacts()`

Comprehensive logging of file distribution to all subscribers:
```
DEBUG: FANOUT LOGIC - ARTIFACT DISTRIBUTION
DEBUG: Organization ID: <org_id>
DEBUG: Session ID: <session_id>
DEBUG: Session data: <JSON>
DEBUG: Total subscribers found: <count>
```

For each subscriber:
```
DEBUG: Subscriber 1: user_id=<user_id>, data=<JSON>
DEBUG: FANOUT SOURCE
DEBUG: Source User ID: <user_id>
DEBUG: Source Meeting ID: <meeting_id>
DEBUG: Source GCS prefix: gs://<bucket>/<path>
DEBUG: Found <count> source objects
```

For additional subscribers:
```
DEBUG: Processing additional subscriber:
DEBUG:   User ID: <user_id>
DEBUG:   Meeting ID: <meeting_id>
DEBUG:   Meeting Path: <path>
DEBUG:   Destination GCS prefix: gs://<bucket>/<path>
DEBUG:   Copying artifacts...
DEBUG:     COPY: <src> -> <dst>
DEBUG:     SKIP (exists): <file>
DEBUG:   Copy complete: <copied> copied, <skipped> skipped, <total> total
DEBUG:   FANOUT RECIPIENT: User <user_id> received <count> files
DEBUG:   FANOUT RECIPIENT UPDATE: User <user_id> meeting updated with transcription
```

**Captures:**
- All subscribers in the session
- Source location for files
- Each file being copied
- Files skipped (already exist)
- Meeting document updates for each recipient
- Success/failure of transcription delivery

### 5. Bot Claiming Logic
Enhanced logging for bot instance claiming decisions:
```
DEBUG: Attempting to find or create bot instance for legacy flow...
DEBUG: Checking meeting document for existing bot_instance_id...
DEBUG: Bot instance ID from meeting doc: <id>
DEBUG: Attempting to claim bot_instance: <id>
DEBUG: Failed to claim bot instance, checking if it exists...
DEBUG: Creating job without state tracking...
```

**Captures:**
- Whether bot instance was successfully claimed
- Alternative flows when claiming fails
- Job creation success/failure

## Manager Logging (`manager/main.py`)

### 1. Environment and Configuration
**Location:** `run()`

Logs all environment variables and configuration at startup:
```
DEBUG: ENVIRONMENT VARIABLES:
DEBUG:   MEETING_ID: <id>
DEBUG:   FS_MEETING_ID: <id>
DEBUG:   MEETING_URL: <url>
DEBUG:   USER_ID: <user_id>
DEBUG:   TEAM_ID: <team_id>
DEBUG:   GCS_BUCKET: <bucket>
DEBUG:   GCS_PATH: <path>
DEBUG:   MEETING_BOT_API_URL: <url>
DEBUG:   TRANSCRIPTION_MODE: <mode>
DEBUG:   FIRESTORE_DATABASE: <database>
DEBUG: Meeting metadata: <JSON>
```

**Captures:**
- All configuration affecting meeting processing
- Meeting metadata passed from controller

### 2. Pre-Meeting Decisions
**Location:** `process_meeting()`

Logs decisions before the meeting starts:
```
DEBUG: PRE-MEETING CHECK: Verifying meeting-bot API availability
DEBUG: PRE-MEETING DECISION: Aborting - API not ready
DEBUG: PRE-MEETING DECISION: API ready, proceeding to join meeting
DEBUG: PRE-MEETING: Sending join request to meeting-bot API
DEBUG: Join parameters:
DEBUG:   URL: <url>
DEBUG:   Metadata: <JSON>
DEBUG: PRE-MEETING DECISION: Bot did not join - could indicate meeting not started, incorrect URL, or API issue
DEBUG: PRE-MEETING DECISION: Bot successfully joined meeting
```

**Captures:**
- API readiness checks
- Join request parameters
- Success/failure of joining
- Reasons for failures

### 3. During Meeting
**Location:** `process_meeting()`

Monitors meeting progress:
```
DEBUG: DURING MEETING: Starting monitoring loop
DEBUG: POST-MEETING: Recording file created successfully
DEBUG: Recording path: <path>
```

**Captures:**
- Meeting monitoring status
- Recording file location

### 4. Post-Meeting Processing
**Location:** `process_meeting()`

Detailed logging of all post-meeting steps:

**File Validation:**
```
DEBUG: POST-MEETING: Verifying recording file exists
DEBUG: POST-MEETING: Validating file size
DEBUG: POST-MEETING DECISION: File too small, likely corrupted
DEBUG: POST-MEETING: Uploading to GCS path: <path>
DEBUG: POST-MEETING: WEBM upload successful
```

**Ad-hoc Meeting Creation:**
```
DEBUG: POST-MEETING: Checking for ad-hoc meeting requirements
DEBUG: Fetching meeting document from Firestore...
DEBUG: Meeting data from Firestore: <JSON>
DEBUG: Calculating recording duration...
DEBUG: Recording duration: <seconds> seconds
DEBUG: POST-MEETING DECISION: Creating ad-hoc meeting document
DEBUG: Ad-hoc meeting details:
DEBUG:   organization_id: <org_id>
DEBUG:   user_id: <user_id>
DEBUG:   meeting_url: <url>
DEBUG:   start_at: <timestamp>
```

**Transcription:**
```
DEBUG: POST-MEETING: Starting transcription process
DEBUG: Transcription mode: <mode>
DEBUG: POST-MEETING: Using offline transcription (whisper.cpp)
DEBUG: Transcription input file: <path>
DEBUG: Transcription language: <lang>
DEBUG: Max speakers: <count>
DEBUG: POST-MEETING: Transcription files generated successfully
DEBUG: POST-MEETING: Persisting transcript to Firestore
DEBUG: Firestore meeting ID: <id>
DEBUG: POST-MEETING: Transcript persisted to Firestore successfully
DEBUG: POST-MEETING: Transcription failed (continuing without it)
```

**File Uploads:**
```
DEBUG: POST-MEETING: Checking for transcript files to upload
DEBUG:   transcript_txt_path: <path>
DEBUG:   transcript_json_path: <path>
DEBUG:   transcript_md_path: <path>
DEBUG: Uploading transcript.txt...
DEBUG:   transcript.txt uploaded: True/False
DEBUG: POST-MEETING: Transcript upload summary:
DEBUG:   TXT: True, JSON: True, MD: True, VTT: True
```

**Session Completion:**
```
DEBUG: POST-MEETING: Marking session complete
DEBUG: Artifacts manifest:
DEBUG: <JSON of all uploaded files>
DEBUG: FANOUT: These artifacts will be copied to all session subscribers
```

**Captures:**
- Every file operation (read, validate, upload)
- Ad-hoc meeting creation details
- Transcription process and results
- All artifacts available for distribution
- Success/failure at each step

## Using the Logs

### To diagnose "Bot didn't join but should have":

1. Search logs for the meeting URL or meeting ID
2. Look for `PUB/SUB MESSAGE RECEIVED` or `SCHEDULED MEETING DISCOVERY`
3. Check `PRE-MEETING DECISION` logs for:
   - API not ready
   - Bot did not join
   - Missing/incorrect meeting URL
4. Review `DEDUPLICATION DECISION` logs to see if session was created

### To diagnose "Recipient didn't receive files":

1. Search logs for the user ID or meeting ID
2. Look for `FANOUT RECIPIENT` logs to see if user was subscribed
3. Check `FANOUT LOGIC - ARTIFACT DISTRIBUTION` for:
   - Whether files were found at source
   - Copy operations for that user
   - Meeting document update status
4. Review `Artifacts manifest` to see what files were available

### To see all decisions for a specific meeting:

```bash
# Search for a specific meeting ID across all decision points
grep "meeting_id_here" logs.txt | grep "DECISION\|RECIPIENT\|FANOUT"
```

## Log Format

All debug logs follow this pattern:
- Use semantic prefixes: `PRE-MEETING:`, `DURING MEETING:`, `POST-MEETING:`, `DECISION:`
- Include full JSON for data structures (using `json.dumps()`)
- Use separator lines (`=` * 80) for major sections
- Include both high-level decisions and detailed operations

## Environment Variable

To see these DEBUG logs, ensure:
```bash
LOG_LEVEL=DEBUG
```

For the controller, this is already set in the code. The logs will appear in:
- Kubernetes pod logs (`kubectl logs <pod-name>`)
- Cloud Logging (if configured)
- Standard output (for local development)
