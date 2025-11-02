# Meeting Bot Controller

A Python-based controller service that manages the lifecycle of meeting recordings for the Meeting Bot platform.

## Overview

The controller runs as a Kubernetes Job (triggered by KEDA) and performs the following workflow:

1. **Pull Message**: Retrieves a message from GCP Pub/Sub queue
2. **Join Meeting**: Calls the meeting-bot API to join the meeting
3. **Monitor**: Checks meeting status every 10 seconds until completion
4. **Convert**: Converts the recording to MP4 format and extracts AAC audio
5. **Upload**: Saves both files to Google Cloud Storage

## Architecture

- **Deployment**: Kubernetes Job in a pod with the main meeting-bot service
- **Triggering**: KEDA scales based on Pub/Sub queue depth
- **Language**: Python 3.11
- **Dependencies**: ffmpeg for media conversion, GCP client libraries

## Components

### Main Application (`main.py`)
Orchestrates the entire workflow and coordinates between components.

### Pub/Sub Client (`pubsub_client.py`)
- Connects to GCP Pub/Sub
- Pulls messages with auto-acknowledgment
- Decodes message data

### Meeting Monitor (`meeting_monitor.py`)
- Calls meeting-bot API to join meetings
- Polls job status every 10 seconds
- Detects meeting completion and retrieves recording path

### Media Converter (`media_converter.py`)
- Converts recordings to MP4 using H.264 codec
- Extracts audio as AAC
- Uses ffmpeg for all conversions

### Storage Client (`storage_client.py`)
- Uploads files to Google Cloud Storage
- Handles file metadata and content types
- Manages GCS paths from message data

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GCP_PROJECT_ID` | Yes | Google Cloud Project ID |
| `PUBSUB_SUBSCRIPTION` | Yes | Pub/Sub subscription name |
| `GCS_BUCKET` | Yes | Google Cloud Storage bucket name |
| `MEETING_BOT_API_URL` | No | Meeting bot API endpoint (default: `http://localhost:3000`) |

## Message Format

The controller expects Pub/Sub messages with the following JSON structure:

```json
{
  "meeting_id": "unique-meeting-identifier",
  "meeting_url": "https://meet.google.com/abc-defg-hij",
  "gcs_path": "recordings/2024/11/meeting-123"
}
```

## Output

Files are uploaded to GCS with the following structure:

```
gs://{GCS_BUCKET}/{gcs_path}/video.mp4
gs://{GCS_BUCKET}/{gcs_path}/audio.aac
```

## Building

### Docker Build

```bash
docker build -f Dockerfile.controller -t meeting-bot-controller .
```

### Local Development

```bash
cd controller
pip install -r requirements.txt
python main.py
```

## Deployment

The controller is automatically built and pushed to Google Artifact Registry via GitHub Actions:

- **Development**: Pushes to `australia-southeast1-docker.pkg.dev/aw-development-7226/meeting-bot-controller/controller`
- **Production**: Pushes to `australia-southeast1-docker.pkg.dev/aw-production-4df9/meeting-bot-controller/controller`

### Kubernetes Job Example

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: meeting-controller
spec:
  template:
    spec:
      containers:
      - name: controller
        image: australia-southeast1-docker.pkg.dev/aw-production-4df9/meeting-bot-controller/controller:latest
        env:
        - name: GCP_PROJECT_ID
          value: "your-project-id"
        - name: PUBSUB_SUBSCRIPTION
          value: "meeting-attendee-subscription"
        - name: GCS_BUCKET
          value: "your-recordings-bucket"
        - name: MEETING_BOT_API_URL
          value: "http://meeting-bot-service:3000"
      restartPolicy: OnFailure
```

## KEDA Scaling

The controller is designed to work with KEDA's Pub/Sub scaler:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledJob
metadata:
  name: meeting-controller-scaledjob
spec:
  jobTargetRef:
    template:
      spec:
        # ... same as Job spec above
  triggers:
  - type: gcp-pubsub
    metadata:
      subscriptionName: meeting-attendee-subscription
      subscriptionSize: "1"
```

## Error Handling

- Failed messages are logged but acknowledged to prevent infinite retries
- Media conversion failures are logged with ffmpeg output
- API failures include retry logic in the monitoring loop
- All errors return non-zero exit codes for Kubernetes restart handling

## Logging

All components use Python's logging module with structured output:
- INFO: Normal workflow progress
- WARNING: Recoverable issues
- ERROR: Failed operations
- DEBUG: Detailed debugging information

## Performance

- **Conversion**: Uses ffmpeg with medium preset for balanced speed/quality
- **Monitoring**: 10-second polling interval (configurable)
- **Timeout**: 4-hour maximum wait time for meetings (configurable)

## Security

- Runs as non-root user (UID 1000)
- Credentials via GCP Workload Identity
- No secrets in environment variables
- Temporary files cleaned up after upload

## Future Enhancements

- Parallel conversion of MP4 and AAC
- Configurable video quality settings
- Support for additional audio formats
- Webhook notifications on completion
- Metrics and monitoring integration
