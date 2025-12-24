#!/usr/bin/env python3
"""
Meeting Bot Controller - Kubernetes Job Orchestrator

This controller monitors GCP Pub/Sub for meeting requests and spawns
Kubernetes Jobs to process each meeting via the manager.

Workflow:
1. Pull messages from GCP Pub/Sub (batch of up to 10)
2. ACK messages immediately
3. Create a Kubernetes Job with the manager image for each message
4. Pass all meeting details as environment variables to the job
5. Continue monitoring for new messages
"""

import os
import sys
import time
import logging
import json
from typing import Optional, Dict

from google.cloud import pubsub_v1
from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

# Reduce noise from some verbose libraries
logging.getLogger("google.auth").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("google.cloud").setLevel(logging.INFO)
logging.getLogger("kubernetes").setLevel(logging.INFO)


class HealthCheckServer:
    """Simple HTTP server for health checks"""

    def __init__(self, port: int = 8080):
        from http.server import HTTPServer, BaseHTTPRequestHandler

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/health":
                    self.send_response(200)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"OK")
                elif self.path == "/ready":
                    self.send_response(200)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"READY")
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format, *args):
                pass  # Suppress default logging

        self.server = HTTPServer(("0.0.0.0", port), HealthHandler)

    def start(self):
        import threading

        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"Health check server started on port 8080")


class MeetingController:
    """Controller that creates Kubernetes Jobs for meeting processing"""

    def __init__(self):
        # Required environment variables
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.subscription_name = os.getenv("PUBSUB_SUBSCRIPTION")
        self.gcs_bucket = os.getenv("GCS_BUCKET")
        self.manager_image = os.getenv("MANAGER_IMAGE")
        self.meeting_bot_image = os.getenv("MEETING_BOT_IMAGE")

        # Kubernetes configuration
        self.k8s_namespace = os.getenv("KUBERNETES_NAMESPACE", "default")
        self.job_service_account = os.getenv("JOB_SERVICE_ACCOUNT", "meeting-bot-job")

        # Optional configuration
        self.node_env = os.getenv("NODE_ENV", "development")
        self.max_recording_duration = int(
            os.getenv("MAX_RECORDING_DURATION_MINUTES", "240")
        )
        self.meeting_inactivity = int(os.getenv("MEETING_INACTIVITY_MINUTES", "15"))
        self.inactivity_detection_delay = int(
            os.getenv("INACTIVITY_DETECTION_START_DELAY_MINUTES", "5")
        )
        self.poll_interval = int(os.getenv("POLL_INTERVAL", "10"))

        # Validate required environment variables
        self._validate_config()

        # Initialize Pub/Sub client
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            self.project_id, self.subscription_name
        )

        # Initialize Kubernetes client
        try:
            # Try to load in-cluster config first
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            # Fall back to kubeconfig for local development
            config.load_kube_config()
            logger.info("Loaded kubeconfig configuration")

        self.batch_v1 = client.BatchV1Api()
        self.core_v1 = client.CoreV1Api()

        logger.info(f"Controller initialized:")
        logger.info(f"  Project: {self.project_id}")
        logger.info(f"  Subscription: {self.subscription_name}")
        logger.info(f"  Namespace: {self.k8s_namespace}")
        logger.info(f"  Manager Image: {self.manager_image}")
        logger.info(f"  Meeting Bot Image: {self.meeting_bot_image}")

    def _validate_config(self):
        """Validate required environment variables"""
        required_vars = {
            "GCP_PROJECT_ID": self.project_id,
            "PUBSUB_SUBSCRIPTION": self.subscription_name,
            "MANAGER_IMAGE": self.manager_image,
            "MEETING_BOT_IMAGE": self.meeting_bot_image,
            "GCS_BUCKET": self.gcs_bucket,
        }

        missing = [k for k, v in required_vars.items() if not v]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

    def create_manager_job(self, message_data: Dict, message_id: str) -> bool:
        """
        Create a Kubernetes Job to process the meeting

        Args:
            message_data: Message data containing meeting details
            message_id: Pub/Sub message ID for unique job naming

        Returns:
            True if job created successfully, False otherwise
        """
        try:
            meeting_id = message_data.get("meeting_id", message_id)
            meeting_url = message_data.get("meeting_url")
            gcs_path = message_data.get("gcs_path", f"recordings/{meeting_id}")

            if not meeting_url:
                logger.error(
                    f"Invalid message data - missing meeting_url: {message_data}"
                )
                return False

            # Generate unique job name (must be DNS-1123 compliant)
            # K8s names must be lowercase alphanumeric + hyphens
            timestamp = int(time.time())
            job_name = f"meeting-{meeting_id.lower()[:50]}-{timestamp}"
            job_name = job_name.replace("_", "-")[:63]  # K8s name length limit

            logger.info(f"Creating Kubernetes Job: {job_name}")

            # Build environment variables for the manager
            env_vars = [
                client.V1EnvVar(name="MEETING_URL", value=meeting_url),
                client.V1EnvVar(name="MEETING_ID", value=meeting_id),
                client.V1EnvVar(name="GCS_PATH", value=gcs_path),
                client.V1EnvVar(name="GCS_BUCKET", value=self.gcs_bucket),
                client.V1EnvVar(name="MEETING_BOT_IMAGE", value=self.meeting_bot_image),
                client.V1EnvVar(name="NODE_ENV", value=self.node_env),
                client.V1EnvVar(
                    name="MAX_RECORDING_DURATION_MINUTES",
                    value=str(self.max_recording_duration),
                ),
                client.V1EnvVar(
                    name="MEETING_INACTIVITY_MINUTES",
                    value=str(self.meeting_inactivity),
                ),
                client.V1EnvVar(
                    name="INACTIVITY_DETECTION_START_DELAY_MINUTES",
                    value=str(self.inactivity_detection_delay),
                ),
            ]

            # Add ALL fields from Pub/Sub message as environment variables
            # This ensures the manager has all the data it needs for the meeting-bot API
            # We add both original case AND uppercase versions for compatibility
            for key, value in message_data.items():
                if value is not None and isinstance(value, (str, int, float, bool)):
                    # Skip keys we've already added
                    if key.lower() not in ["meeting_url", "meeting_id", "gcs_path"]:
                        # Add original case (e.g., bearerToken, teamId, userId)
                        env_vars.append(client.V1EnvVar(name=key, value=str(value)))

                        # Also add UPPERCASE version for backward compatibility (e.g., BEARERTOKEN, TEAM_ID)
                        env_key_upper = key.upper().replace("-", "_")
                        if env_key_upper != key:  # Only add if different from original
                            env_vars.append(
                                client.V1EnvVar(name=env_key_upper, value=str(value))
                            )

            # Add optional metadata fields (for backward compatibility)
            if message_data.get("meeting_title"):
                env_vars.append(
                    client.V1EnvVar(
                        name="MEETING_TITLE", value=message_data["meeting_title"]
                    )
                )
            if message_data.get("organizer"):
                env_vars.append(
                    client.V1EnvVar(
                        name="MEETING_ORGANIZER", value=message_data["organizer"]
                    )
                )
            if message_data.get("start_time"):
                env_vars.append(
                    client.V1EnvVar(
                        name="MEETING_START_TIME", value=message_data["start_time"]
                    )
                )

            # Container 1: meeting-bot (TypeScript app that joins meetings)
            meeting_bot_container = client.V1Container(
                name="meeting-bot",
                image=self.meeting_bot_image,
                image_pull_policy="IfNotPresent",
                env=[
                    client.V1EnvVar(name="PORT", value="3000"),
                    client.V1EnvVar(name="NODE_ENV", value=self.node_env),
                    client.V1EnvVar(
                        name="MAX_RECORDING_DURATION_MINUTES",
                        value=str(self.max_recording_duration),
                    ),
                    client.V1EnvVar(
                        name="MEETING_INACTIVITY_MINUTES",
                        value=str(self.meeting_inactivity),
                    ),
                    client.V1EnvVar(
                        name="INACTIVITY_DETECTION_START_DELAY_MINUTES",
                        value=str(self.inactivity_detection_delay),
                    ),
                    # Disable S3 upload - manager will handle the recording file
                    client.V1EnvVar(name="S3_ENDPOINT", value=""),
                ],
                volume_mounts=[
                    client.V1VolumeMount(
                        name="recordings", mount_path="/usr/src/app/dist/_tempvideo"
                    ),
                    # Mount shared memory for Chrome (prevents crashes)
                    client.V1VolumeMount(name="dshm", mount_path="/dev/shm"),
                    # Mount tmp for XDG and PulseAudio runtime directories
                    client.V1VolumeMount(name="tmp", mount_path="/tmp"),
                ],
                resources=client.V1ResourceRequirements(
                    requests={
                        "cpu": "3000m",  # 2 CPU cores for smooth audio/video processing
                        "memory": "2Gi",  # 2 GB memory
                        "ephemeral-storage": "10Gi",
                    },
                    limits={
                        "cpu": "4000m",  # 4 CPU cores (doubled for better performance)
                        "memory": "3Gi",  # 4 GB memory (increased for high-quality recording)
                        "ephemeral-storage": "10Gi",
                    },
                ),
            )

            # Container 2: manager (Python orchestrator that calls meeting-bot API)
            manager_container = client.V1Container(
                name="manager",
                image=self.manager_image,
                env=env_vars
                + [
                    # Manager needs to communicate with meeting-bot on localhost
                    client.V1EnvVar(
                        name="MEETING_BOT_API_URL", value="http://localhost:3000"
                    ),
                ],
                volume_mounts=[
                    client.V1VolumeMount(name="recordings", mount_path="/recordings")
                ],
                image_pull_policy="IfNotPresent",
                resources=client.V1ResourceRequirements(
                    requests={
                        "cpu": "2500m",  # 2.5 CPU cores
                        "memory": "1Gi",  # 1 GB memory (doubled)
                        "ephemeral-storage": "10Gi",
                    },
                    limits={
                        "cpu": "3750m",  # 50% higher (3.75 CPU cores)
                        "memory": "1536Mi",  # 1.5 GB memory (doubled)
                        "ephemeral-storage": "10Gi",
                    },
                ),
            )

            # Define the pod template with BOTH containers
            template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "app": "meeting-bot-manager",
                        "meeting-id": meeting_id[:63],  # K8s label value max length
                    },
                    annotations={
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
                    },
                ),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    priority_class_name="high-priority",
                    containers=[meeting_bot_container, manager_container],
                    service_account_name=self.job_service_account,
                    # Security context for audio/video capture
                    security_context=client.V1PodSecurityContext(
                        run_as_user=1001,  # nodejs user
                        run_as_group=1001,
                        fs_group=1001,  # Ensures volume mounts have correct permissions
                    ),
                    volumes=[
                        client.V1Volume(
                            name="recordings", empty_dir=client.V1EmptyDirVolumeSource()
                        ),
                        # Shared memory for Chrome
                        client.V1Volume(
                            name="dshm",
                            empty_dir=client.V1EmptyDirVolumeSource(
                                medium="Memory", size_limit="2Gi"
                            ),
                        ),
                        # Temporary storage for runtime dirs (XDG, PulseAudio)
                        client.V1Volume(
                            name="tmp", empty_dir=client.V1EmptyDirVolumeSource()
                        ),
                    ],
                ),
            )

            # Define the job
            job = client.V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=client.V1ObjectMeta(
                    name=job_name,
                    namespace=self.k8s_namespace,
                    labels={
                        "app": "meeting-bot-manager",
                        "meeting-id": meeting_id[:63],
                        "managed-by": "meeting-bot-controller",
                    },
                ),
                spec=client.V1JobSpec(
                    template=template,
                    backoff_limit=0,  # Do not retry on failure
                    ttl_seconds_after_finished=3600,  # Clean up after 1 hour
                ),
            )

            # Create the job
            self.batch_v1.create_namespaced_job(namespace=self.k8s_namespace, body=job)

            logger.info(f"✅ Created job '{job_name}' for meeting {meeting_id}")
            return True

        except ApiException as e:
            logger.error(f"❌ Kubernetes API error creating job: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error creating manager job: {e}")
            return False

    def process_message(
        self, message: pubsub_v1.types.PubsubMessage, ack_id: str
    ) -> None:
        """
        Process a single Pub/Sub message

        Args:
            message: Pub/Sub message containing meeting details
            ack_id: ACK ID for the message
        """
        try:
            # Parse message data
            message_data = json.loads(message.data.decode("utf-8"))
            logger.info(
                f"📨 Received message {message.message_id}: {message_data.get('meeting_id', 'unknown')}"
            )

            # ACK the message immediately to prevent redelivery
            self.subscriber.acknowledge(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [ack_id],
                }
            )
            logger.info(f"✓ ACKed message {message.message_id}")

            # Create Kubernetes Job
            success = self.create_manager_job(message_data, message.message_id)

            if not success:
                logger.warning(
                    f"⚠️  Job creation failed for message {message.message_id}, but message was already ACKed"
                )

        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in message {message.message_id}: {e}")
            # ACK invalid messages to remove them from queue
            self.subscriber.acknowledge(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [ack_id],
                }
            )

        except Exception as e:
            logger.error(
                f"❌ Error processing message {message.message_id}: {e}", exc_info=True
            )
            # ACK to prevent infinite retries
            self.subscriber.acknowledge(
                request={
                    "subscription": self.subscription_path,
                    "ack_ids": [ack_id],
                }
            )

    def run(self):
        """Main run loop - continuously process messages"""
        logger.info("=" * 50)
        logger.info("🚀 Meeting Bot Controller starting...")
        logger.info("=" * 50)
        logger.info(f"📡 Project ID: {self.project_id}")
        logger.info(f"📡 Subscription: {self.subscription_name}")
        logger.info(f"📁 Namespace: {self.k8s_namespace}")
        logger.info(f"🐳 Manager Image: {self.manager_image}")
        logger.info(f"🐳 Meeting Bot Image: {self.meeting_bot_image}")
        logger.info("=" * 50)

        # Start health check server
        health_server = HealthCheckServer()
        health_server.start()

        while True:
            try:
                # Pull messages with a short timeout (batch of up to 10)
                response = self.subscriber.pull(
                    request={
                        "subscription": self.subscription_path,
                        "max_messages": 10,
                    },
                    timeout=30.0,
                )

                if response.received_messages:
                    logger.info(
                        f"📬 Received {len(response.received_messages)} message(s)"
                    )

                    for received_message in response.received_messages:
                        self.process_message(
                            received_message.message, received_message.ack_id
                        )
                else:
                    logger.debug(f"No messages, waiting {self.poll_interval}s...")
                    time.sleep(self.poll_interval)

            except KeyboardInterrupt:
                logger.info("👋 Received shutdown signal")
                break
            except Exception as e:
                logger.error(f"❌ Error in main loop: {e}", exc_info=True)
                time.sleep(self.poll_interval)


def main():
    """Entry point"""
    try:
        controller = MeetingController()
        controller.run()
    except KeyboardInterrupt:
        logger.info("👋 Shutting down controller")
        sys.exit(0)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
