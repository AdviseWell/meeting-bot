#!/bin/bash
# Run controller locally against Firebase emulator for fast iteration
# No Docker or K8s required - just Python

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CONTROLLER_DIR="$PROJECT_ROOT/controller"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# Check if Firebase emulator is running
if ! curl -s http://localhost:4000 > /dev/null 2>&1; then
    log_warn "Firebase emulator UI not accessible at localhost:4000"
    log_warn "Make sure the emulator is running!"
fi

# Set environment for Firebase emulator
export FIRESTORE_EMULATOR_HOST="localhost:8080"
export PUBSUB_EMULATOR_HOST="localhost:8085"
export FIREBASE_AUTH_EMULATOR_HOST="localhost:9099"

# GCP project (emulator accepts any project ID)
export GCP_PROJECT_ID="demo-advisewell"
export FIRESTORE_DATABASE="(default)"
export GCS_BUCKET="advisewell-firebase-development"

# Meeting discovery - use collection_group to query across all orgs
export MEETINGS_COLLECTION_PATH="meetings"
export MEETINGS_QUERY_MODE="collection_group"

# Controller settings
export K8S_NAMESPACE="default"
export POLL_INTERVAL="${POLL_INTERVAL:-5}"
export PYTHONUNBUFFERED="1"

# Disable Pub/Sub subscription (polling only)
export PUBSUB_SUBSCRIPTION=""

# Enable dry-run mode (logs K8s job creation but doesn't actually create)
export DRY_RUN="${DRY_RUN:-true}"

# Use a different health port to avoid conflict with Firebase emulator (8080)
export HEALTH_PORT="${HEALTH_PORT:-8088}"

# Manager/bot images (for job spec generation)
export MANAGER_IMAGE="meeting-bot-manager:local"
export MEETING_BOT_IMAGE="meeting-bot:local"

log_info "Starting controller with Firebase emulator..."
log_info "  Firestore: $FIRESTORE_EMULATOR_HOST"
log_info "  Pub/Sub:   $PUBSUB_EMULATOR_HOST"
log_info "  Auth:      $FIREBASE_AUTH_EMULATOR_HOST"
log_info "  Dry Run:   $DRY_RUN"
log_info ""
log_info "Firebase Emulator UI: http://localhost:4000"
log_info ""

cd "$CONTROLLER_DIR"

# Activate venv if available
if [ -d "$PROJECT_ROOT/.venv" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
elif [ -d "$CONTROLLER_DIR/.venv" ]; then
    source "$CONTROLLER_DIR/.venv/bin/activate"
fi

# Run controller
exec python -u main.py "$@"
