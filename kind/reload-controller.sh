#!/bin/bash
# Quick reload of controller after code changes
# Rebuilds and restarts without recreating the cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
CLUSTER_NAME="meeting-bot-dev"

# Colors
GREEN='\033[0;32m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

cd "$PROJECT_ROOT"

log_info "Rebuilding controller image..."
docker build -t meeting-bot-controller:local -f Dockerfile.controller .

log_info "Loading image into KinD..."
kind load docker-image meeting-bot-controller:local --name "$CLUSTER_NAME"

log_info "Restarting controller deployment..."
kubectl rollout restart deployment/controller -n meeting-bot

log_info "Waiting for rollout..."
kubectl rollout status deployment/controller -n meeting-bot --timeout=60s

log_info "Controller reloaded! Tailing logs..."
kubectl logs -f deployment/controller -n meeting-bot
