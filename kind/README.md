# KinD Local Development Setup

This directory contains configuration for running the meeting-bot controller locally using KinD (Kubernetes in Docker), connected to the Firebase emulator.

## Prerequisites

- Docker
- KinD (`brew install kind` or `go install sigs.k8s.io/kind@latest`)
- kubectl
- Firebase emulator running (`advisewell-firebase-emulator-1` container)

## Quick Start

```bash
# 1. Make sure Firebase emulator is running
docker ps | grep firebase

# 2. Set up the KinD cluster and deploy controller
./setup-kind.sh

# 3. View controller logs
kubectl logs -f deployment/controller -n meeting-bot

# 4. After making code changes, reload:
./reload-controller.sh
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Host Machine                          │
│                                                              │
│  ┌──────────────────────┐    ┌──────────────────────────┐   │
│  │  Firebase Emulator   │    │      KinD Cluster        │   │
│  │  (Docker container)  │    │                          │   │
│  │                      │◄───┤  ┌──────────────────┐    │   │
│  │  - Firestore :8080   │    │  │    Controller    │    │   │
│  │  - Pub/Sub   :8085   │    │  │   (polls meetings)│   │   │
│  │  - Auth      :9099   │    │  └────────┬─────────┘    │   │
│  │  - UI        :4000   │    │           │              │   │
│  │                      │    │           ▼              │   │
│  └──────────────────────┘    │  ┌──────────────────┐    │   │
│                              │  │   Manager Job    │    │   │
│                              │  │  (per meeting)   │    │   │
│                              │  └──────────────────┘    │   │
│                              └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Files

- `kind-config.yaml` - KinD cluster configuration
- `controller-deployment.yaml` - Kubernetes manifests for the controller
- `setup-kind.sh` - Full setup script (cluster + images + deploy)
- `reload-controller.sh` - Quick reload after code changes
- `seed-emulator.py` - Seed emulator with test data from production

## Seeding Test Data

To copy meetings from production to the emulator:

```bash
# Copy 10 upcoming meetings from advisewell org
python seed-emulator.py --org advisewell --limit 10

# Copy meetings for next 3 days
python seed-emulator.py --days-ahead 3
```

## Debugging

```bash
# Get controller pod name
kubectl get pods -n meeting-bot

# Stream controller logs
kubectl logs -f deployment/controller -n meeting-bot

# Exec into controller pod
kubectl exec -it deployment/controller -n meeting-bot -- /bin/bash

# Check what jobs are created
kubectl get jobs -n meeting-bot

# View job logs
kubectl logs job/<job-name> -n meeting-bot -c manager
```

## Cleanup

```bash
# Delete the KinD cluster
kind delete cluster --name meeting-bot-dev
```

## Troubleshooting

### Controller can't reach Firebase emulator

On Linux, the `host.docker.internal` hostname may not work. The setup script automatically detects this and uses the docker0 bridge IP instead.

If you still have issues:

```bash
# Find your docker bridge IP
ip -4 addr show docker0

# Manually update FIRESTORE_EMULATOR_HOST in the ConfigMap
kubectl edit configmap controller-config -n meeting-bot
```

### Images not loading

```bash
# Manually load an image
docker build -t meeting-bot-controller:local -f Dockerfile.controller .
kind load docker-image meeting-bot-controller:local --name meeting-bot-dev
```
