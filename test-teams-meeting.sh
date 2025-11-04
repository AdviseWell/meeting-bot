#!/bin/bash

# Test script for joining a Microsoft Teams meeting locally
# This will test the early recording and end detection fixes

MEETING_URL="https://teams.microsoft.com/meet/4833107356442?p=G0SmGWV5RKT2MXBGfc"
BOT_NAME="Test Bot"
TEAM_ID="test-team-123"
USER_ID="test-user-456"
BOT_ID="test-bot-789"
TIMEZONE="America/Los_Angeles"
BEARER_TOKEN="test-token-123"

echo "🤖 Testing Microsoft Teams meeting bot..."
echo "Meeting URL: $MEETING_URL"
echo ""

# Build and start the development container
echo "📦 Building and starting containers..."
docker-compose up -d meeting-bot

# Wait for the service to be ready
echo "⏳ Waiting for service to be ready..."
sleep 5

# Check if service is running
if ! curl -s http://localhost:5001/health > /dev/null 2>&1; then
    echo "❌ Service not ready. Checking logs..."
    docker-compose logs --tail=50 meeting-bot
    exit 1
fi

echo "✅ Service is ready!"
echo ""

# Send join request
echo "🚀 Sending join request..."
RESPONSE=$(curl -s -X POST http://localhost:5001/microsoft/join \
  -H "Content-Type: application/json" \
  -d "{
    \"url\": \"$MEETING_URL\",
    \"name\": \"$BOT_NAME\",
    \"teamId\": \"$TEAM_ID\",
    \"userId\": \"$USER_ID\",
    \"botId\": \"$BOT_ID\",
    \"timezone\": \"$TIMEZONE\",
    \"bearerToken\": \"$BEARER_TOKEN\"
  }")

echo "Response: $RESPONSE"
echo ""

# Show logs
echo "📋 Showing logs (press Ctrl+C to stop)..."
echo "---"
docker-compose logs -f meeting-bot

