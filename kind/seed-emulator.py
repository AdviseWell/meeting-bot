#!/usr/bin/env python3
"""
Seed Firebase emulator with test meeting data from production.

This script copies a subset of meetings from production Firestore
to the local Firebase emulator for testing.

Usage:
    python seed-emulator.py --org advisewell --limit 10
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

# Ensure we can import from the controller
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    parser = argparse.ArgumentParser(description="Seed Firebase emulator with test data")
    parser.add_argument("--org", default="advisewell", help="Organization ID to copy from")
    parser.add_argument("--limit", type=int, default=10, help="Number of meetings to copy")
    parser.add_argument("--days-ahead", type=int, default=1, help="Copy meetings within N days")
    parser.add_argument("--emulator-host", default="localhost:8080", help="Firestore emulator host")
    parser.add_argument("--prod-project", default="aw-development-7226", help="Production GCP project")
    args = parser.parse_args()

    # Set emulator environment
    os.environ["FIRESTORE_EMULATOR_HOST"] = args.emulator_host
    
    from google.cloud import firestore
    
    print(f"Connecting to production Firestore: {args.prod_project}")
    
    # Connect to production (without emulator env)
    del os.environ["FIRESTORE_EMULATOR_HOST"]
    prod_db = firestore.Client(project=args.prod_project)
    
    # Get upcoming meetings
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=args.days_ahead)
    
    print(f"Fetching meetings from {now.isoformat()} to {cutoff.isoformat()}")
    
    meetings_ref = prod_db.collection("organizations").document(args.org).collection("meetings")
    query = meetings_ref.where("start", ">=", now).where("start", "<=", cutoff).limit(args.limit)
    
    meetings = list(query.stream())
    print(f"Found {len(meetings)} meetings")
    
    # Connect to emulator
    os.environ["FIRESTORE_EMULATOR_HOST"] = args.emulator_host
    emulator_db = firestore.Client(project="demo-advisewell")
    
    # Create org document
    org_ref = emulator_db.collection("organizations").document(args.org)
    org_ref.set({
        "name": args.org.title(),
        "bot_display_name": "Test Bot",
        "created_at": now,
    }, merge=True)
    
    # Copy meetings
    for meeting_doc in meetings:
        meeting_data = meeting_doc.to_dict()
        meeting_id = meeting_doc.id
        
        print(f"  Copying: {meeting_data.get('subject', 'No Subject')} ({meeting_id})")
        
        # Reset status for testing
        meeting_data["status"] = "scheduled"
        meeting_data["bot_status"] = None
        meeting_data["session_status"] = None
        meeting_data["bot_instance_id"] = None
        
        emulator_db.collection("organizations").document(args.org).collection("meetings").document(meeting_id).set(meeting_data)
    
    print(f"\n✅ Seeded {len(meetings)} meetings to emulator")
    print(f"   Emulator UI: http://localhost:4000/firestore")

if __name__ == "__main__":
    main()
