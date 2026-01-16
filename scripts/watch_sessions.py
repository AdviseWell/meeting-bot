#!/usr/bin/env python3
"""
Watch for new meeting sessions and monitor them.
Run this before creating a test meeting.
"""

import os
import time
from datetime import datetime, timezone, timedelta
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

PROJECT_ID = "aw-development-7226"
ORG_ID = "advisewell"

MATT_USER_ID = "Mw6Awnh1WtN7631PrSYyGXlpozz1"
CLINTON_USER_ID = "qWsfR283u4Sck6fGF33dvFa3W0r2"


def get_db():
    creds_path = os.path.expanduser(
        "~/.config/gcloud/application_default_credentials.json"
    )
    if os.path.exists(creds_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    return firestore.Client(project=PROJECT_ID, database="(default)")


def format_time(dt):
    if hasattr(dt, "timestamp"):
        return dt.strftime("%H:%M:%S")
    return str(dt)


def get_recent_sessions(db, since_minutes: int = 5):
    """Get sessions created in the last N minutes."""
    sessions_ref = db.collection(f"organizations/{ORG_ID}/meeting_sessions")
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
    
    sessions = list(
        sessions_ref.where(filter=FieldFilter("created_at", ">=", cutoff))
        .order_by("created_at", direction=firestore.Query.DESCENDING)
        .limit(10)
        .stream()
    )
    
    results = []
    for s in sessions:
        data = s.to_dict()
        
        # Get subscribers
        subs = list(s.reference.collection("subscribers").stream())
        sub_list = [sub.id for sub in subs]
        
        has_matt = MATT_USER_ID in sub_list
        has_clinton = CLINTON_USER_ID in sub_list
        
        results.append({
            "id": s.id[:16] + "...",
            "full_id": s.id,
            "status": data.get("status"),
            "title": data.get("meeting_title", "Unknown")[:30],
            "created": format_time(data.get("created_at")),
            "subs": len(sub_list),
            "matt": has_matt,
            "clinton": has_clinton,
        })
    
    return results


def watch_sessions():
    """Watch for new sessions."""
    db = get_db()
    seen = set()
    
    print("=" * 80)
    print("WATCHING FOR NEW MEETING SESSIONS")
    print("=" * 80)
    print(f"\nStarted at: {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")
    print("Looking for sessions from the last 5 minutes...")
    print("Press Ctrl+C to stop\n")
    
    while True:
        sessions = get_recent_sessions(db, since_minutes=10)
        
        for s in sessions:
            if s["full_id"] not in seen:
                seen.add(s["full_id"])
                
                matt_icon = "✅" if s["matt"] else "❌"
                clinton_icon = "✅" if s["clinton"] else "❌"
                
                print("-" * 80)
                print(f"🆕 NEW SESSION: {s['id']}")
                print(f"   Title: {s['title']}")
                print(f"   Status: {s['status']}")
                print(f"   Created: {s['created']}")
                print(f"   Subscribers: {s['subs']}")
                print(f"   Matt: {matt_icon}  Clinton: {clinton_icon}")
                print()
        
        # Update status of seen sessions
        for s in sessions:
            if s["full_id"] in seen:
                status = s["status"]
                if status in ["complete", "failed"]:
                    print(f"📢 Session {s['id']} is now {status}")
        
        time.sleep(5)


if __name__ == "__main__":
    try:
        watch_sessions()
    except KeyboardInterrupt:
        print("\n\nStopped watching.")
