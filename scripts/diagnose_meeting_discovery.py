git chrck#!/usr/bin/env python3
"""
Diagnostic script to investigate why meetings aren't being discovered.

This script checks:
1. The meeting exists in Firestore
2. The meeting has the required fields
3. The meeting's start time relative to the discovery window
4. The meeting's status field

Usage:
    # Check for a specific user's meetings
    python scripts/diagnose_meeting_discovery.py --project=aw-production-4df9 --user-email=matt@advisewell.co

    # Check for a specific meeting by name
    python scripts/diagnose_meeting_discovery.py --project=aw-production-4df9 --meeting-name="Test #2"

    # Check all meetings for an org
    python scripts/diagnose_meeting_discovery.py --project=aw-production-4df9 --org-name="AdviseWell"
"""

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict, List

try:
    from google.cloud import firestore
except ImportError:
    print("ERROR: google-cloud-firestore not installed")
    print("Run: pip install google-cloud-firestore")
    sys.exit(1)


def parse_start_time(start_value: Any) -> Optional[datetime]:
    """Parse start time from various formats (same logic as controller)."""
    if start_value is None:
        return None

    if isinstance(start_value, datetime):
        if start_value.tzinfo is None:
            return start_value.replace(tzinfo=timezone.utc)
        return start_value

    # Handle Firestore Timestamp
    if hasattr(start_value, "timestamp"):
        return datetime.fromtimestamp(start_value.timestamp(), tz=timezone.utc)

    if isinstance(start_value, str):
        try:
            dt = datetime.fromisoformat(start_value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            return None

    return None


def format_timedelta(td: timedelta) -> str:
    """Format timedelta for display."""
    total_seconds = int(td.total_seconds())
    sign = "-" if total_seconds < 0 else "+"
    total_seconds = abs(total_seconds)
    
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    
    if hours:
        return f"{sign}{hours}h {minutes}m {seconds}s"
    elif minutes:
        return f"{sign}{minutes}m {seconds}s"
    else:
        return f"{sign}{seconds}s"


def check_meeting_discovery_readiness(data: Dict[str, Any], doc_path: str) -> Dict[str, Any]:
    """Check if a meeting document meets discovery requirements."""
    now = datetime.now(timezone.utc)
    
    # Controller's discovery window
    target_time = now + timedelta(minutes=8)
    window_start = target_time - timedelta(seconds=30)
    window_end = target_time + timedelta(seconds=30)
    
    result = {
        "path": doc_path,
        "issues": [],
        "warnings": [],
        "info": [],
    }
    
    # Check meeting_url / join_url
    meeting_url = data.get("meeting_url") or data.get("meetingUrl") or data.get("join_url")
    if meeting_url:
        result["info"].append(f"✅ Has meeting URL: {meeting_url[:80]}...")
    else:
        result["issues"].append("❌ MISSING: No meeting_url, meetingUrl, or join_url field")
    
    # Check status
    status = data.get("status")
    if status == "scheduled":
        result["info"].append(f"✅ Status is 'scheduled'")
    elif status:
        result["issues"].append(f"❌ Status is '{status}' (expected 'scheduled')")
    else:
        result["issues"].append("❌ MISSING: No 'status' field")
    
    # Check bot_instance_id (should NOT exist for discovery)
    bot_instance_id = data.get("bot_instance_id")
    if bot_instance_id:
        result["warnings"].append(f"⚠️  Already has bot_instance_id: {bot_instance_id}")
        result["warnings"].append("   (Controller will skip this meeting)")
    else:
        result["info"].append("✅ No bot_instance_id (eligible for discovery)")
    
    # Check start time
    start_value = data.get("start")
    if start_value is None:
        result["issues"].append("❌ MISSING: No 'start' field")
    else:
        parsed_start = parse_start_time(start_value)
        if parsed_start is None:
            result["issues"].append(f"❌ Cannot parse 'start' field: {start_value!r}")
        else:
            result["info"].append(f"📅 Start time: {parsed_start.isoformat()}")
            
            # Check if in discovery window
            time_until_start = parsed_start - now
            result["info"].append(f"   Time until meeting: {format_timedelta(time_until_start)}")
            
            if parsed_start < now:
                result["warnings"].append(f"⚠️  Meeting start time is in the PAST ({format_timedelta(time_until_start)})")
            elif window_start <= parsed_start <= window_end:
                result["info"].append(f"✅ START TIME IS IN DISCOVERY WINDOW!")
                result["info"].append(f"   Window: {window_start.isoformat()} to {window_end.isoformat()}")
            else:
                time_until_window = window_start - parsed_start
                if time_until_window.total_seconds() > 0:
                    result["warnings"].append(f"⏰ Meeting will be discovered in {format_timedelta(time_until_window)}")
                else:
                    result["warnings"].append(f"⏰ Meeting window passed {format_timedelta(-time_until_window)} ago")
                result["info"].append(f"   Discovery window: {window_start.isoformat()} to {window_end.isoformat()}")
    
    # Check user_id
    user_id = data.get("user_id") or data.get("userId")
    if user_id:
        result["info"].append(f"👤 User ID: {user_id}")
    else:
        result["warnings"].append("⚠️  No user_id field")
    
    # Check organization_id
    org_id = data.get("organization_id") or data.get("organizationId") or data.get("teamId")
    if org_id:
        result["info"].append(f"🏢 Org ID: {org_id}")
    else:
        result["warnings"].append("⚠️  No organization_id field")
    
    # Check ai_assistant / auto_join settings
    ai_enabled = data.get("ai_assistant_enabled") or data.get("ai_assistant") or data.get("auto_join")
    if ai_enabled:
        result["info"].append(f"🤖 AI/Auto-join enabled: {ai_enabled}")
    
    return result


def find_org_by_name(db: firestore.Client, org_name: str) -> Optional[str]:
    """Find organization ID by name."""
    orgs = db.collection("organizations").where("name", "==", org_name).limit(1).stream()
    for org in orgs:
        return org.id
    return None


def find_user_by_email(db: firestore.Client, email: str) -> Optional[Dict[str, Any]]:
    """Find user by email."""
    users = db.collection_group("users").where("email", "==", email).limit(1).stream()
    for user in users:
        return {"id": user.id, "path": user.reference.path, "data": user.to_dict()}
    return None


def main():
    parser = argparse.ArgumentParser(description="Diagnose meeting discovery issues")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--database", default="(default)", help="Firestore database")
    parser.add_argument("--user-email", help="Find meetings for this user email")
    parser.add_argument("--meeting-name", help="Find meetings with this name/subject")
    parser.add_argument("--org-name", help="Find meetings for this organization name")
    parser.add_argument("--org-id", help="Find meetings for this organization ID")
    parser.add_argument("--show-all-fields", action="store_true", help="Show all document fields")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("MEETING DISCOVERY DIAGNOSTIC")
    print("=" * 80)
    print(f"Project: {args.project}")
    print(f"Database: {args.database}")
    print(f"Current time: {datetime.now(timezone.utc).isoformat()}")
    print()
    
    # Initialize Firestore
    db = firestore.Client(project=args.project, database=args.database)
    
    # Find org ID if needed
    org_id = args.org_id
    if args.org_name and not org_id:
        org_id = find_org_by_name(db, args.org_name)
        if org_id:
            print(f"Found organization '{args.org_name}': {org_id}")
        else:
            print(f"❌ Organization '{args.org_name}' not found")
            return
    
    # Find user if needed
    user_info = None
    if args.user_email:
        user_info = find_user_by_email(db, args.user_email)
        if user_info:
            print(f"Found user '{args.user_email}':")
            print(f"  Path: {user_info['path']}")
            print(f"  ID: {user_info['id']}")
            # Extract org from path if not specified
            if not org_id and "/organizations/" in user_info["path"]:
                parts = user_info["path"].split("/")
                org_idx = parts.index("organizations") + 1
                if org_idx < len(parts):
                    org_id = parts[org_idx]
                    print(f"  Org ID (from path): {org_id}")
        else:
            print(f"❌ User '{args.user_email}' not found")
            return
    
    print()
    print("=" * 80)
    print("SCANNING MEETINGS COLLECTION_GROUP")
    print("=" * 80)
    
    # Query meetings using collection_group (like the controller does)
    meetings_query = db.collection_group("meetings")
    
    # Add filters based on args
    if args.meeting_name:
        meetings_query = meetings_query.where("subject", "==", args.meeting_name)
    
    # We can't filter by org in collection_group easily, so we'll do it post-query
    
    found_meetings = []
    for doc in meetings_query.limit(100).stream():
        data = doc.to_dict() or {}
        path = doc.reference.path
        
        # Filter by org if specified
        if org_id:
            if f"organizations/{org_id}/" not in path:
                continue
        
        # Filter by user if specified
        if user_info:
            user_id_in_doc = data.get("user_id") or data.get("userId")
            if user_id_in_doc != user_info["id"]:
                # Also check path
                if f"/users/{user_info['id']}/" not in path:
                    continue
        
        # Filter by meeting name if specified
        if args.meeting_name:
            subject = data.get("subject") or data.get("name") or data.get("title")
            if subject != args.meeting_name:
                continue
        
        found_meetings.append((doc, path, data))
    
    if not found_meetings:
        print("❌ No meetings found matching criteria")
        print()
        print("Try checking:")
        print("  1. Is the meeting in a 'meetings' subcollection?")
        print("  2. Does the meeting document exist?")
        print("  3. Are the filter criteria correct?")
        return
    
    print(f"Found {len(found_meetings)} meeting(s)")
    print()
    
    for doc, path, data in found_meetings:
        print("-" * 80)
        result = check_meeting_discovery_readiness(data, path)
        
        print(f"📄 Document: {path}")
        print(f"   ID: {doc.id}")
        
        # Show subject/name if available
        subject = data.get("subject") or data.get("name") or data.get("title")
        if subject:
            print(f"   Subject: {subject}")
        
        print()
        
        if result["issues"]:
            print("ISSUES (will prevent discovery):")
            for issue in result["issues"]:
                print(f"  {issue}")
            print()
        
        if result["warnings"]:
            print("WARNINGS:")
            for warning in result["warnings"]:
                print(f"  {warning}")
            print()
        
        if result["info"]:
            print("INFO:")
            for info in result["info"]:
                print(f"  {info}")
            print()
        
        if args.show_all_fields:
            print("ALL FIELDS:")
            for key, value in sorted(data.items()):
                print(f"  {key}: {value!r}")
            print()
    
    print("=" * 80)
    print("CONTROLLER DISCOVERY WINDOW INFO")
    print("=" * 80)
    now = datetime.now(timezone.utc)
    target_time = now + timedelta(minutes=8)
    window_start = target_time - timedelta(seconds=30)
    window_end = target_time + timedelta(seconds=30)
    print(f"Current time:      {now.isoformat()}")
    print(f"Discovery window:  {window_start.isoformat()} to {window_end.isoformat()}")
    print(f"                   (meetings starting 7.5 to 8.5 minutes from now)")
    print()
    print("For a meeting to be discovered by time-window scanning:")
    print("  1. 'start' field must be a Timestamp or ISO string")
    print("  2. 'start' must fall within the discovery window")
    print("  3. 'status' must be 'scheduled' (or configured status)")
    print("  4. Must have 'meeting_url', 'meetingUrl', or 'join_url'")
    print("  5. Must NOT have 'bot_instance_id' already set")
    print()
    print("Alternatively, meetings can be discovered via queued sessions:")
    print("  - A document in 'meeting_sessions' with status='queued'")


if __name__ == "__main__":
    main()
