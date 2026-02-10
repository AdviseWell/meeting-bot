#!/usr/bin/env python3
"""Quick check of collection structure."""
from google.cloud import firestore

db = firestore.Client(project="aw-production-4df9")

print("=== Checking collection structure for advisewell org ===")
print()

# Check direct org collections
org_ref = db.collection("organizations").document("advisewell")
print("Collections under organizations/advisewell:")
for coll in org_ref.collections():
    print(f"  - {coll.id}")

# Check if there are user-level meetings
print()
print("=== Checking for user-level meetings ===")

# Look for users collection and their meetings
users_ref = org_ref.collection("users")
for user_doc in users_ref.limit(10).stream():
    print(f"User: {user_doc.id}")
    user_data = user_doc.to_dict() or {}
    print(f'  Email: {user_data.get("email")}')
    # Check for meetings collection under user
    meetings = list(user_doc.reference.collection("meetings").limit(5).stream())
    if meetings:
        print(f"  Meetings under user: {len(meetings)}")
        for m in meetings[:3]:
            mdata = m.to_dict() or {}
            print(f'    - {mdata.get("title")}')
    print()
