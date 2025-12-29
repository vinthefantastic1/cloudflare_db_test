#!/usr/bin/env python3
"""Debug script to check D1 response format"""

from list_wbs import D1Config, D1Client, WBSLister
import json

# Initialize
d1_config = D1Config.from_env()
d1_client = D1Client(d1_config)
wbs_lister = WBSLister(d1_client)

print("🔍 Testing database query response format...")

# Test a simple query
result = d1_client.query("SELECT * FROM wbs LIMIT 2")
print(f"\n📋 Raw query result:")
print(json.dumps(result, indent=2))

# Test the extraction method
extracted = wbs_lister._extract_results(result)
print(f"\n📋 Extracted results:")
print(json.dumps(extracted, indent=2))

print(f"\n📊 Number of extracted results: {len(extracted)}")
if extracted:
    print(f"📋 First item keys: {list(extracted[0].keys())}")
    print(f"📋 First item: {extracted[0]}")