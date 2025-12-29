#!/usr/bin/env python3
"""
Quick verification script for wbs_2 table
"""

from __future__ import annotations
import json
import os
import urllib.request
import urllib.error
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class D1Config:
    account_id: str
    database_id: str
    api_token: str

    @staticmethod
    def from_env() -> "D1Config":
        missing = []
        account_id = os.getenv("CF_ACCOUNT_ID")
        database_id = os.getenv("CF_D1_DATABASE_ID")
        api_token = os.getenv("CF_API_TOKEN")

        if not account_id:
            missing.append("CF_ACCOUNT_ID")
        if not database_id:
            missing.append("CF_D1_DATABASE_ID")
        if not api_token:
            missing.append("CF_API_TOKEN")

        if missing:
            raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

        return D1Config(account_id=account_id, database_id=database_id, api_token=api_token)

class D1Client:
    def __init__(self, cfg: D1Config, timeout_seconds: int = 30) -> None:
        self._cfg = cfg
        self._timeout_seconds = timeout_seconds

    @property
    def _endpoint(self) -> str:
        return (
            f"https://api.cloudflare.com/client/v4/accounts/"
            f"{self._cfg.account_id}/d1/database/{self._cfg.database_id}/query"
        )

    def query(self, sql: str, params=None):
        payload = {"sql": sql}
        if params is not None:
            payload["params"] = list(params)

        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._endpoint,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._cfg.api_token}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=self._timeout_seconds) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {e.code} {e.reason}: {raw}") from e

        if not data.get("success", False):
            raise RuntimeError(f"D1 query failed: {data.get('errors')}")
        return data

def main():
    print("🔍 Checking wbs_2 table...")
    
    try:
        config = D1Config.from_env()
        client = D1Client(config)
        
        # Check if table exists and count records
        result = client.query("SELECT COUNT(*) as count FROM wbs_2")
        count = result["result"][0]["results"][0]["count"]
        print(f"📊 Total records in wbs_2: {count:,}")
        
        if count > 0:
            # Show sample data
            sample_result = client.query("SELECT * FROM wbs_2 LIMIT 3")
            samples = sample_result["result"][0]["results"]
            
            print(f"\n📋 Sample records:")
            for i, record in enumerate(samples, 1):
                print(f"\nRecord {i}:")
                for key, value in record.items():
                    if key not in ['created_at', 'updated_at']:  # Skip auto-generated timestamps
                        print(f"  {key}: {value}")
            
            # Show column info
            columns_result = client.query("PRAGMA table_info(wbs_2)")
            columns = columns_result["result"][0]["results"]
            print(f"\n📝 Table schema ({len(columns)} columns):")
            for col in columns:
                print(f"  {col['name']}: {col['type']}")
        
        print("\n✅ Verification complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()