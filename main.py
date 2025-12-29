#!/usr/bin/env python3
"""
Insert into your D1 table: wbs(WBS_ELEMENT_CDE, WBS_ELEMENT_DESC)

Env vars (loaded from .env file):
  CF_ACCOUNT_ID
  CF_D1_DATABASE_ID
  CF_API_TOKEN
"""

from __future__ import annotations

import json
import os
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any, Sequence
from dotenv import load_dotenv

# Load environment variables from .env file
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

    def query(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"sql": sql}
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


def create_wbs_table(d1: D1Client) -> dict[str, Any]:
    """Create the wbs table if it doesn't exist"""
    sql = """
    CREATE TABLE IF NOT EXISTS "wbs" (
        "WBS_ELEMENT_CDE" TEXT PRIMARY KEY,
        "WBS_ELEMENT_DESC" TEXT
    );
    """
    return d1.query(sql)


def list_tables(d1: D1Client) -> dict[str, Any]:
    """List all tables in the database"""
    sql = "SELECT name FROM sqlite_master WHERE type='table';"
    return d1.query(sql)


def get_table_info(d1: D1Client, table_name: str = "wbs") -> dict[str, Any]:
    """Get information about the table structure"""
    sql = f"PRAGMA table_info('{table_name}');"
    return d1.query(sql)


def drop_table(d1: D1Client, table_name: str = "wbs") -> dict[str, Any]:
    """Drop a table (for testing purposes)"""
    sql = f"DROP TABLE IF EXISTS '{table_name}';"
    return d1.query(sql)


def insert_wbs(d1: D1Client, wbs_element_cde: str, wbs_element_desc: str) -> dict[str, Any]:
    # If WBS_ELEMENT_CDE is your primary key and may already exist, consider INSERT OR REPLACE
    sql = """
    INSERT INTO "wbs" ("WBS_ELEMENT_CDE", "WBS_ELEMENT_DESC")
    VALUES (?, ?);
    """
    return d1.query(sql, params=[wbs_element_cde, wbs_element_desc])


def upsert_wbs(d1: D1Client, wbs_element_cde: str, wbs_element_desc: str) -> dict[str, Any]:
    # Safe “overwrite if exists” approach (common when CDE is the key)
    sql = """
    INSERT OR REPLACE INTO "wbs" ("WBS_ELEMENT_CDE", "WBS_ELEMENT_DESC")
    VALUES (?, ?);
    """
    return d1.query(sql, params=[wbs_element_cde, wbs_element_desc])


def main() -> int:
    try:
        d1 = D1Client(D1Config.from_env())
        print("✓ Successfully loaded configuration from .env file")
        print(f"Account ID: {d1._cfg.account_id[:8]}...")
        print(f"Database ID: {d1._cfg.database_id[:8]}...")
        print(f"API Token: {d1._cfg.api_token[:8]}...")
    except RuntimeError as e:
        print(f"❌ Configuration error: {e}")
        return 1

    # List all tables first
    try:
        print("\n📋 Listing all tables in database...")
        tables = list_tables(d1)
        print("All tables:")
        print(json.dumps(tables, indent=2))
    except RuntimeError as e:
        print(f"❌ Failed to list tables: {e}")
        
    # Check if wbs table exists and its structure
    try:
        print("\n🔍 Checking 'wbs' table structure...")
        table_info = get_table_info(d1, "wbs")
        print("WBS table info:")
        print(json.dumps(table_info, indent=2))
    except RuntimeError as e:
        print(f"❌ Failed to get table info: {e}")
        
    # Drop and recreate the table to ensure it has the right structure
    try:
        print("\n🗑️ Dropping existing table...")
        drop_resp = drop_table(d1, "wbs")
        print("Drop result:")
        print(json.dumps(drop_resp, indent=2))
        
        print("\n📝 Creating fresh table...")
        create_resp = create_wbs_table(d1)
        print("✓ Table creation completed")
        print(json.dumps(create_resp, indent=2))
        
        print("\n🔍 Checking new table structure...")
        table_info = get_table_info(d1, "wbs")
        print("New table info:")
        print(json.dumps(table_info, indent=2))
        
    except RuntimeError as e:
        print(f"❌ Failed to recreate table: {e}")
        return 1

    # Example insert
    try:
        print("\n➕ Attempting to insert WBS element...")
        resp = insert_wbs(d1, wbs_element_cde="1.2.3.4", wbs_element_desc="Site prep / mobilization")
        print("✓ Successfully inserted WBS element")
        print(json.dumps(resp, indent=2))
    except RuntimeError as e:
        print(f"❌ Database operation failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
