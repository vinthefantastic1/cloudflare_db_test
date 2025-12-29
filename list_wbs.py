#!/usr/bin/env python3
"""
WBS Database Lister
Lists all WBS items from the Cloudflare D1 database

Required packages:
- python-dotenv
"""

from __future__ import annotations

import json
import os
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any, Sequence, Optional
from dotenv import load_dotenv
import datetime

# Load environment variables from .env file
load_dotenv()


@dataclass(frozen=True)
class D1Config:
    """Configuration for Cloudflare D1 database connection"""
    account_id: str
    database_id: str
    api_token: str

    @staticmethod
    def from_env() -> "D1Config":
        """Load configuration from environment variables"""
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
    """Client for interacting with Cloudflare D1 database"""
    
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
        """Execute a SQL query against the D1 database"""
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


class WBSLister:
    """Handles listing and displaying WBS data from D1 database"""
    
    def __init__(self, d1_client: D1Client):
        self.d1 = d1_client
        
    def _extract_results(self, d1_response: dict[str, Any]) -> list[dict]:
        """Extract actual results from D1 response format"""
        result_data = d1_response.get("result", [])
        if result_data and len(result_data) > 0:
            return result_data[0].get("results", [])
        return []
        
    def get_all_wbs_items(self, limit: Optional[int] = None, offset: int = 0) -> list[dict]:
        """Get all WBS items from the database"""
        sql = "SELECT * FROM wbs ORDER BY WBS_ELEMENT_CDE"
        
        if limit:
            sql += f" LIMIT {limit} OFFSET {offset}"
            
        result = self.d1.query(sql)
        return self._extract_results(result)
        
    def count_wbs_items(self) -> int:
        """Get the total count of WBS items"""
        result = self.d1.query("SELECT COUNT(*) as total FROM wbs")
        
        # Extract count from D1 response format
        result_data = result.get("result", [])
        if result_data and len(result_data) > 0:
            # D1 wraps the actual results in a 'results' key
            actual_results = result_data[0].get("results", [])
            if actual_results and len(actual_results) > 0:
                return actual_results[0].get("total", 0)
        
        return 0
        
    def search_wbs_items(self, search_term: str) -> list[dict]:
        """Search for WBS items by code or description"""
        sql = """
        SELECT * FROM wbs 
        WHERE WBS_ELEMENT_CDE LIKE ? OR WBS_ELEMENT_DESC LIKE ?
        ORDER BY WBS_ELEMENT_CDE
        """
        search_pattern = f"%{search_term}%"
        result = self.d1.query(sql, params=[search_pattern, search_pattern])
        return self._extract_results(result)
        
    def get_table_info(self) -> dict[str, Any]:
        """Get information about the WBS table structure"""
        return self.d1.query("PRAGMA table_info('wbs');")
        
    def check_table_exists(self) -> bool:
        """Check if the WBS table exists"""
        try:
            result = self.d1.query("SELECT name FROM sqlite_master WHERE type='table' AND name='wbs';")
            tables = result.get("result", [])
            return len(tables) > 0
        except Exception:
            return False
        
    def display_wbs_items(self, items: list[dict], title: str = "WBS Items") -> None:
        """Display WBS items in a formatted table"""
        if not items:
            print("📝 No WBS items found.")
            return
            
        print(f"\n📋 {title}")
        print("="*80)
        
        # Display header
        print(f"{'#':<4} {'WBS Code':<25} {'Description':<35} {'Created':<14}")
        print("-"*80)
        
        # Display items
        for i, item in enumerate(items, 1):
            code = item.get("WBS_ELEMENT_CDE", "N/A")[:24]
            desc = item.get("WBS_ELEMENT_DESC", "N/A")[:34]
            created = item.get("CREATE_DATE", "N/A")
            
            # Format date if it's a timestamp
            if created != "N/A" and created:
                try:
                    # Handle different date formats
                    if isinstance(created, str) and "T" in created:
                        created = created.split("T")[0]  # Just the date part
                    elif len(str(created)) > 10:
                        created = str(created)[:10]
                except:
                    created = str(created)[:13]
            
            print(f"{i:<4} {code:<25} {desc:<35} {created:<14}")
            
        print("-"*80)
        print(f"Total items displayed: {len(items)}")


def main() -> int:
    """Main execution function"""
    print("📋 WBS Database Lister")
    print("="*50)
    
    # Load configuration
    try:
        d1_config = D1Config.from_env()
        print("✓ Successfully loaded D1 configuration")
        print(f"  Account ID: {d1_config.account_id[:8]}...")
        print(f"  Database ID: {d1_config.database_id[:8]}...")
    except RuntimeError as e:
        print(f"❌ Configuration error: {e}")
        return 1
    
    # Initialize clients
    d1_client = D1Client(d1_config)
    wbs_lister = WBSLister(d1_client)
    
    # Check database connection and table existence
    try:
        print("\n🔍 Checking database connection...")
        
        # First check if we can connect to the database
        test_result = d1_client.query("SELECT 1 as test;")
        print("✓ Database connection successful")
        
        # Check if the WBS table exists
        print("\n🔍 Checking if 'wbs' table exists...")
        if not wbs_lister.check_table_exists():
            print("❌ The 'wbs' table does not exist in the database.")
            print("💡 You may need to create the table first or run the wbs_loader.py script.")
            return 1
            
        print("✓ Table 'wbs' exists")
        
        # Get table structure info
        print("\n🔍 Getting table information...")
        table_info = wbs_lister.get_table_info()
        
        # Extract columns from the D1 response format
        result_data = table_info.get("result", [])
        if result_data and len(result_data) > 0:
            # D1 wraps the actual results in a 'results' key
            actual_results = result_data[0].get("results", [])
            columns = [col.get("name", "unknown") for col in actual_results]
        else:
            columns = []
                
        print(f"✓ Table columns: {columns}")
        
        # Get total count
        print("\n🔍 Counting records...")
        total_count = wbs_lister.count_wbs_items()
        print(f"📊 Total WBS items in database: {total_count}")
        
        if total_count == 0:
            print("📝 No WBS items found in the database.")
            print("💡 You may need to load data using the wbs_loader.py script.")
            return 0
            
    except RuntimeError as e:
        print(f"❌ Database API error: {e}")
        print("💡 Check your Cloudflare credentials and database ID")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}: {e}")
        import traceback
        print("\n📋 Full error details:")
        traceback.print_exc()
        return 1
    
    # Interactive menu
    while True:
        print(f"\n📋 AVAILABLE ACTIONS:")
        print("  1. List all WBS items")
        print("  2. List first N items")
        print("  3. Search WBS items")
        print("  4. Show database statistics")
        print("  5. Exit")
        
        choice = input(f"\nEnter your choice (1-5): ").strip()
        
        try:
            if choice == "1":
                print(f"\n🔄 Loading all {total_count} WBS items...")
                items = wbs_lister.get_all_wbs_items()
                wbs_lister.display_wbs_items(items, f"All WBS Items ({len(items)})")
                
            elif choice == "2":
                try:
                    limit = int(input("How many items to display? "))
                    print(f"\n🔄 Loading first {limit} WBS items...")
                    items = wbs_lister.get_all_wbs_items(limit=limit)
                    wbs_lister.display_wbs_items(items, f"First {len(items)} WBS Items")
                except ValueError:
                    print("❌ Please enter a valid number")
                    
            elif choice == "3":
                search_term = input("Enter search term (code or description): ").strip()
                if search_term:
                    print(f"\n🔍 Searching for '{search_term}'...")
                    items = wbs_lister.search_wbs_items(search_term)
                    wbs_lister.display_wbs_items(items, f"Search Results for '{search_term}' ({len(items)})")
                else:
                    print("❌ Please enter a search term")
                    
            elif choice == "4":
                print(f"\n📊 DATABASE STATISTICS")
                print("="*40)
                print(f"Total WBS Items: {total_count}")
                print(f"Table Columns: {len(columns)}")
                print(f"Column Names: {', '.join(columns)}")
                
                # Get a sample record to show data types
                if total_count > 0:
                    sample_items = wbs_lister.get_all_wbs_items(limit=1)
                    if sample_items:
                        sample_item = sample_items[0]
                        print(f"\nSample Record:")
                        for key, value in sample_item.items():
                            value_str = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                            print(f"  {key}: {value_str}")
                        
            elif choice == "5":
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please enter 1-5.")
                
        except Exception as e:
            print(f"❌ Operation failed: {e}")
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())