#!/usr/bin/env python3
"""
WBS Excel to D1 Database Loader
Loads WBS data from Excel file and inserts into Cloudflare D1 database

Required packages:
- pandas
- openpyxl
- python-dotenv
"""

from __future__ import annotations

import json
import os
import urllib.request
import urllib.error
import pandas as pd
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence, Optional
from dotenv import load_dotenv
import datetime
import time

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


class WBSExcelLoader:
    """Handles loading WBS data from Excel files"""
    
    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self.data: Optional[pd.DataFrame] = None
        
    def _make_json_serializable(self, value: Any) -> Any:
        """Convert value to JSON serializable format"""
        if pd.isna(value) or value is None:
            return None
        elif isinstance(value, (pd.Timestamp, datetime.datetime)):
            return value.isoformat()
        elif isinstance(value, datetime.date):
            return value.isoformat()
        elif isinstance(value, datetime.time):
            return value.isoformat()
        elif isinstance(value, pd.Timedelta):
            return str(value)
        elif hasattr(value, 'item'):  # pandas scalar types
            return value.item()
        elif isinstance(value, (int, float, str, bool)):
            return value
        else:
            return str(value)
        
    def load_data(self, nrows: Optional[int] = None) -> pd.DataFrame:
        """Load the Excel file and return the DataFrame"""
        try:
            if not self.file_path.exists():
                raise FileNotFoundError(f"Excel file not found: {self.file_path}")
                
            print(f"📖 Reading Excel file: {self.file_path}")
            if nrows:
                print(f"   Loading first {nrows} rows...")
                
            # Load only the first 2 columns (WBS_ELEMENT_CDE and WBS_ELEMENT_DESC)
            print("   Loading first 2 columns only...")
            self.data = pd.read_excel(self.file_path, nrows=nrows, usecols=[0, 1])
            print(f"✓ Successfully loaded {len(self.data)} rows and {len(self.data.columns)} columns")
            
            # Clean column names (remove extra spaces, normalize)
            self.data.columns = self.data.columns.str.strip()
            
            return self.data
            
        except Exception as e:
            print(f"❌ Error reading Excel file: {e}")
            raise
    
    def get_wbs_records(self) -> list[dict[str, Any]]:
        """Extract WBS records as dictionaries"""
        if self.data is None:
            raise RuntimeError("No data loaded. Call load_data() first.")
            
        records = []
        for _, row in self.data.iterrows():
            record = {}
            for column, value in row.items():
                record[column] = self._make_json_serializable(value)
            records.append(record)
            
        return records


class WBSD1Manager:
    """Manages WBS data operations with D1 database"""
    
    def __init__(self, d1_client: D1Client):
        self.d1 = d1_client
        
    def create_comprehensive_wbs_table(self) -> dict[str, Any]:
        """Create a simple WBS table with just the first 2 columns plus CREATE_DATE"""
        sql = """
        CREATE TABLE IF NOT EXISTS "wbs" (
            "WBS_ELEMENT_CDE" TEXT PRIMARY KEY,
            "WBS_ELEMENT_DESC" TEXT,
            "CREATE_DATE" DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        return self.d1.query(sql)
        
    def recreate_wbs_table_with_CREATE_DATE(self) -> dict[str, Any]:
        """Drop and recreate the WBS table with CREATE_DATE column"""
        # First drop the existing table
        drop_sql = 'DROP TABLE IF EXISTS "wbs";'
        self.d1.query(drop_sql)
        
        # Then create the new table with CREATE_DATE column
        create_sql = """
        CREATE TABLE "wbs" (
            "WBS_ELEMENT_CDE" TEXT PRIMARY KEY,
            "WBS_ELEMENT_DESC" TEXT,
            "CREATE_DATE" DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        return self.d1.query(create_sql)
        
    def insert_wbs_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Insert a single WBS record into the database"""
        # Map Excel columns to database columns
        # Excel: WBS_ELEMENT_CDE -> Database: WBS_ELEMENT_CDE
        # Excel: WBS_ELEMENT_NME -> Database: WBS_ELEMENT_DESC
        # CREATE_DATE will be set to current timestamp
        
        wbs_code = record.get("WBS_ELEMENT_CDE")
        wbs_desc = record.get("WBS_ELEMENT_NME")  # Excel column name
        
        # Convert empty strings to None
        if wbs_code == "":
            wbs_code = None
        if wbs_desc == "":
            wbs_desc = None
            
        values = [wbs_code, wbs_desc]
        
        sql = """
        INSERT OR REPLACE INTO "wbs" ("WBS_ELEMENT_CDE", "WBS_ELEMENT_DESC", "CREATE_DATE")
        VALUES (?, ?, CURRENT_TIMESTAMP);
        """
        
        return self.d1.query(sql, params=values)
        
    def batch_insert_wbs_records(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Insert multiple WBS records"""
        results = []
        
        for i, record in enumerate(records, 1):
            try:
                wbs_code = record.get("WBS_ELEMENT_CDE", f"Record_{i}")
                print(f"   📝 Inserting record {i}: {wbs_code}")
                
                result = self.insert_wbs_record(record)
                results.append(result)
                print(f"   ✓ Successfully inserted {wbs_code}")
                
            except Exception as e:
                print(f"   ❌ Failed to insert record {i}: {e}")
                results.append({"error": str(e)})
                
        return results
        
    def get_table_info(self) -> dict[str, Any]:
        """Get information about the WBS table structure"""
        return self.d1.query("PRAGMA table_info('wbs');")
        
    def count_records(self) -> dict[str, Any]:
        """Count the number of records in the WBS table"""
        return self.d1.query("SELECT COUNT(*) as record_count FROM wbs;")


def main() -> int:
    """Main execution function"""
    print("🚀 WBS Excel to D1 Database Loader")
    print("=" * 50)
    
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
    wbs_manager = WBSD1Manager(d1_client)
    
    # Load Excel data
    excel_file = Path("wbs_100.xlsx")
    if not excel_file.exists():
        print(f"❌ Excel file '{excel_file}' not found in current directory.")
        return 1
        
    try:
        excel_loader = WBSExcelLoader(excel_file)
        # Load only first 10 records as requested
        data = excel_loader.load_data(nrows=20)
        records = excel_loader.get_wbs_records()
        
        print(f"\n📊 Loaded {len(records)} records from Excel")
        if records:
            print("   Sample columns:", list(records[0].keys())[:5])
            
    except Exception as e:
        print(f"❌ Failed to load Excel data: {e}")
        return 1
    
    # Setup database table
    try:
        print("\n🔧 Setting up database table...")
        
        # Check if we need to recreate the table with CREATE_DATE column
        try:
            table_info = wbs_manager.get_table_info()
            result = table_info.get("result", [])
            columns = [row[1] for row in result if len(row) > 1] if result else []
            
            if "CREATE_DATE" not in columns:
                print("🔄 Recreating table with CREATE_DATE column...")
                recreate_result = wbs_manager.recreate_wbs_table_with_CREATE_DATE()
                print("✓ Table recreated successfully with CREATE_DATE column")
            else:
                print("✓ Table already has CREATE_DATE column")
                
        except Exception as check_error:
            print(f"⚠️  Could not check table structure, creating new table: {check_error}")
            create_result = wbs_manager.create_comprehensive_wbs_table()
            print("✓ Table created successfully")
        
        # Show final table info
        try:
            table_info = wbs_manager.get_table_info()
            print("✓ Retrieved table information")
            
            # Parse table info more safely
            result = table_info.get("result", [])
            if result and len(result) > 0:
                columns = [row[1] for row in result if len(row) > 1]
                print(f"   Table columns ({len(columns)}): {columns}")
            else:
                print("   Table info result is empty, but table exists")
                
        except Exception as table_info_error:
            print(f"⚠️  Warning: Could not retrieve table info: {table_info_error}")
            print("   Proceeding with insert operation...")
        
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return 1
    
    # Insert records
    try:
        print(f"\n📝 Inserting {len(records)} records into D1 database...")
        
        # Start timing
        start_time = time.time()
        results = wbs_manager.batch_insert_wbs_records(records)
        end_time = time.time()
        
        # Calculate elapsed time
        elapsed_time = end_time - start_time
        
        # Count successful insertions
        successful = sum(1 for r in results if "error" not in r)
        failed = len(results) - successful
        
        print(f"\n📊 Import Summary:")
        print(f"   ✓ Successfully inserted: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   ⏱️  Insert elapsed time: {elapsed_time:.2f} seconds")
        print(f"   📈 Average time per record: {(elapsed_time/len(records)):.3f} seconds")
        
        # Show final record count
        count_result = wbs_manager.count_records()
        total_records = count_result.get("result", [{}])[0].get("record_count", 0)
        print(f"   📈 Total records in database: {total_records}")
        
    except Exception as e:
        print(f"❌ Insert operation failed: {e}")
        return 1
    
    print("\n🎉 Process completed successfully!")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())