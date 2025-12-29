#!/usr/bin/env python3
"""
Advanced WBS Excel to D1 Database Loader (Version 2)
Loads ALL columns from WBS data into Cloudflare D1 database table 'wbs_2'

Features:
- Comprehensive schema handling for all 23 columns
- Advanced data type mapping and validation
- Optimized bulk insertion with proper NULL handling
- Enhanced error handling and performance monitoring
- Smart data cleaning and transformation

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
import numpy as np
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence, Optional, Dict, List, Union
from dotenv import load_dotenv
import datetime
import time
import sys
from decimal import Decimal

# Load environment variables from .env file
load_dotenv()


MAX_ROWS_TO_LOAD = 200  # Limit rows for performance during testing

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
    """Enhanced client for interacting with Cloudflare D1 database"""
    
    def __init__(self, cfg: D1Config, timeout_seconds: int = 45) -> None:
        self._cfg = cfg
        self._timeout_seconds = timeout_seconds

    @property
    def _endpoint(self) -> str:
        return (
            f"https://api.cloudflare.com/client/v4/accounts/"
            f"{self._cfg.account_id}/d1/database/{self._cfg.database_id}/query"
        )

    def query(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any]:
        """Execute a SQL query against the D1 database with enhanced error handling"""
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
        except Exception as e:
            raise RuntimeError(f"D1 query failed: {str(e)}") from e

        if not data.get("success", False):
            raise RuntimeError(f"D1 query failed: {data.get('errors')}")
        return data


class WBSSchemaManager:
    """Manages the comprehensive WBS database schema with all 23 columns"""
    
    # Complete column definitions with proper SQLite types
    COLUMN_DEFINITIONS = {
        'WBS_ELEMENT_CDE': 'TEXT PRIMARY KEY',           # Unique WBS code
        'WBS_ELEMENT_NME': 'TEXT',                       # WBS element name/description
        'PROJ_ID': 'TEXT',                               # Project ID
        'PROJ_NAME': 'TEXT',                             # Project name
        'PROJ_TYPE_CDE': 'TEXT',                         # Project type code
        'PROJ_FY': 'INTEGER',                            # Project fiscal year
        'REQ_COST_CENTER_CDE': 'REAL',                   # Requesting cost center code
        'RESP_COST_CENTER_CDE': 'REAL',                  # Responsible cost center code
        'COMPANY_CDE': 'TEXT',                           # Company code (IBRD/IFC/MIGA)
        'CNTRY_CODE': 'TEXT',                            # Country code
        'FUND_CENTER_CDE': 'REAL',                       # Fund center code
        'RGN_ABBR_NME': 'TEXT',                          # Region abbreviation name
        'SECTOR_CDE': 'TEXT',                            # Sector code
        'BUS_AREA_CDE': 'TEXT',                          # Business area code
        'BUS_AREA_NME': 'TEXT',                          # Business area name
        'BUS_PROC_CDE': 'TEXT',                          # Business process code
        'BUS_PROC_NME': 'TEXT',                          # Business process name
        'ACCT_IND': 'TEXT',                              # Account indicator (Y/N)
        'CLOSED_IND': 'TEXT',                            # Closed indicator (Y/N)
        'RELEASED_IND': 'TEXT',                          # Released indicator (Y/N)
        'SAP_STATUS': 'TEXT',                            # SAP status code
        'CREATE_DATE': 'TEXT',                           # Creation timestamp
        'LAST_UPDATE_DATE': 'TEXT'                       # Last update timestamp
    }
    
    @classmethod
    def get_create_table_sql(cls) -> str:
        """Generate CREATE TABLE SQL for wbs_2 with all columns"""
        columns = []
        for col_name, col_type in cls.COLUMN_DEFINITIONS.items():
            columns.append(f"    {col_name} {col_type}")
        
        columns_sql = ",\n".join(columns)
        return f"""
        CREATE TABLE IF NOT EXISTS wbs_2 (
{columns_sql},
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    
    @classmethod
    def get_column_names(cls) -> List[str]:
        """Get list of all column names"""
        return list(cls.COLUMN_DEFINITIONS.keys())


class WBSDataProcessor:
    """Advanced data processing and validation for WBS data"""


    def __init__(self, excel_file: str = "wbs_100.xlsx"):
        self.excel_file = excel_file
        self.df: pd.DataFrame = None
        
    def load_excel_data(self) -> pd.DataFrame:
        """Load and preprocess Excel data with comprehensive validation"""
        print(f"📖 Loading Excel file: {self.excel_file}")
        
        if not Path(self.excel_file).exists():
            raise FileNotFoundError(f"Excel file not found: {self.excel_file}")
            
        try:
            # Load with optimal settings for mixed data types
            self.df = pd.read_excel(
                self.excel_file,
                engine='openpyxl',
                nrows=MAX_ROWS_TO_LOAD,
                na_values=['', 'N/A', 'NULL', 'null', 'None', 'none', '#N/A', '#NULL!']
            )
            
            print(f"✓ Successfully loaded {len(self.df):,} rows and {len(self.df.columns)} columns")
            
            # Validate we have all expected columns
            expected_cols = set(WBSSchemaManager.get_column_names())
            actual_cols = set(self.df.columns)
            
            missing_cols = expected_cols - actual_cols
            if missing_cols:
                raise ValueError(f"Missing columns in Excel file: {missing_cols}")
            
            extra_cols = actual_cols - expected_cols
            if extra_cols:
                print(f"⚠️  Extra columns found (will be ignored): {extra_cols}")
            
            return self.df
            
        except Exception as e:
            raise RuntimeError(f"Failed to load Excel file: {str(e)}") from e
    
    def clean_and_validate_data(self) -> pd.DataFrame:
        """Clean and validate data with comprehensive transformations"""
        if self.df is None:
            raise RuntimeError("No data loaded. Call load_excel_data() first.")
        
        print("🧹 Cleaning and validating data...")
        
        # Create a copy to avoid modifying original
        df_clean = self.df.copy()
        
        # 1. Handle numeric columns
        numeric_columns = ['PROJ_FY', 'REQ_COST_CENTER_CDE', 'RESP_COST_CENTER_CDE', 'FUND_CENTER_CDE']
        for col in numeric_columns:
            if col in df_clean.columns:
                # Convert to proper numeric types, preserving NaN
                if col == 'PROJ_FY':
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').astype('Int64')
                else:
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        # 2. Handle text columns - clean whitespace and empty strings
        text_columns = [col for col in df_clean.columns if col not in numeric_columns]
        for col in text_columns:
            if col in df_clean.columns:
                # Strip whitespace and convert empty strings to None
                df_clean[col] = df_clean[col].astype(str).str.strip()
                df_clean[col] = df_clean[col].replace(['', 'nan', 'None', 'NULL'], None)
        
        # 3. Handle datetime columns
        datetime_columns = ['CREATE_DATE', 'LAST_UPDATE_DATE']
        for col in datetime_columns:
            if col in df_clean.columns:
                # Convert datetime objects to string format
                df_clean[col] = df_clean[col].apply(self._format_datetime_value)
        
        # 4. Handle boolean indicators
        indicator_columns = ['ACCT_IND', 'CLOSED_IND', 'RELEASED_IND']
        for col in indicator_columns:
            if col in df_clean.columns:
                # Normalize boolean indicators to Y/N or NULL
                df_clean[col] = df_clean[col].apply(self._normalize_indicator)
        
        # 5. Validate primary key uniqueness
        if 'WBS_ELEMENT_CDE' in df_clean.columns:
            duplicates = df_clean['WBS_ELEMENT_CDE'].duplicated().sum()
            if duplicates > 0:
                print(f"⚠️  Found {duplicates} duplicate WBS codes - will keep first occurrence")
                df_clean = df_clean.drop_duplicates(subset=['WBS_ELEMENT_CDE'], keep='first')
        
        # 6. Remove completely empty rows
        df_clean = df_clean.dropna(how='all')
        
        print(f"✓ Data cleaning complete. Final shape: {df_clean.shape}")
        
        return df_clean
    
    def _format_datetime_value(self, value) -> Optional[str]:
        """Format datetime values consistently"""
        if pd.isna(value) or value is None:
            return None
        
        if isinstance(value, datetime.time):
            return value.strftime('%H:%M:%S')
        elif isinstance(value, datetime.datetime):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(value, str):
            return value.strip() if value.strip() else None
        else:
            return str(value)
    
    def _normalize_indicator(self, value) -> Optional[str]:
        """Normalize boolean indicators to consistent format"""
        if pd.isna(value) or value is None:
            return None
        
        value_str = str(value).strip().upper()
        if value_str in ['Y', 'YES', 'TRUE', '1']:
            return 'Y'
        elif value_str in ['N', 'NO', 'FALSE', '0']:
            return 'N'
        else:
            return None


class WBSBulkLoader:
    """High-performance bulk loader for WBS data"""
    
    def __init__(self, d1_client: D1Client, batch_size: int = 50):
        self.d1 = d1_client
        self.batch_size = batch_size
        self.schema_manager = WBSSchemaManager()
        
    def create_table(self) -> None:
        """Create the wbs_2 table with comprehensive schema"""
        print("🏗️  Creating wbs_2 table...")
        
        create_sql = self.schema_manager.get_create_table_sql()
        print(f"📝 SQL: {create_sql}")
        
        try:
            self.d1.query(create_sql)
            print("✓ Table wbs_2 created successfully")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("ℹ️  Table wbs_2 already exists")
            else:
                raise RuntimeError(f"Failed to create table: {str(e)}") from e
    
    def clear_existing_data(self) -> None:
        """Clear existing data from wbs_2 table"""
        print("🗑️  Clearing existing data from wbs_2...")
        try:
            result = self.d1.query("DELETE FROM wbs_2")
            print("✓ Existing data cleared")
        except Exception as e:
            print(f"⚠️  Warning: Could not clear existing data: {str(e)}")
    
    def bulk_insert_data(self, df: pd.DataFrame) -> None:
        """Bulk insert data with optimized batching and error handling"""
        total_records = len(df)
        print(f"📦 Starting bulk insert of {total_records:,} records...")
        
        # Get column names in correct order
        columns = self.schema_manager.get_column_names()
        
        # Prepare insert SQL
        placeholders = ', '.join(['?' for _ in columns])
        columns_str = ', '.join(columns)
        insert_sql = f"INSERT OR IGNORE INTO wbs_2 ({columns_str}) VALUES ({placeholders})"
        
        print(f"📝 Insert SQL: {insert_sql}")
        
        # Process in batches
        successful_batches = 0
        failed_records = 0
        start_time = time.time()
        
        for batch_start in range(0, total_records, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total_records)
            batch_df = df.iloc[batch_start:batch_end]
            batch_num = (batch_start // self.batch_size) + 1
            total_batches = (total_records + self.batch_size - 1) // self.batch_size
            
            print(f"⚡ Processing batch {batch_num}/{total_batches} (records {batch_start+1}-{batch_end})...")
            
            try:
                # Prepare batch data
                batch_values = []
                for _, row in batch_df.iterrows():
                    record_values = []
                    for col in columns:
                        value = row[col]
                        # Handle different data types properly
                        if pd.isna(value) or value is None:
                            record_values.append(None)
                        elif isinstance(value, (int, float)):
                            if pd.isna(value):
                                record_values.append(None)
                            else:
                                record_values.append(value)
                        else:
                            # Convert to string and handle None
                            str_value = str(value).strip() if value is not None else None
                            record_values.append(str_value if str_value else None)
                    
                    batch_values.append(record_values)
                
                # Execute bulk insert with multiple statements
                batch_queries = []
                for values in batch_values:
                    batch_queries.append({
                        "sql": insert_sql,
                        "params": values
                    })
                
                # Execute batch
                self._execute_batch_queries(batch_queries)
                successful_batches += 1
                
                # Progress indicator
                elapsed = time.time() - start_time
                avg_time_per_batch = elapsed / successful_batches
                estimated_remaining = (total_batches - successful_batches) * avg_time_per_batch
                
                print(f"   ✓ Batch {batch_num} completed ({elapsed:.1f}s elapsed, ~{estimated_remaining:.1f}s remaining)")
                
            except Exception as e:
                print(f"   ❌ Batch {batch_num} failed: {str(e)}")
                failed_records += len(batch_df)
                
                # Try individual inserts for this batch
                print(f"   🔄 Attempting individual inserts for batch {batch_num}...")
                individual_failures = self._insert_batch_individually(batch_df, columns, insert_sql)
                failed_records = failed_records - len(batch_df) + individual_failures
        
        # Final statistics
        total_time = time.time() - start_time
        successful_records = total_records - failed_records
        
        print(f"\n📊 BULK INSERT COMPLETED")
        print(f"   ✓ Successfully inserted: {successful_records:,} records")
        print(f"   ❌ Failed records: {failed_records:,}")
        print(f"   ⏱️  Total time: {total_time:.2f} seconds")
        print(f"   🚀 Average rate: {successful_records/total_time:.0f} records/second")
        print(f"   📈 Success rate: {(successful_records/total_records)*100:.1f}%")
    
    def _execute_batch_queries(self, queries: List[Dict]) -> None:
        """Execute batch queries efficiently"""
        # For now, execute individually due to D1 API limitations
        # In future, this could be optimized with transaction support
        for query in queries:
            self.d1.query(query["sql"], query["params"])
    
    def _insert_batch_individually(self, batch_df: pd.DataFrame, columns: List[str], insert_sql: str) -> int:
        """Insert batch records individually as fallback"""
        failures = 0
        for idx, (_, row) in enumerate(batch_df.iterrows()):
            try:
                values = []
                for col in columns:
                    value = row[col]
                    if pd.isna(value) or value is None:
                        values.append(None)
                    else:
                        values.append(str(value).strip() if str(value).strip() else None)
                
                self.d1.query(insert_sql, values)
            except Exception as e:
                print(f"      ❌ Individual record {idx+1} failed: {str(e)}")
                failures += 1
        
        if failures < len(batch_df):
            print(f"      ✓ Individual inserts: {len(batch_df) - failures}/{len(batch_df)} successful")
        
        return failures
    
    def verify_data_load(self) -> Dict[str, Any]:
        """Verify the data load and return statistics"""
        print("🔍 Verifying data load...")
        
        try:
            # Count total records
            count_result = self.d1.query("SELECT COUNT(*) as total FROM wbs_2")
            total_count = count_result["result"][0]["results"][0]["total"]
            
            # Sample data
            sample_result = self.d1.query("SELECT * FROM wbs_2 LIMIT 3")
            sample_data = sample_result["result"][0]["results"]
            
            # Company distribution
            company_result = self.d1.query("""
                SELECT COMPANY_CDE, COUNT(*) as count 
                FROM wbs_2 
                WHERE COMPANY_CDE IS NOT NULL 
                GROUP BY COMPANY_CDE 
                ORDER BY count DESC
            """)
            company_stats = company_result["result"][0]["results"]
            
            stats = {
                "total_records": total_count,
                "sample_data": sample_data,
                "company_distribution": company_stats
            }
            
            print(f"✓ Verification complete:")
            print(f"   📊 Total records in wbs_2: {total_count:,}")
            print(f"   🏢 Company distribution: {company_stats}")
            
            return stats
            
        except Exception as e:
            raise RuntimeError(f"Verification failed: {str(e)}") from e


def main():
    """Main execution function"""
    print("🚀 WBS Advanced Loader v2.0 - Loading ALL Columns to wbs_2")
    print("=" * 80)
    
    try:
        # 1. Initialize configuration
        print("🔧 Initializing configuration...")
        config = D1Config.from_env()
        d1_client = D1Client(config)
        print("✓ D1 client initialized")
        
        # 2. Initialize components
        processor = WBSDataProcessor()
        loader = WBSBulkLoader(d1_client, batch_size=25)  # Smaller batches for reliability
        
        # 3. Load and process data
        df = processor.load_excel_data()
        df_clean = processor.clean_and_validate_data()
        
        # 4. Create table
        loader.create_table()
        
        # 5. Clear existing data (optional)
        response = input("🤔 Clear existing data in wbs_2? (y/N): ").strip().lower()
        if response == 'y':
            loader.clear_existing_data()
        
        # 6. Bulk insert data
        loader.bulk_insert_data(df_clean)
        
        # 7. Verify results
        stats = loader.verify_data_load()
        
        print("\n🎉 SUCCESS! WBS data loaded successfully to wbs_2 table")
        print(f"📊 Final count: {stats['total_records']:,} records")
        
    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()