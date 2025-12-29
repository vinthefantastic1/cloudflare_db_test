#!/usr/bin/env python3
"""
Excel WBS Data Reader
Reads and processes WBS data from wbs_100.xlsx file

Required packages:
- pandas
- openpyxl (for Excel file reading)
"""

from __future__ import annotations

import pandas as pd
from pathlib import Path
import sys
from typing import Optional


class WBSExcelReader:
    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self.data: Optional[pd.DataFrame] = None
        
    def load_data(self) -> pd.DataFrame:
        """Load the Excel file and return the DataFrame"""
        try:
            if not self.file_path.exists():
                raise FileNotFoundError(f"Excel file not found: {self.file_path}")
                
            print(f"📖 Reading Excel file: {self.file_path}")
            self.data = pd.read_excel(self.file_path)
            print(f"✓ Successfully loaded {len(self.data)} rows and {len(self.data.columns)} columns")
            return self.data
            
        except Exception as e:
            print(f"❌ Error reading Excel file: {e}")
            raise
    
    def display_info(self) -> None:
        """Display basic information about the dataset"""
        if self.data is None:
            print("❌ No data loaded. Call load_data() first.")
            return
            
        print("\n" + "="*80)
        print("📊 DATASET INFORMATION")
        print("="*80)
        
        print(f"Shape: {self.data.shape[0]} rows × {self.data.shape[1]} columns")
        print(f"Memory usage: {self.data.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        print(f"\n📋 Column Names ({len(self.data.columns)}):")
        for i, col in enumerate(self.data.columns, 1):
            print(f"  {i:2d}. {col}")
            
    def display_sample(self, n: int = 5) -> None:
        """Display first n rows of the dataset"""
        if self.data is None:
            print("❌ No data loaded. Call load_data() first.")
            return
            
        print(f"\n📋 First {n} rows:")
        print("-" * 120)
        
        # Display key columns first
        key_columns = ['WBS_ELEMENT_CDE', 'WBS_ELEMENT_NME', 'PROJ_ID', 'PROJ_NAME']
        available_key_cols = [col for col in key_columns if col in self.data.columns]
        
        if available_key_cols:
            print("Key columns:")
            print(self.data[available_key_cols].head(n).to_string())
            
        print(f"\n📊 Full dataset preview:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        print(self.data.head(n))
        
    def get_column_stats(self) -> None:
        """Display statistics for each column"""
        if self.data is None:
            print("❌ No data loaded. Call load_data() first.")
            return
            
        print("\n📈 COLUMN STATISTICS")
        print("="*80)
        
        for col in self.data.columns:
            print(f"\n🔍 {col}:")
            print(f"   Data type: {self.data[col].dtype}")
            print(f"   Non-null count: {self.data[col].notna().sum()}")
            print(f"   Null count: {self.data[col].isna().sum()}")
            print(f"   Unique values: {self.data[col].nunique()}")
            
            # Show sample values for non-numeric columns
            if self.data[col].dtype == 'object':
                unique_vals = self.data[col].dropna().unique()[:5]
                print(f"   Sample values: {list(unique_vals)}")
            else:
                # Show basic stats for numeric columns
                print(f"   Min: {self.data[col].min()}")
                print(f"   Max: {self.data[col].max()}")
                
    def search_wbs(self, search_term: str) -> pd.DataFrame:
        """Search for WBS elements containing the search term"""
        if self.data is None:
            print("❌ No data loaded. Call load_data() first.")
            return pd.DataFrame()
            
        # Search in WBS code and name columns
        search_columns = ['WBS_ELEMENT_CDE', 'WBS_ELEMENT_NME']
        available_search_cols = [col for col in search_columns if col in self.data.columns]
        
        if not available_search_cols:
            print("❌ No WBS columns found to search")
            return pd.DataFrame()
            
        mask = pd.Series([False] * len(self.data))
        for col in available_search_cols:
            mask |= self.data[col].astype(str).str.contains(search_term, case=False, na=False)
            
        results = self.data[mask]
        print(f"🔍 Found {len(results)} records matching '{search_term}'")
        return results
        
    def export_filtered_data(self, filtered_data: pd.DataFrame, output_file: str) -> None:
        """Export filtered data to Excel"""
        try:
            output_path = Path(output_file)
            filtered_data.to_excel(output_path, index=False)
            print(f"✓ Exported {len(filtered_data)} rows to {output_path}")
        except Exception as e:
            print(f"❌ Error exporting data: {e}")


def main() -> int:
    # Look for the Excel file in the current directory
    excel_file = Path("wbs_100.xlsx")
    
    if not excel_file.exists():
        print(f"❌ Excel file '{excel_file}' not found in current directory.")
        print("Please make sure the file exists and try again.")
        return 1
        
    try:
        # Create reader instance and load data
        reader = WBSExcelReader(excel_file)
        data = reader.load_data()
        
        # Display dataset information
        reader.display_info()
        reader.display_sample(3)
        
        # Ask user what they want to do
        while True:
            print("\n" + "="*80)
            print("📋 AVAILABLE ACTIONS:")
            print("  1. Show column statistics")
            print("  2. Search WBS elements")
            print("  3. Display more sample data")
            print("  4. Export all data to new Excel file")
            print("  5. Exit")
            
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == "1":
                reader.get_column_stats()
            elif choice == "2":
                search_term = input("Enter search term: ").strip()
                if search_term:
                    results = reader.search_wbs(search_term)
                    if len(results) > 0:
                        print("\n🎯 Search Results:")
                        print(results.head(10).to_string())
                        
                        export = input("\nExport results to Excel? (y/n): ").strip().lower()
                        if export == 'y':
                            filename = f"wbs_search_{search_term.replace(' ', '_')}.xlsx"
                            reader.export_filtered_data(results, filename)
            elif choice == "3":
                try:
                    n = int(input("How many rows to display? "))
                    reader.display_sample(n)
                except ValueError:
                    print("❌ Please enter a valid number")
            elif choice == "4":
                output_file = input("Enter output filename (e.g., processed_data.xlsx): ").strip()
                if output_file:
                    if not output_file.endswith('.xlsx'):
                        output_file += '.xlsx'
                    reader.export_filtered_data(data, output_file)
            elif choice == "5":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please enter 1-5.")
                
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return 1
        
    return 0


if __name__ == "__main__":
    raise SystemExit(main())