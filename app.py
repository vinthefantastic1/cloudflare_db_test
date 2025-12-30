#!/usr/bin/env python3
"""
WBS Flask Web Application
A modern, responsive web interface for browsing and searching WBS elements

Required packages:
- flask
- python-dotenv
"""

from __future__ import annotations

import json
import os
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Any, Sequence, Optional
from flask import Flask, render_template, request, jsonify, flash
from dotenv import load_dotenv
import datetime
import time
from functools import lru_cache

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
        self._cache = {}
        self._cache_timeout = 300  # 5 minutes
        
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache entry is still valid"""
        if cache_key not in self._cache:
            return False
        return time.time() - self._cache[cache_key]['timestamp'] < self._cache_timeout
        
    def _get_from_cache(self, cache_key: str):
        """Get data from cache if valid"""
        if self._is_cache_valid(cache_key):
            return self._cache[cache_key]['data']
        return None
        
    def _set_cache(self, cache_key: str, data):
        """Store data in cache"""
        self._cache[cache_key] = {
            'data': data,
            'timestamp': time.time()
        }
        
    def _extract_results(self, d1_response: dict[str, Any]) -> list[dict]:
        """Extract actual results from D1 response format"""
        if not d1_response:
            print("⚠️  Warning: Empty D1 response")
            return []
            
        result_data = d1_response.get("result", [])
        if not result_data:
            print("⚠️  Warning: No result data in D1 response")
            return []
            
        if not isinstance(result_data, list) or len(result_data) == 0:
            print(f"⚠️  Warning: Invalid result_data type: {type(result_data)}")
            return []
            
        first_result = result_data[0]
        if not isinstance(first_result, dict):
            print(f"⚠️  Warning: Invalid first_result type: {type(first_result)}")
            return []
            
        results = first_result.get("results", [])
        if not isinstance(results, list):
            print(f"⚠️  Warning: Invalid results type: {type(results)}")
            return []
            
        return results
        
    def get_all_wbs_items(self, limit: Optional[int] = None, offset: int = 0) -> list[dict]:
        """Get all WBS items from the database with caching"""
        cache_key = f"wbs_items_{limit}_{offset}"
        
        # Check cache first
        cached_result = self._get_from_cache(cache_key)
        if cached_result is not None:
            print(f"📋 Using cached result for {cache_key}")
            return cached_result
        
        # Filter out NULL records for better performance
        sql = "SELECT * FROM wbs_2 WHERE WBS_ELEMENT_CDE IS NOT NULL ORDER BY WBS_ELEMENT_CDE"
        
        if limit:
            sql += f" LIMIT {limit} OFFSET {offset}"
            
        print(f"🔍 Executing query: {sql}")
        start_time = time.time()
        result = self.d1.query(sql)
        query_time = time.time() - start_time
        
        extracted_results = self._extract_results(result)
        print(f"📊 Extracted {len(extracted_results)} items in {query_time:.2f}s")
        
        # Cache the result
        self._set_cache(cache_key, extracted_results)
        
        if extracted_results:
            print(f"🔍 First item sample: {extracted_results[0]}")
        
        return extracted_results
        
    def count_wbs_items(self) -> int:
        """Get the total count of WBS items with caching"""
        cache_key = "wbs_count"
        
        # Check cache first
        cached_result = self._get_from_cache(cache_key)
        if cached_result is not None:
            return cached_result
        
        # Only count non-NULL records for accuracy
        result = self.d1.query("SELECT COUNT(*) as total FROM wbs_2 WHERE WBS_ELEMENT_CDE IS NOT NULL")
        extracted_results = self._extract_results(result)
        
        count = 0
        if extracted_results and len(extracted_results) > 0:
            count = extracted_results[0].get("total", 0)
        
        # Cache the result
        self._set_cache(cache_key, count)
        return count
        
    def search_wbs_items(self, search_term: str) -> list[dict]:
        """Search for WBS items by code or description"""
        sql = """
        SELECT * FROM wbs_2 
        WHERE WBS_ELEMENT_CDE LIKE ? OR WBS_ELEMENT_NME LIKE ?
        ORDER BY WBS_ELEMENT_CDE
        """
        search_pattern = f"%{search_term}%"
        print(f"🔍 Searching for: {search_term}")
        result = self.d1.query(sql, params=[search_pattern, search_pattern])
        extracted_results = self._extract_results(result)
        print(f"📊 Found {len(extracted_results)} matching items")
        return extracted_results

    def get_wbs_stats(self) -> dict[str, Any]:
        """Get statistics about the WBS data"""
        stats = {}
        
        # Total count
        stats['total_count'] = self.count_wbs_items()
        
        # Count by prefix
        prefix_sql = """
        SELECT SUBSTR(WBS_ELEMENT_CDE, 1, 2) as prefix, COUNT(*) as count
        FROM wbs_2 
        WHERE WBS_ELEMENT_CDE IS NOT NULL
        GROUP BY SUBSTR(WBS_ELEMENT_CDE, 1, 2)
        ORDER BY count DESC
        LIMIT 10
        """
        result = self.d1.query(prefix_sql)
        stats['top_prefixes'] = self._extract_results(result)
        
        return stats


# Initialize Flask application
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret-key-change-in-production')

# Add datetime to template context
@app.context_processor
def utility_processor():
    return dict(moment=datetime.datetime.now)

# Initialize database components
try:
    print("🔧 Initializing database connection...")
    d1_config = D1Config.from_env()
    print("✅ D1 config loaded successfully")
    d1_client = D1Client(d1_config)
    print("✅ D1 client created successfully")
    wbs_lister = WBSLister(d1_client)
    print("✅ WBS lister created successfully")
    
    # Test database connection
    test_count = wbs_lister.count_wbs_items()
    print(f"📊 Database test successful - found {test_count} items")
    
except Exception as e:
    print(f"❌ Failed to initialize database connection: {e}")
    import traceback
    traceback.print_exc()
    d1_config = None
    d1_client = None
    wbs_lister = None


@app.route('/')
def index():
    """Main page showing WBS items with optimized pagination"""
    page = request.args.get('page', 1, type=int)
    per_page = 15  # Reduced from 20 for faster loading
    search_term = request.args.get('search', '', type=str)
    
    if not wbs_lister:
        flash('Database connection not available', 'error')
        return render_template('error.html', 
                             error_message="Database connection could not be established")
    
    try:
        start_time = time.time()
        
        if search_term:
            # Search functionality
            items = wbs_lister.search_wbs_items(search_term)
            total_count = len(items)
            
            # Manual pagination for search results
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            items = items[start_idx:end_idx]
            
        else:
            # Optimized pagination - get count and items efficiently
            total_count = wbs_lister.count_wbs_items()
            offset = (page - 1) * per_page
            items = wbs_lister.get_all_wbs_items(limit=per_page, offset=offset)
        
        # Calculate pagination info
        total_pages = (total_count + per_page - 1) // per_page
        
        # Get stats only for first page and non-search views to reduce load time
        stats = None
        if page == 1 and not search_term:
            stats = wbs_lister.get_wbs_stats()
        
        load_time = time.time() - start_time
        print(f"⚡ Page loaded in {load_time:.2f}s")
        
        return render_template('index.html', 
                             items=items,
                             total_count=total_count,
                             page=page,
                             total_pages=total_pages,
                             per_page=per_page,
                             search_term=search_term,
                             stats=stats)
                             
    except Exception as e:
        flash(f'Database error: {str(e)}', 'error')
        return render_template('error.html', error_message=str(e))


@app.route('/api/search')
def api_search():
    """API endpoint for live search suggestions"""
    query = request.args.get('q', '', type=str)
    
    if not wbs_lister or not query or len(query) < 2:
        return jsonify([])
    
    try:
        # Limit suggestions to 10 items
        items = wbs_lister.search_wbs_items(query)[:10]
        
        suggestions = []
        for item in items:
            suggestions.append({
                'code': item.get('WBS_ELEMENT_CDE', ''),
                'description': item.get('WBS_ELEMENT_NME', ''),
                'created': item.get('CREATE_DATE', '')
            })
            
        return jsonify(suggestions)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats')
def api_stats():
    """API endpoint for dashboard statistics"""
    if not wbs_lister:
        return jsonify({'error': 'Database not available'}), 503
    
    try:
        stats = wbs_lister.get_wbs_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/item/<wbs_code>')
def item_detail(wbs_code):
    """Detailed view of a specific WBS item"""
    if not wbs_lister:
        flash('Database connection not available', 'error')
        return render_template('error.html')
    
    try:
        # Search for the specific item
        items = wbs_lister.search_wbs_items(wbs_code)
        item = None
        
        # Find exact match
        for i in items:
            if i.get('WBS_ELEMENT_CDE') == wbs_code:
                item = i
                break
        
        if not item:
            flash(f'WBS item "{wbs_code}" not found', 'error')
            return render_template('error.html', 
                                 error_message=f'WBS item "{wbs_code}" not found')
        
        return render_template('item_detail.html', item=item)
        
    except Exception as e:
        flash(f'Database error: {str(e)}', 'error')
        return render_template('error.html', error_message=str(e))


@app.errorhandler(404)
def not_found_error(error):
    return render_template('error.html', 
                         error_message="Page not found"), 404


@app.errorhandler(500)
def internal_error(error):
    return render_template('error.html', 
                         error_message="Internal server error"), 500


if __name__ == '__main__':
    # Use 0.0.0.0 for Docker compatibility, 127.0.0.1 for local development
    import os
    host = '0.0.0.0' if os.getenv('FLASK_ENV') == 'production' else '127.0.0.1'
    debug = os.getenv('FLASK_ENV') != 'production'
    app.run(debug=debug, host=host, port=5000)