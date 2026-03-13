#!/usr/bin/env python3
"""
Script to clean all fact tables while preserving dimension tables and structure
"""

import sqlite3
import sys

def clean_database():
    """Clean all fact tables while preserving dimension tables"""
    conn = sqlite3.connect('zimstats.sqlite')
    cursor = conn.cursor()
    
    print("Cleaning database...")
    
    # List of fact tables to clean
    fact_tables = [
        'fact_labour',
        'fact_prices', 
        'fact_national_accounts',
        'fact_trade'
    ]
    
    # Clean each fact table
    for table in fact_tables:
        try:
            cursor.execute(f"DELETE FROM {table}")
            rows_deleted = cursor.rowcount
            print(f"✅ Cleaned {table}: {rows_deleted} rows deleted")
        except sqlite3.Error as e:
            print(f"❌ Error cleaning {table}: {e}")
    
    # Reset autoincrement counters
    for table in fact_tables:
        try:
            cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}'")
            print(f"✅ Reset autoincrement for {table}")
        except sqlite3.Error as e:
            print(f"❌ Error resetting sequence for {table}: {e}")
    
    # Keep dimension tables intact but show their counts
    dimension_tables = [
        'dim_date',
        'dim_geography', 
        'dim_industry',
        'dim_demographics',
        'dim_currency',
        'dim_trade_group'
    ]
    
    print("\n--- Dimension Tables Status (Preserved) ---")
    for table in dimension_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"📋 {table}: {count} rows (preserved)")
        except sqlite3.Error as e:
            print(f"❌ Error checking {table}: {e}")
    
    # Also clean the upload metadata and data_uploads tables
    upload_tables = ['data_uploads', 'upload_metadata']
    print("\n--- Upload Tables Status ---")
    for table in upload_tables:
        try:
            cursor.execute(f"DELETE FROM {table}")
            rows_deleted = cursor.rowcount
            print(f"🗑️  Cleaned {table}: {rows_deleted} rows deleted")
        except sqlite3.Error as e:
            print(f"❌ Error cleaning {table}: {e}")
    
    # Reset upload table sequences
    for table in upload_tables:
        try:
            cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}'")
            print(f"✅ Reset autoincrement for {table}")
        except sqlite3.Error as e:
            print(f"❌ Error resetting sequence for {table}: {e}")
    
    conn.commit()
    
    # Show final database status
    print("\n--- Final Database Status ---")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    all_tables = [row[0] for row in cursor.fetchall()]
    
    print(f"Total tables: {len(all_tables)}")
    print("Tables:", ", ".join(all_tables))
    
    # Show fact tables are empty
    print("\n--- Fact Tables (Should be Empty) ---")
    for table in fact_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            status = "✅ Empty" if count == 0 else f"⚠️  {count} rows remaining"
            print(f"{table}: {status}")
        except sqlite3.Error as e:
            print(f"❌ Error checking {table}: {e}")
    
    conn.close()
    print("\n🎉 Database cleaning completed!")
    print("📝 Ready for fresh data uploads through the dashboard")

if __name__ == "__main__":
    try:
        clean_database()
    except Exception as e:
        print(f"❌ Database cleaning failed: {e}")
        sys.exit(1)
