import sqlite3
import pandas as pd
import time
import os
import sys

# Add current dir to path to import from app
sys.path.append(os.getcwd())
from app import create_table_from_dataframe, find_data_total

DATABASE = 'zimstats.sqlite'

def run_stress_test():
    print("🚀 Starting Scalability Stress Test...")
    
    # 1. Clean up any existing master_stress table
    conn = sqlite3.connect(DATABASE)
    conn.execute("DROP TABLE IF EXISTS master_stress")
    conn.commit()
    conn.close()
    
    # 2. Simulate 35 years of data (1990 - 2025)
    print("📊 Generating 35 years of historical data...")
    years = list(range(1990, 2026))
    provinces = ['Harare', 'Bulawayo', 'Manicaland', 'Mashonaland Central', 'Mashonaland East', 
                 'Mashonaland West', 'Matabeleland North', 'Matabeleland South', 'Midlands', 'Masvingo']
    
    data = []
    for year in years:
        for prov in provinces:
            data.append({
                'Year': year,
                'Province': prov,
                'Indicator': 'employment_total',
                'Value': 1000 + (year - 1990) * 10
            })
    
    df = pd.DataFrame(data)
    
    # 3. Upload data (Simulating consolidation)
    print(f"📥 Consolidating {len(df)} rows into 'master_stress'...")
    start_time = time.time()
    create_table_from_dataframe(df, 'master_stress', 'stress')
    upload_duration = time.time() - start_time
    print(f"✅ Upload & Indexing completed in {upload_duration:.2f}s")
    
    # 4. Measure Query Performance
    print("\n⏱️ Measuring Aggregation Performance (35 years of history)...")
    
    from app import app
    with app.app_context():
        filters = {'year': 2025, 'region': 'Harare'}
        query_start = time.time()
        res = find_data_total(['stress'], ['employment_total'], filters)
        query_duration = time.time() - query_start
    
    print(f"🔍 Query Result (Harare 2025): {res}")
    print(f"⚡ Query execution time: {query_duration*1000:.2f}ms")
    
    # 5. Verify Indexing
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA index_list('master_stress')")
    indexes = cursor.fetchall()
    conn.close()
    
    print(f"\n📑 Indexes found on 'master_stress': {len(indexes)}")
    for idx in indexes:
        print(f"  - {idx[1]}")

    if query_duration < 0.05: # 50ms threshold
        print("\n✅ PERFORMANCE PASS: Query is lightning fast!")
    else:
        print("\n⚠️ PERFORMANCE WARNING: Query took longer than 50ms.")

if __name__ == "__main__":
    run_stress_test()
