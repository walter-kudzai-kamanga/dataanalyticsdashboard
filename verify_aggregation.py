import sqlite3
import os
import sys
import json

def verify():
    db_path = 'test_verify_agg.sqlite'
    if os.path.exists(db_path): os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Create TWO WIDE Labour tables with INTEGER for Year to simplify match
    cursor.execute('CREATE TABLE labour_q1_province (Province TEXT, Male REAL, Female REAL, Year INTEGER)')
    cursor.execute('INSERT INTO labour_q1_province VALUES ("Harare", 1000, 1500, 2025)')
    
    cursor.execute('CREATE TABLE labour_q2_province (Province TEXT, Male REAL, Female REAL, Year INTEGER)')
    cursor.execute('INSERT INTO labour_q2_province VALUES ("Harare", 500, 500, 2025)')
    
    # 3. Create a LONG table (GDP)
    cursor.execute('CREATE TABLE gdp_long (Date INTEGER, indicator TEXT, value REAL)')
    cursor.execute('INSERT INTO gdp_long VALUES (2025, "Gdp At Market Prices Usd", 50000000000)')
    
    conn.commit()
    conn.close()

    import app 
    app.DATABASE = db_path
    
    from flask import Flask
    flask_app = Flask(__name__)
    
    with flask_app.app_context():
        import flask
        flask.g._database = sqlite3.connect(db_path)
        flask.g._database.row_factory = sqlite3.Row
        
        print(f"Tables found: {app.get_all_table_names()}")
        
        # Test Query Directly
        q = 'SELECT SUM(IFNULL("Male", 0)+IFNULL("Female", 0)) FROM "labour_q1_province" WHERE "Province" COLLATE NOCASE = ? AND ("Year" = ? OR CAST("Year" AS REAL) = ?)'
        p = ['Harare', 2025, 2025]
        res = app.query_db(q, p, one=True)
        print(f"DIRECT QUERY RES: {list(res) if res else 'None'}")

        print("Testing Data Aggregation (Multiple Labour Tables)...")
        filters = {'year': '2025', 'region': 'Harare'}
        labour = app.query_labour_kpis(filters)
        print(f"Employed: {labour['employed']} (Expected 3500)")
        
        if labour['employed'] != 3500:
             raise Exception(f"Aggregation test failed: Got {labour['employed']}")
        
        print("Testing Long Format (GDP)...")
        gdp_filters = {'year': '2025'}
        gdp = app.query_gdp_kpis(gdp_filters)
        print(f"GDP: {gdp['gdp']}B (Expected 50.0B)")
        if gdp['gdp'] != 50.0: raise Exception(f"GDP Long test failed: {gdp['gdp']}")
        
        print("Verification Successful! Aggregation is working.")

if __name__ == '__main__':
    try:
        verify()
    except Exception as e:
        print(f"Verification Failed: {e}")
        sys.exit(1)
    finally:
        if os.path.exists('test_verify_agg.sqlite'): os.remove('test_verify_agg.sqlite')
