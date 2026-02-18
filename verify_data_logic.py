import sqlite3
import os
import sys
import json

# Mocking Flask's g for the database helper
class MockG:
    _database = None

def verify():
    db_path = 'test_verify.sqlite'
    if os.path.exists(db_path): os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE employment_province_table (Province TEXT, Male REAL, Female REAL, Year REAL)')
    cursor.execute('INSERT INTO employment_province_table VALUES ("Harare", 1000, 2000, 2025)')
    cursor.execute('CREATE TABLE gdp_long (Date REAL, indicator TEXT, value REAL)')
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
        print(f"Keywords search ['employment', 'province']: {app.find_tables_by_keywords(['employment', 'province'])}")
        
        print("Testing Wide Format (Labour)...")
        filters = {'year': '2025', 'region': 'Harare'}
        labour = app.query_labour_kpis(filters)
        print(f"Employed: {labour['employed']} (Expected 3000)")
        if labour['employed'] != 3000:
            # Let's see what find_data_total is doing
            tbl = 'employment_province_table'
            cols = app.guess_column_names(tbl)
            print(f"Columns for {tbl}: {cols}")
            raise Exception(f"Labour Wide test failed: {labour['employed']}")
        
        print("Testing Long Format (GDP)...")
        gdp_filters = {'year': '2025'}
        gdp = app.query_gdp_kpis(gdp_filters)
        print(f"GDP: {gdp['gdp']}B (Expected 50.0B)")
        if gdp['gdp'] != 50.0: raise Exception(f"GDP Long test failed: {gdp['gdp']}")
        
        print("Verification Successful!")

if __name__ == '__main__':
    try:
        verify()
    except Exception as e:
        print(f"Verification Failed: {e}")
        sys.exit(1)
    finally:
        if os.path.exists('test_verify.sqlite'): os.remove('test_verify.sqlite')
