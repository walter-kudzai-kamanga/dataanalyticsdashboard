import sqlite3
import csv
import io
import json
import re
from datetime import datetime
from flask import (
    Flask, render_template, request, jsonify, session,
    send_file, g
)
import pandas as pd

app = Flask(__name__)
app.secret_key = 'replace-this-with-a-secret-key-for-production'

DATABASE = 'zimstats.sqlite'

# ----------------------------------------------------------------------
# Database helpers
# ----------------------------------------------------------------------
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def execute_db(query, args=()):
    conn = get_db()
    cur = conn.execute(query, args)
    conn.commit()
    last_id = cur.lastrowid
    cur.close()
    return last_id

# ----------------------------------------------------------------------
# Ensure uploads table exists (direct connection)
# ----------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DATABASE)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT NOT NULL,
            upload_time TIMESTAMP NOT NULL,
            filename TEXT,
            data_json TEXT NOT NULL,
            table_name TEXT,
            sheet_name TEXT,
            rows_count INTEGER,
            columns_count INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS upload_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_id INTEGER,
            table_name TEXT NOT NULL,
            sheet_name TEXT,
            domain TEXT,
            filename TEXT,
            upload_time TIMESTAMP,
            rows_count INTEGER,
            columns_count INTEGER,
            columns_info TEXT,
            FOREIGN KEY (upload_id) REFERENCES data_uploads(id)
        )
    """)
    conn.commit()
    conn.close()

init_db()

# ----------------------------------------------------------------------
# Helper: get latest uploaded data for a domain
# ----------------------------------------------------------------------
def get_uploaded_data(domain):
    row = query_db(
        "SELECT data_json FROM data_uploads WHERE domain = ? ORDER BY upload_time DESC LIMIT 1",
        (domain,),
        one=True
    )
    if row:
        return json.loads(row['data_json'])
    return None

# ----------------------------------------------------------------------
# Dynamic query helpers (table discovery, column guessing)
# ----------------------------------------------------------------------
def get_all_table_names():
    rows = query_db("SELECT name FROM sqlite_master WHERE type='table'")
    return [row['name'] for row in rows]

def guess_column_names(table_name):
    try:
        cur = get_db().execute(f'SELECT * FROM "{table_name}" LIMIT 1')
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        return colnames
    except:
        return []

def find_tables_by_keywords(keywords, mode='all'):
    """Return tables that contain all (or any) of the keywords in their name."""
    tables = get_all_table_names()
    matches = []
    for tbl in tables:
        tbl_lower = tbl.lower()
        if mode == 'all':
            if all(kw.lower() in tbl_lower for kw in keywords):
                matches.append(tbl)
        else:  # any
            if any(kw.lower() in tbl_lower for kw in keywords):
                matches.append(tbl)
    return matches

def safe_float(val):
    try:
        return float(val)
    except:
        return 0.0

# ----------------------------------------------------------------------
# Labour domain – real queries
# ----------------------------------------------------------------------
def query_labour_kpis(filters):
    """Extract total employed, unemployed, labour force, unemployment rate."""
    year = filters.get('year')
    region = filters.get('region')
    gender = filters.get('gender')
    age = filters.get('age')

    employed = unemployed = labour_force = None

    # First, try to get employment from WIDE EMPLOYMENT BY PROVINCE table (has Male/Female columns)
    emp_tables = find_tables_by_keywords(['employment', 'province', 'sex'], mode='any')
    for tbl in emp_tables:
        cols = guess_column_names(tbl)
        # Check if this is the WIDE table with Male/Female columns
        if 'Male' in cols and 'Female' in cols and 'Province' in cols:
            try:
                # Apply filters
                q = f'SELECT SUM("Male" + "Female") FROM "{tbl}"'
                params = []
                conditions = []
                
                if region and region != 'All' and 'Province' in cols:
                    conditions.append('"Province" = ?')
                    params.append(region)
                
                if gender and gender in ['Male', 'Female']:
                    # Use only the specified gender column
                    q = f'SELECT SUM("{gender}") FROM "{tbl}"'
                    if conditions:
                        q += ' WHERE ' + ' AND '.join(conditions)
                elif conditions:
                    q += ' WHERE ' + ' AND '.join(conditions)
                
                res = query_db(q, params, one=True)
                if res and res[0]:
                    employed = safe_float(res[0])
                    break
            except Exception as e:
                continue

    # Get unemployment and labour force from QLFS table
    qlfs_tables = find_tables_by_keywords(['qlfs', 'province'], mode='any')
    for tbl in qlfs_tables:
        cols = guess_column_names(tbl)
        if 'Indicator' in cols and 'Value' in cols:
            try:
                # Build query with filters
                conditions = ['"Indicator" = ?']
                params = ['unemployed']
                
                if region and region != 'All' and 'Province' in cols:
                    conditions.append('"Province" = ?')
                    params.append(region)
                
                if gender and 'Sex' in cols:
                    conditions.append('"Sex" = ?')
                    params.append(gender.lower())
                
                q = f'SELECT SUM("Value") FROM "{tbl}" WHERE ' + ' AND '.join(conditions)
                res = query_db(q, params, one=True)
                if res and res[0]:
                    unemployed = safe_float(res[0])

                # Get total labour force with same filters
                params[0] = 'labour_force'
                q = f'SELECT SUM("Value") FROM "{tbl}" WHERE ' + ' AND '.join(conditions)
                res = query_db(q, params, one=True)
                if res and res[0]:
                    labour_force = safe_float(res[0])
                    break
            except Exception as e:
                continue

    # Fallback if not found
    if employed is None:
        employed = 5821  # fallback (thousands)
    if unemployed is None:
        unemployed = 550  # fallback
    if labour_force is None:
        labour_force = employed + unemployed if employed and unemployed else employed * 1.087  # approx
    unemp_rate = (unemployed / labour_force * 100) if labour_force and unemployed else 8.7

    return {
        'employed': employed,
        'unemployed': unemployed,
        'labour_force': labour_force,
        'unemp_rate': unemp_rate
    }

def query_labour_by_province(filters):
    """Employment by province (for donut chart)."""
    region = filters.get('region')
    gender = filters.get('gender')
    
    tables = find_tables_by_keywords(['employment', 'province', 'sex'], mode='any')
    prov_data = {}
    for tbl in tables:
        cols = guess_column_names(tbl)
        # Look for WIDE table with Male/Female columns
        if 'Male' in cols and 'Female' in cols and 'Province' in cols:
            try:
                if gender and gender in ['Male', 'Female']:
                    # Filter by gender
                    rows = query_db(f'SELECT "Province", SUM("{gender}") FROM "{tbl}" GROUP BY "Province"')
                else:
                    # Sum both genders
                    rows = query_db(f'SELECT "Province", SUM("Male" + "Female") FROM "{tbl}" GROUP BY "Province"')
                
                for r in rows:
                    prov = r[0]
                    val = safe_float(r[1])
                    if prov and val:
                        # Apply region filter if specified
                        if region and region != 'All':
                            if prov == region:
                                prov_data[prov] = prov_data.get(prov, 0) + val
                        else:
                            prov_data[prov] = prov_data.get(prov, 0) + val
                break  # Found the right table
            except Exception as e:
                continue
    if prov_data:
        # Sort by value, take top 5
        top = sorted(prov_data.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0] for t in top]
        data = [t[1] for t in top]
    else:
        # Fallback
        labels = ['Harare', 'Bulawayo', 'Manicaland', 'Mash East', 'Other']
        data = [28, 12, 15, 14, 31]
    return labels, data

# ----------------------------------------------------------------------
# National Accounts (GDP) – real queries
# ----------------------------------------------------------------------
def query_gdp_kpis(filters):
    """Extract GDP, growth, per capita, sector share."""
    year = filters.get('year', '2025')
    try:
        year_float = float(year)
    except:
        year_float = 2025.0
    
    gdp_tables = find_tables_by_keywords(['gdp', 'provincial'], mode='any')
    total_gdp = None
    prev_gdp = None
    per_capita = None

    # Look for WIDE PROV GDP ALL YEARS table with Gdp At Market Prices Usd column
    for tbl in gdp_tables:
        cols = guess_column_names(tbl)
        if 'Gdp At Market Prices Usd' in cols and 'Date' in cols:
            try:
                # Apply region filter if specified
                region = filters.get('region')
                conditions = ['"Date" = ?', '"Gdp At Market Prices Usd" IS NOT NULL']
                params = [year_float]
                
                if region and region != 'All' and 'Province' in cols:
                    conditions.append('"Province" = ?')
                    params.append(region)
                
                # Sum GDP for current year (check if data exists)
                q = f'SELECT SUM("Gdp At Market Prices Usd") FROM "{tbl}" WHERE ' + ' AND '.join(conditions)
                res = query_db(q, params, one=True)
                if res and res[0]:
                    total_gdp = safe_float(res[0]) / 1e9  # convert to billions
                else:
                    # Try to get most recent year with data
                    q_conditions = ['"Gdp At Market Prices Usd" IS NOT NULL']
                    if region and region != 'All' and 'Province' in cols:
                        q_conditions.append('"Province" = ?')
                    q = f'SELECT MAX("Date") FROM "{tbl}" WHERE ' + ' AND '.join(q_conditions)
                    res = query_db(q, [region] if region and region != 'All' else [], one=True)
                    if res and res[0]:
                        latest_year = float(res[0])
                        params[0] = latest_year
                        q = f'SELECT SUM("Gdp At Market Prices Usd") FROM "{tbl}" WHERE ' + ' AND '.join(conditions)
                        res = query_db(q, params, one=True)
                        if res and res[0]:
                            total_gdp = safe_float(res[0]) / 1e9
                            year_float = latest_year  # Update to use the year with data
                
                # Get per capita GDP
                if 'Per Capita Gdp In Usd' in cols and total_gdp:
                    q_conditions = ['"Date" = ?', '"Per Capita Gdp In Usd" IS NOT NULL']
                    q_params = [year_float]
                    if region and region != 'All' and 'Province' in cols:
                        q_conditions.append('"Province" = ?')
                        q_params.append(region)
                    q = f'SELECT AVG("Per Capita Gdp In Usd") FROM "{tbl}" WHERE ' + ' AND '.join(q_conditions)
                    res = query_db(q, q_params, one=True)
                    if res and res[0]:
                        per_capita = safe_float(res[0])
                
                # Previous year for growth
                if total_gdp:
                    prev_year = year_float - 1
                    prev_conditions = ['"Date" = ?', '"Gdp At Market Prices Usd" IS NOT NULL']
                    prev_params = [prev_year]
                    if region and region != 'All' and 'Province' in cols:
                        prev_conditions.append('"Province" = ?')
                        prev_params.append(region)
                    q = f'SELECT SUM("Gdp At Market Prices Usd") FROM "{tbl}" WHERE ' + ' AND '.join(prev_conditions)
                    res = query_db(q, prev_params, one=True)
                    if res and res[0]:
                        prev_gdp = safe_float(res[0]) / 1e9
                break
            except Exception as e:
                continue

    if total_gdp is None:
        total_gdp = 32.4   # billion USD fallback
    if prev_gdp is None:
        growth = 2.3
    else:
        growth = ((total_gdp - prev_gdp) / prev_gdp * 100) if prev_gdp else 2.3
    if per_capita is None:
        per_capita = total_gdp * 1e9 / 15.0e6 if total_gdp else 1.987  # rough population estimate

    return {
        'gdp': total_gdp,
        'growth': growth,
        'per_capita': per_capita,
        'agri_share': 11.2   # could query sector tables
    }

def query_gdp_by_sector(filters):
    """Sector composition of GDP."""
    year = filters.get('year')
    region = filters.get('region')
    
    # Look for tables with sector/industry breakdown
    sector_tables = find_tables_by_keywords(['gdp', 'sector', 'industry'], mode='any')
    sector_data = {}
    for tbl in sector_tables:
        cols = guess_column_names(tbl)
        val_col = next((c for c in cols if any(x in c.lower() for x in ['gdp', 'value', 'share'])), None)
        sec_col = next((c for c in cols if any(x in c.lower() for x in ['sector', 'industry'])), None)
        if val_col and sec_col:
            try:
                # Build query with filters
                conditions = []
                params = []
                
                if year and 'Year' in cols:
                    conditions.append('"Year" = ?')
                    params.append(int(float(year)))
                elif year and 'Date' in cols:
                    conditions.append('"Date" = ?')
                    params.append(float(year))
                
                if region and region != 'All' and 'Province' in cols:
                    conditions.append('"Province" = ?')
                    params.append(region)
                
                where_clause = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
                q = f'SELECT "{sec_col}", SUM("{val_col}") FROM "{tbl}"{where_clause} GROUP BY "{sec_col}"'
                rows = query_db(q, params)
                for r in rows:
                    sec = r[0]
                    val = safe_float(r[1])
                    if sec and val:
                        sector_data[sec] = sector_data.get(sec, 0) + val
                if sector_data:
                    break
            except:
                continue
    if sector_data:
        top = sorted(sector_data.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0] for t in top]
        data = [t[1] for t in top]
    else:
        labels = ['Services', 'Agriculture', 'Manufacturing', 'Mining', 'Construction']
        data = [52, 11, 14, 12, 11]
    return labels, data

# ----------------------------------------------------------------------
# Prices (CPI / Inflation) – real queries
# ----------------------------------------------------------------------
def query_cpi_kpis(filters):
    """CPI index, MoM, YoY inflation."""
    year = filters.get('year')
    cpi_tables = find_tables_by_keywords(['cpi', 'inflation'], mode='any')
    cpi_value = None
    yoy_inflation = None
    mom_inflation = None
    
    # Look for LONG CPI WEIGHTED ANNUAL SUMMARY table
    for tbl in cpi_tables:
        cols = guess_column_names(tbl)
        if 'Category' in cols and 'Item' in cols and 'Value' in cols:
            try:
                # Apply year filter if specified
                if year:
                    # Category might be year (e.g., "2024", "2025")
                    row = query_db(f'SELECT "Value" FROM "{tbl}" WHERE "Item" = ? AND "Category" = ? ORDER BY "Category" DESC LIMIT 1', ['all_items', str(year)], one=True)
                    if row:
                        cpi_value = safe_float(row['Value'])
                        break
                
                # Fallback: Get latest CPI all_items value
                if cpi_value is None:
                    row = query_db(f'SELECT "Value" FROM "{tbl}" WHERE "Item" = ? ORDER BY "Category" DESC LIMIT 1', ['all_items'], one=True)
                    if row:
                        cpi_value = safe_float(row['Value'])
                        break
            except Exception as e:
                continue
    
    # Try to get YoY and MoM inflation from WIDE CPI WEIGHTED MONTHLY AND YEARLY INFLATION table
    for tbl in cpi_tables:
        cols = guess_column_names(tbl)
        if 'Inflation.Rate.Percent.Annual' in cols and 'Inflation.Rate.Percent.Monthly' in cols:
            try:
                # Get most recent YoY and MoM inflation
                row = query_db(f'SELECT "Inflation.Rate.Percent.Annual", "Inflation.Rate.Percent.Monthly" FROM "{tbl}" ORDER BY rowid DESC LIMIT 1', one=True)
                if row:
                    yoy_inflation = safe_float(row['Inflation.Rate.Percent.Annual'])
                    mom_inflation = safe_float(row['Inflation.Rate.Percent.Monthly'])
                    break
            except Exception as e:
                continue
    
    # Fallback: try LONG table with Indicator column
    if yoy_inflation is None:
        for tbl in cpi_tables:
            cols = guess_column_names(tbl)
            if 'Indicator' in cols and 'Value' in cols:
                try:
                    # Get annual inflation rate
                    row = query_db(f'SELECT "Value" FROM "{tbl}" WHERE "Indicator" = ? ORDER BY rowid DESC LIMIT 1', ['inflation_rate_percent_annual'], one=True)
                    if row:
                        yoy_inflation = safe_float(row['Value'])
                    # Get monthly inflation rate
                    row = query_db(f'SELECT "Value" FROM "{tbl}" WHERE "Indicator" = ? ORDER BY rowid DESC LIMIT 1', ['inflation_rate_percent_monthly'], one=True)
                    if row:
                        mom_inflation = safe_float(row['Value'])
                    if yoy_inflation:
                        break
                except:
                    pass
    
    if cpi_value is None:
        cpi_value = 105.2
    if yoy_inflation is None:
        yoy_inflation = 12.1
    if mom_inflation is None:
        mom_inflation = 0.8
    
    return {
        'cpi': cpi_value,
        'mom': mom_inflation,
        'yoy': yoy_inflation,
        'food': 13.5
    }

# ----------------------------------------------------------------------
# Trade – real queries
# ----------------------------------------------------------------------
def query_trade_kpis(filters):
    """Exports, imports, balance."""
    year = filters.get('year')
    trade_tables = find_tables_by_keywords(['trade', 'summary'], mode='any')
    exports = imports = None

    # Look for TRADE SUMMARY table with Total.Exports and Imports columns
    for tbl in trade_tables:
        cols = guess_column_names(tbl)
        if 'Total.Exports' in cols and 'Imports' in cols:
            try:
                # Apply year filter if Period column exists
                if 'Period' in cols and year:
                    # Try to match year in Period (e.g., "Jan-2023")
                    year_str = str(year)
                    rows = query_db(f'SELECT "Total.Exports", "Imports" FROM "{tbl}" WHERE "Period" LIKE ? ORDER BY "Period" DESC LIMIT 1', [f'%-{year_str}'], one=True)
                    if rows:
                        exports = safe_float(rows['Total.Exports']) / 1e6
                        imports = safe_float(rows['Imports']) / 1e6
                        break
                
                # Fallback: Get latest trade data
                if exports is None:
                    rows = query_db(f'SELECT "Total.Exports", "Imports" FROM "{tbl}" ORDER BY rowid DESC LIMIT 1', one=True)
                    if rows:
                        exports = safe_float(rows['Total.Exports']) / 1e6  # convert to millions
                        imports = safe_float(rows['Imports']) / 1e6
                        break
            except Exception as e:
                continue
    
    # Fallback: try export/import value tables
    if exports is None or imports is None:
        exp_tables = find_tables_by_keywords(['export', 'value'], mode='any')
        for tbl in exp_tables:
            cols = guess_column_names(tbl)
            if 'Value' in cols:
                try:
                    res = query_db(f'SELECT SUM("Value") FROM "{tbl}"', one=True)
                    if res and res[0]:
                        exports = safe_float(res[0]) / 1e6
                        break
                except:
                    pass
        
        imp_tables = find_tables_by_keywords(['import', 'value'], mode='any')
        for tbl in imp_tables:
            cols = guess_column_names(tbl)
            if 'Value' in cols:
                try:
                    res = query_db(f'SELECT SUM("Value") FROM "{tbl}"', one=True)
                    if res and res[0]:
                        imports = safe_float(res[0]) / 1e6
                        break
                except:
                    pass

    if exports is None:
        exports = 4210
    if imports is None:
        imports = 5890
    balance = exports - imports
    cover = (exports / imports * 100) if imports else 71.5

    return {
        'exports': exports,
        'imports': imports,
        'balance': balance,
        'cover': cover
    }

def query_imports_by_province():
    """Imports by province for trade extra chart."""
    # Note: Import data by province may not be available in the database
    # Using employee earnings by province as proxy or fallback
    imp_tables = find_tables_by_keywords(['import', 'province'], mode='all')
    prov_imports = {}
    for tbl in imp_tables:
        cols = guess_column_names(tbl)
        val_col = next((c for c in cols if 'value' in c.lower() or 'import' in c.lower()), None)
        prov_col = next((c for c in cols if 'province' in c.lower()), None)
        if val_col and prov_col:
            try:
                rows = query_db(f'SELECT "{prov_col}", SUM("{val_col}") FROM "{tbl}" GROUP BY "{prov_col}"')
                for r in rows:
                    prov = r[0]
                    val = safe_float(r[1])
                    if prov and val:
                        prov_imports[prov] = prov_imports.get(prov, 0) + val
            except:
                continue
    
    # If no import data, try employee earnings by province as proxy
    if not prov_imports:
        emp_tables = find_tables_by_keywords(['employee', 'earnings', 'province'], mode='any')
        for tbl in emp_tables:
            cols = guess_column_names(tbl)
            if 'Province' in cols and 'Value' in cols:
                try:
                    rows = query_db(f'SELECT "Province", SUM("Value") FROM "{tbl}" GROUP BY "Province"')
                    for r in rows:
                        prov = r[0]
                        val = safe_float(r[1]) / 1e6  # convert to millions
                        if prov and val:
                            prov_imports[prov] = prov_imports.get(prov, 0) + val
                    break
                except:
                    continue
    
    if prov_imports:
        top = sorted(prov_imports.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0] for t in top]
        data = [t[1] for t in top]
    else:
        labels = ['Harare', 'Bulawayo', 'Manicaland', 'Mash West', 'Other']
        data = [1800, 620, 450, 380, 2640]
    return labels, data

# ----------------------------------------------------------------------
# Domain assemblers – now with real DB queries + fallback
# ----------------------------------------------------------------------
def get_dashboard_data(domain, filters):
    # First check for uploaded data (JSON format - backward compatibility)
    uploaded = get_uploaded_data(domain)
    if uploaded:
        return _build_from_upload(domain, uploaded, filters)
    
    # Check for uploaded tables in database
    uploaded_table = get_uploaded_table(domain, filters)
    if uploaded_table:
        return _build_from_uploaded_table(domain, uploaded_table, filters)

    # Otherwise fetch from database
    if domain == 'labour':
        return assemble_labour(filters)
    elif domain == 'accounts':
        return assemble_accounts(filters)
    elif domain == 'prices':
        return assemble_prices(filters)
    elif domain == 'trade':
        return assemble_trade(filters)
    elif domain == 'dashboard':
        return assemble_overview(filters)
    else:
        return fallback_data()

def get_uploaded_table(domain, filters):
    """Get the most recent uploaded table for a domain."""
    row = query_db(
        """SELECT table_name, columns_info FROM upload_metadata 
           WHERE domain = ? ORDER BY upload_time DESC LIMIT 1""",
        (domain,),
        one=True
    )
    if row:
        return {
            'table_name': row['table_name'],
            'columns_info': json.loads(row['columns_info']) if row['columns_info'] else {}
        }
    return None

def _build_from_uploaded_table(domain, table_info, filters):
    """Build dashboard data from uploaded database table."""
    table_name = table_info['table_name']
    columns_info = table_info.get('columns_info', {})
    
    try:
        # Build query with filters
        where_clauses = []
        params = []
        
        search_term = filters.get('search', '').strip()
        if search_term:
            # Search across all text columns
            text_cols = columns_info.get('categorical_columns', [])
            if text_cols:
                search_conditions = []
                for col in text_cols[:3]:  # Limit to first 3 text columns
                    col_clean = sanitize_table_name(str(col))
                    search_conditions.append(f'"{col_clean}" LIKE ?')
                    params.append(f'%{search_term}%')
                if search_conditions:
                    where_clauses.append('(' + ' OR '.join(search_conditions) + ')')
        
        # Apply year filter if Year column exists
        year = filters.get('year')
        if year:
            try:
                # Check if table has Year column
                cols = guess_column_names(table_name)
                year_cols = [c for c in cols if 'year' in c.lower()]
                if year_cols:
                    where_clauses.append(f'"{year_cols[0]}" = ?')
                    params.append(int(float(year)))
            except:
                pass
        
        # Apply region filter if Province/Region column exists
        region = filters.get('region')
        if region and region != 'All':
            try:
                cols = guess_column_names(table_name)
                region_cols = [c for c in cols if any(x in c.lower() for x in ['province', 'region', 'area'])]
                if region_cols:
                    where_clauses.append(f'"{region_cols[0]}" = ?')
                    params.append(region)
            except:
                pass
        
        where_sql = ' WHERE ' + ' AND '.join(where_clauses) if where_clauses else ''
        
        # Get data
        rows = query_db(f'SELECT * FROM "{table_name}"{where_sql} LIMIT 1000', tuple(params))
        
        if not rows:
            return fallback_data()
        
        # Convert to DataFrame-like structure
        df_data = []
        for row in rows:
            df_data.append(dict(row))
        
        # Build KPIs from numeric columns
        numeric_cols = columns_info.get('numeric_columns', [])
        kpis = []
        for i, col in enumerate(numeric_cols[:8]):
            try:
                col_clean = sanitize_table_name(str(col))
                total_row = query_db(f'SELECT SUM("{col_clean}") as total FROM "{table_name}"{where_sql}', tuple(params), one=True)
                if total_row and total_row['total']:
                    total = safe_float(total_row['total'])
                    kpis.append({'label': col[:30], 'value': f'{total:,.0f}'})
            except:
                pass
        
        while len(kpis) < 4:
            kpis.append({'label': 'No data', 'value': '0'})
        
        # Build charts
        if numeric_cols:
            col_clean = sanitize_table_name(str(numeric_cols[0]))
            chart_data = query_db(f'SELECT "{col_clean}" FROM "{table_name}"{where_sql} ORDER BY rowid LIMIT 20', tuple(params))
            main_chart = {
                'title': f'{numeric_cols[0][:30]} Trend',
                'type': 'line',
                'labels': list(range(1, min(21, len(chart_data)+1))),
                'data': [safe_float(r[col_clean]) for r in chart_data] if chart_data else []
            }
        else:
            main_chart = {
                'title': 'Data Overview',
                'type': 'line',
                'labels': [],
                'data': []
            }
        
        # Build side chart from categorical data
        cat_cols = columns_info.get('categorical_columns', [])
        if cat_cols:
            col_clean = sanitize_table_name(str(cat_cols[0]))
            cat_data = query_db(f'SELECT "{col_clean}", COUNT(*) as cnt FROM "{table_name}"{where_sql} GROUP BY "{col_clean}" ORDER BY cnt DESC LIMIT 5', tuple(params))
            if cat_data:
                side_chart = {
                    'title': f'{cat_cols[0][:30]} Distribution',
                    'type': 'doughnut',
                    'labels': [str(r[col_clean]) for r in cat_data],
                    'data': [r['cnt'] for r in cat_data]
                }
            else:
                side_chart = {'title': 'No data', 'type': 'doughnut', 'labels': [], 'data': []}
        else:
            side_chart = {'title': 'No categorical data', 'type': 'doughnut', 'labels': ['No data'], 'data': [1]}
        
        # Build table
        columns = list(df_data[0].keys()) if df_data else []
        table_rows = df_data[:100]  # Limit to 100 rows for display
        
        insights = [
            f"Uploaded data: {len(rows)} rows, {len(columns)} columns",
            f"Table: {table_name}",
            f"Numeric columns: {len(numeric_cols)}",
            f"Categorical columns: {len(cat_cols)}"
        ]
        
        return {
            'kpis': kpis,
            'charts': {'main': main_chart, 'side': side_chart, 'imports': None},
            'table': {'columns': columns, 'rows': table_rows},
            'insights': insights,
            'title': f'{domain.title()} – Uploaded Data'
        }
        
    except Exception as e:
        return fallback_data()

def assemble_labour(filters):
    kpi_data = query_labour_kpis(filters)
    prov_labels, prov_data = query_labour_by_province(filters)
    sector_labels, sector_data = query_sector_distribution(filters)
    informal = query_informal_employment(filters)
    neet = query_youth_neet(filters)
    
    # Calculate percentages
    informal_pct = (informal / kpi_data['employed'] * 100) if kpi_data['employed'] else 0
    lfpr = (kpi_data['labour_force'] / (kpi_data['labour_force'] + neet) * 100) if (kpi_data['labour_force'] + neet) else 62.3
    neet_pct = (neet / (kpi_data['labour_force'] + neet) * 100) if (kpi_data['labour_force'] + neet) else 0

    kpis = [
        {'label': 'Labour force (thousands)', 'value': f"{kpi_data['labour_force']:,.0f}"},
        {'label': 'Employment (thousands)', 'value': f"{kpi_data['employed']:,.0f}"},
        {'label': 'Unemployment rate', 'value': f"{kpi_data['unemp_rate']:.1f}%"},
        {'label': 'LFPR', 'value': f"{lfpr:.1f}%"},
        {'label': 'Informal sector', 'value': f"{informal_pct:.1f}%"},
        {'label': 'Youth NEET', 'value': f"{neet:,.0f}"},
        {'label': 'Unemployed', 'value': f"{kpi_data['unemployed']:,.0f}"},
        {'label': 'Employment rate', 'value': f"{(kpi_data['employed']/kpi_data['labour_force']*100):.1f}%"},
    ]

    main_chart = {
        'title': 'Employment by industry sector',
        'type': 'bar',
        'labels': sector_labels,
        'data': sector_data
    }

    side_chart = {
        'title': 'Employment by province',
        'type': 'doughnut',
        'labels': prov_labels,
        'data': prov_data
    }

    columns = ['Province', 'Employed', 'Unemployed', 'Unemployment Rate']
    rows = []
    # Get province-level data
    qlfs_tables = find_tables_by_keywords(['qlfs', 'province'], mode='any')
    for tbl in qlfs_tables:
        cols = guess_column_names(tbl)
        if 'Province' in cols and 'Indicator' in cols and 'Value' in cols:
            try:
                prov_data_dict = {}
                for prov in prov_labels:
                    emp_row = query_db(f'SELECT "Value" FROM "{tbl}" WHERE "Province" = ? AND "Indicator" = ?', [prov, 'unemployed'], one=True)
                    unemp_row = query_db(f'SELECT "Value" FROM "{tbl}" WHERE "Province" = ? AND "Indicator" = ?', [prov, 'unemployment_rate'], one=True)
                    if emp_row:
                        prov_data_dict[prov] = {
                            'unemployed': safe_float(emp_row['Value']),
                            'unemp_rate': safe_float(unemp_row['Value']) if unemp_row else 0
                        }
                
                for i, prov in enumerate(prov_labels):
                    if prov in prov_data_dict:
                        rows.append({
                            'Province': prov,
                            'Employed': f"{prov_data[i]:,.0f}",
                            'Unemployed': f"{prov_data_dict[prov]['unemployed']:,.0f}",
                            'Unemployment Rate': f"{prov_data_dict[prov]['unemp_rate']:.1f}%"
                        })
                    else:
                        rows.append({
                            'Province': prov,
                            'Employed': f"{prov_data[i]:,.0f}",
                            'Unemployed': 'N/A',
                            'Unemployment Rate': 'N/A'
                        })
                break
            except:
                pass
    
    if not rows:
        rows = [{'Province': prov_labels[i], 'Employed': f"{prov_data[i]:,.0f}", 'Unemployed': 'N/A', 'Unemployment Rate': 'N/A'} for i in range(len(prov_labels))]

    insights = [
        f"Total employed: {kpi_data['employed']:,.0f} thousand people",
        f"Unemployment rate: {kpi_data['unemp_rate']:.1f}%",
        f"Informal sector accounts for {informal_pct:.1f}% of total employment",
        f"Youth NEET population: {neet:,.0f}",
        f"Labour force participation rate: {lfpr:.1f}%"
    ]

    sector_chart = {
        'title': 'Employment by Industry Sector',
        'type': 'bar',
        'labels': sector_labels,
        'data': sector_data
    }

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': None, 'sector': sector_chart},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'Labour Market Statistics'
    }

def assemble_accounts(filters):
    gdp_data = query_gdp_kpis(filters)
    sector_labels, sector_data = query_gdp_by_sector(filters)
    gdp_years, gdp_values = query_gdp_timeseries()
    
    # Calculate GDP per capita growth
    prev_per_capita = gdp_data['per_capita'] / (1 + gdp_data['growth']/100) if gdp_data['growth'] else gdp_data['per_capita']
    per_capita_growth = ((gdp_data['per_capita'] - prev_per_capita) / prev_per_capita * 100) if prev_per_capita else 0

    kpis = [
        {'label': 'GDP (current US$ B)', 'value': f"{gdp_data['gdp']:.1f}"},
        {'label': 'GDP per capita (US$)', 'value': f"{gdp_data['per_capita']:,.0f}"},
        {'label': 'GDP growth (annual)', 'value': f"{gdp_data['growth']:.1f}%"},
        {'label': 'Agriculture share', 'value': f"{gdp_data['agri_share']:.1f}%"},
        {'label': 'Per capita growth', 'value': f"{per_capita_growth:.1f}%"},
        {'label': 'GDP (ZWL B)', 'value': f"{gdp_data['gdp'] * 1.2:.1f}"},  # Approximate conversion
        {'label': 'Services share', 'value': f"{100 - gdp_data['agri_share'] - 20:.1f}%"},
        {'label': 'GDP trend', 'value': '↑' if gdp_data['growth'] > 0 else '↓'},
    ]

    if not gdp_years:
        gdp_years = ['2020', '2021', '2022', '2023', '2024']
        gdp_values = [32.0, 33.5, 34.2, 35.1, gdp_data['gdp']]

    main_chart = {
        'title': 'GDP Trend (US$ Billions)',
        'type': 'line',
        'labels': gdp_years,
        'data': gdp_values
    }

    side_chart = {
        'title': 'GDP by sector',
        'type': 'doughnut',
        'labels': sector_labels,
        'data': sector_data
    }

    columns = ['Sector', 'Value (US$M)', 'Share (%)']
    total_sector = sum(sector_data) if sector_data else 1
    rows = [{
        'Sector': sector_labels[i],
        'Value (US$M)': f"{sector_data[i]:,.0f}",
        'Share (%)': f"{(sector_data[i]/total_sector*100):.1f}"
    } for i in range(len(sector_labels))]

    insights = [
        f"GDP: US$ {gdp_data['gdp']:.1f} billion (current prices)",
        f"Annual growth rate: {gdp_data['growth']:.1f}%",
        f"GDP per capita: US$ {gdp_data['per_capita']:,.0f}",
        f"Agriculture contributes {gdp_data['agri_share']:.1f}% to GDP",
        'Mining and services are key economic drivers'
    ]

    sector_chart = {
        'title': 'GDP by Sector Breakdown',
        'type': 'bar',
        'labels': sector_labels,
        'data': sector_data
    }

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': None, 'sector': sector_chart},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'National Accounts & GDP'
    }

def assemble_prices(filters):
    cpi_data = query_cpi_kpis(filters)
    
    # Get CPI time series
    cpi_tables = find_tables_by_keywords(['cpi', 'weighted', 'index'], mode='any')
    cpi_months = []
    cpi_values = []
    
    for tbl in cpi_tables:
        cols = guess_column_names(tbl)
        if 'Category' in cols and 'Item' in cols and 'Value' in cols:
            try:
                rows = query_db(f'SELECT "Category", "Value" FROM "{tbl}" WHERE "Item" = ? ORDER BY "Category" DESC LIMIT 12', ['all_items'])
                for r in rows:
                    cpi_months.insert(0, str(r['Category'])[:7] if len(str(r['Category'])) > 7 else str(r['Category']))
                    cpi_values.insert(0, safe_float(r['Value']))
                break
            except:
                pass
    
    if not cpi_months:
        cpi_months = ['Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun']
        cpi_values = [98,99,100,101,102,103,104,105,105,106,107,108]

    # Calculate real inflation impact
    real_interest = 0  # Would need interest rate data
    core_inflation = cpi_data['yoy'] * 0.85  # Estimate

    kpis = [
        {'label': 'CPI (All items)', 'value': f"{cpi_data['cpi']:.1f}"},
        {'label': 'Inflation (MoM)', 'value': f"{cpi_data['mom']:.2f}%"},
        {'label': 'Inflation (YoY)', 'value': f"{cpi_data['yoy']:.1f}%"},
        {'label': 'Food inflation', 'value': f"{cpi_data['food']:.1f}%"},
        {'label': 'Core inflation', 'value': f"{core_inflation:.1f}%"},
        {'label': 'CPI base (2020=100)', 'value': '100.0'},
        {'label': 'Price level change', 'value': f"{((cpi_data['cpi']-100)/100*100):.1f}%"},
        {'label': 'Monthly change', 'value': f"{cpi_data['mom']:.2f}%"},
    ]

    main_chart = {
        'title': 'CPI Trend (Index)',
        'type': 'line',
        'labels': cpi_months[-12:] if len(cpi_months) > 12 else cpi_months,
        'data': cpi_values[-12:] if len(cpi_values) > 12 else cpi_values
    }

    side_chart = {
        'title': 'Inflation contribution',
        'type': 'doughnut',
        'labels': ['Food','Housing','Transport','Other'],
        'data': [42,18,15,25]
    }

    columns = ['Period', 'CPI Index', 'MoM Change', 'YoY Change']
    rows = [
        {
            'Period': 'Latest',
            'CPI Index': f"{cpi_data['cpi']:.1f}",
            'MoM Change': f"{cpi_data['mom']:.2f}%",
            'YoY Change': f"{cpi_data['yoy']:.1f}%"
        }
    ]

    insights = [
        f"CPI Index: {cpi_data['cpi']:.1f} (base year = 100)",
        f"Year-on-year inflation: {cpi_data['yoy']:.1f}%",
        f"Month-on-month inflation: {cpi_data['mom']:.2f}%",
        f"Food inflation: {cpi_data['food']:.1f}% (higher than overall)",
        f"Core inflation estimate: {core_inflation:.1f}%"
    ]

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': None},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'Prices & Inflation Statistics'
    }

def assemble_trade(filters):
    trade_data = query_trade_kpis(filters)
    imp_labels, imp_data = query_imports_by_province()
    periods, exports_ts, imports_ts = query_trade_timeseries(filters)
    
    # Calculate trade metrics
    trade_deficit = abs(trade_data['balance']) if trade_data['balance'] < 0 else 0
    export_growth = ((exports_ts[-1] - exports_ts[-2]) / exports_ts[-2] * 100) if len(exports_ts) >= 2 else 0
    import_growth = ((imports_ts[-1] - imports_ts[-2]) / imports_ts[-2] * 100) if len(imports_ts) >= 2 else 0

    kpis = [
        {'label': 'Exports (US$ M)', 'value': f"{trade_data['exports']:,.0f}"},
        {'label': 'Imports (US$ M)', 'value': f"{trade_data['imports']:,.0f}"},
        {'label': 'Trade balance (US$ M)', 'value': f"{trade_data['balance']:,.0f}"},
        {'label': 'Cover ratio', 'value': f"{trade_data['cover']:.1f}%"},
        {'label': 'Export growth', 'value': f"{export_growth:.1f}%"},
        {'label': 'Import growth', 'value': f"{import_growth:.1f}%"},
        {'label': 'Trade deficit', 'value': f"${trade_deficit:,.0f}M"},
        {'label': 'Net exports', 'value': f"${trade_data['balance']:,.0f}M"},
    ]

    if not periods:
        periods = ['2021','2022','2023','2024','2025']
        exports_ts = [3500,3800,4100,4300, trade_data['exports']]
        imports_ts = [4200,4500,4800,5100, trade_data['imports']]

    # Prepare comparison chart data
    comparison_data = {
        'title': 'Exports vs Imports Trend',
        'type': 'line',
        'labels': periods[-12:] if len(periods) > 12 else periods,
        'datasets': [
            {'label': 'Exports', 'data': exports_ts[-12:] if len(exports_ts) > 12 else exports_ts, 'borderColor': '#14b8a6', 'backgroundColor': '#14b8a633'},
            {'label': 'Imports', 'data': imports_ts[-12:] if len(imports_ts) > 12 else imports_ts, 'borderColor': '#0b2f2e', 'backgroundColor': '#0b2f2e33'}
        ]
    }
    
    main_chart = {
        'title': 'Exports Trend',
        'type': 'line',
        'labels': periods[-12:] if len(periods) > 12 else periods,
        'data': exports_ts[-12:] if len(exports_ts) > 12 else exports_ts
    }

    side_chart = {
        'title': 'Export destinations',
        'type': 'doughnut',
        'labels': ['SA','UAE','China','EU','Other'],
        'data': [45,18,15,12,10]
    }

    imports_chart = {
        'labels': imp_labels,
        'data': imp_data
    }

    columns = ['Partner', 'Exports (US$M)', 'Imports (US$M)', 'Balance (US$M)']
    rows = [
        {'Partner': 'South Africa', 'Exports (US$M)': '850', 'Imports (US$M)': '2100', 'Balance (US$M)': '-1250'},
        {'Partner': 'UAE', 'Exports (US$M)': '420', 'Imports (US$M)': '310', 'Balance (US$M)': '110'},
        {'Partner': 'China', 'Exports (US$M)': '380', 'Imports (US$M)': '520', 'Balance (US$M)': '-140'},
        {'Partner': 'EU', 'Exports (US$M)': '320', 'Imports (US$M)': '280', 'Balance (US$M)': '40'}
    ]

    insights = [
        f"Total exports: US$ {trade_data['exports']:,.0f} million",
        f"Total imports: US$ {trade_data['imports']:,.0f} million",
        f"Trade balance: US$ {trade_data['balance']:,.0f} million",
        f"Export coverage ratio: {trade_data['cover']:.1f}%",
        f"Export growth: {export_growth:.1f}% year-over-year",
        'South Africa is the largest trading partner'
    ]

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': imports_chart, 'comparison': comparison_data},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'International Trade Statistics'
    }

# ----------------------------------------------------------------------
# Additional KPI queries
# ----------------------------------------------------------------------
def query_earnings_kpis(filters):
    """Average earnings by province and sector."""
    year = filters.get('year', '2025')
    try:
        year_int = int(float(year))
    except:
        year_int = 2025
    
    earnings_tables = find_tables_by_keywords(['earnings', 'usd', 'province'], mode='any')
    total_earnings = None
    avg_earnings = None
    
    for tbl in earnings_tables:
        cols = guess_column_names(tbl)
        if 'Year' in cols:
            # Sum all province columns
            prov_cols = [c for c in cols if c != 'Year']
            try:
                q = 'SELECT '
                for i, col in enumerate(prov_cols):
                    if i > 0:
                        q += ' + '
                    q += f'SUM("{col}")'
                q += f' FROM "{tbl}" WHERE "Year" = ?'
                res = query_db(q, [year_int], one=True)
                if res and res[0]:
                    total_earnings = safe_float(res[0])
                    avg_earnings = total_earnings / len(prov_cols) if prov_cols else total_earnings
                    break
            except:
                continue
    
    return {
        'total_earnings': total_earnings or 0,
        'avg_earnings': avg_earnings or 0
    }

def query_youth_neet(filters):
    """Youth Not in Education, Employment, or Training."""
    neet_tables = find_tables_by_keywords(['youth', 'neet'], mode='any')
    total_neet = None
    
    for tbl in neet_tables:
        cols = guess_column_names(tbl)
        if 'Male Youth Neet' in cols and 'Female Youth Neet' in cols:
            try:
                rows = query_db(f'SELECT SUM("Male Youth Neet" + "Female Youth Neet") FROM "{tbl}"')
                if rows and rows[0]:
                    total_neet = safe_float(rows[0][0])
                    break
            except:
                continue
    
    return total_neet or 0

def query_informal_employment(filters):
    """Informal sector employment."""
    inf_tables = find_tables_by_keywords(['informal', 'employment', 'province'], mode='any')
    total_informal = None
    
    for tbl in inf_tables:
        cols = guess_column_names(tbl)
        if 'Male' in cols and 'Female' in cols and 'Province' in cols:
            try:
                # Sum Male + Female for all provinces
                rows = query_db(f'SELECT SUM("Male" + "Female") FROM "{tbl}" WHERE "Province" = ?', ['Male'], one=True)
                if rows and rows[0]:
                    total_informal = safe_float(rows[0])
                    break
            except:
                # Try alternative structure
                try:
                    prov_cols = [c for c in cols if c not in ['Province', 'Male', 'Female']]
                    if prov_cols:
                        total = 0
                        for col in ['Male', 'Female']:
                            if col in cols:
                                rows = query_db(f'SELECT SUM("{col}") FROM "{tbl}"')
                                if rows and rows[0]:
                                    total += safe_float(rows[0][0])
                        if total > 0:
                            total_informal = total
                            break
                except:
                    continue
    
    return total_informal or 0

def query_sector_distribution(filters):
    """Employment by industry sector."""
    sector_tables = find_tables_by_keywords(['employed', 'population', 'industry'], mode='any')
    sector_data = {}
    
    for tbl in sector_tables:
        cols = guess_column_names(tbl)
        # Look for table with industry columns
        industry_cols = [c for c in cols if c not in ['Industry', 'Sex', 'Province']]
        if industry_cols:
            try:
                for col in industry_cols[:10]:  # Limit to top sectors
                    rows = query_db(f'SELECT SUM("{col}") FROM "{tbl}"')
                    if rows and rows[0]:
                        val = safe_float(rows[0][0])
                        if val > 0:
                            sector_data[col] = val
                if sector_data:
                    break
            except:
                continue
    
    if sector_data:
        top = sorted(sector_data.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0][:30] for t in top]  # Truncate long names
        data = [t[1] for t in top]
    else:
        labels = ['Agriculture', 'Manufacturing', 'Services', 'Mining', 'Construction']
        data = [1900, 620, 1450, 450, 380]
    
    return labels, data

def query_gdp_timeseries():
    """GDP time series for trend analysis."""
    gdp_tables = find_tables_by_keywords(['gdp', 'provincial'], mode='any')
    years = []
    gdp_values = []
    base_gdp_2020 = None
    
    # First: Get 2020 actual GDP value from WIDE PROV GDP table
    for tbl in gdp_tables:
        cols = guess_column_names(tbl)
        if 'Gdp At Market Prices Usd' in cols and 'Date' in cols:
            try:
                row = query_db(f'SELECT SUM("Gdp At Market Prices Usd") FROM "{tbl}" WHERE "Date" = 2020.0 AND "Gdp At Market Prices Usd" IS NOT NULL', one=True)
                if row and row[0]:
                    base_gdp_2020 = safe_float(row[0]) / 1e9  # Convert to billions
                    break
            except:
                continue
    
    # Second: Use WIDE CURRENT PRICES GDP SHARES table - has data for all years
    shares_table = None
    for tbl in gdp_tables:
        cols = guess_column_names(tbl)
        if 'Year' in cols and 'Gdp At Basic Prices' in cols:
            shares_table = tbl
            break
    
    if shares_table and base_gdp_2020:
        try:
            # Get GDP At Basic Prices for all years (these are percentages/indices)
            rows = query_db(f'SELECT "Year", AVG("Gdp At Basic Prices") FROM "{shares_table}" GROUP BY "Year" ORDER BY "Year"')
            if rows:
                # Get 2020 index value
                row_2020 = query_db(f'SELECT AVG("Gdp At Basic Prices") FROM "{shares_table}" WHERE "Year" = 2020', one=True)
                index_2020 = safe_float(row_2020[0]) if row_2020 and row_2020[0] else 96.26
                
                # Calculate GDP for each year using the index relative to 2020
                for r in rows:
                    year_val = int(r[0])
                    index_val = safe_float(r[1])
                    # Scale based on 2020 actual GDP
                    gdp_val = base_gdp_2020 * (index_val / index_2020)
                    years.append(str(year_val))
                    gdp_values.append(gdp_val)
        except Exception as e:
            pass
    
    # Fallback: Use 2020 data and estimate based on growth
    if not years and base_gdp_2020:
        years = ['2020', '2021', '2022', '2023', '2024']
        # Use conservative growth estimates
        growth_rates = [0, 2.5, 3.0, 2.8, 2.5]
        gdp_values = [base_gdp_2020]
        for i in range(1, len(years)):
            gdp_values.append(gdp_values[-1] * (1 + growth_rates[i]/100))
    
    # Final fallback
    if not years:
        years = ['2020', '2021', '2022', '2023', '2024']
        base_gdp = 51.43
        growth_rates = [0, 2.5, 3.0, 2.8, 2.5]
        gdp_values = [base_gdp]
        for i in range(1, len(years)):
            gdp_values.append(gdp_values[-1] * (1 + growth_rates[i]/100))
    
    return years, gdp_values

def query_trade_timeseries(filters=None):
    """Trade time series for trend analysis."""
    year = filters.get('year') if filters else None
    trade_tables = find_tables_by_keywords(['trade', 'summary'], mode='any')
    periods = []
    exports = []
    imports = []
    
    for tbl in trade_tables:
        cols = guess_column_names(tbl)
        if 'Total.Exports' in cols and 'Imports' in cols and 'Period' in cols:
            try:
                if year:
                    # Filter by year in Period column
                    year_str = str(year)
                    rows = query_db(f'SELECT "Period", "Total.Exports", "Imports" FROM "{tbl}" WHERE "Period" LIKE ? ORDER BY "Period" DESC LIMIT 12', [f'%-{year_str}'])
                else:
                    rows = query_db(f'SELECT "Period", "Total.Exports", "Imports" FROM "{tbl}" ORDER BY "Period" DESC LIMIT 12')
                
                for r in rows:
                    periods.insert(0, r['Period'])
                    exports.insert(0, safe_float(r['Total.Exports']) / 1e6)
                    imports.insert(0, safe_float(r['Imports']) / 1e6)
                break
            except:
                continue
    
    return periods, exports, imports

def assemble_overview(filters):
    # Combine top indicators from other domains
    labour = query_labour_kpis(filters)
    gdp = query_gdp_kpis(filters)
    cpi = query_cpi_kpis(filters)
    trade = query_trade_kpis(filters)
    earnings = query_earnings_kpis(filters)
    neet = query_youth_neet(filters)
    informal = query_informal_employment(filters)
    
    # Calculate informal employment percentage
    informal_pct = (informal / labour['employed'] * 100) if labour['employed'] else 0
    
    # Get NEET percentage (rough estimate)
    neet_pct = (neet / (labour['labour_force'] + neet) * 100) if labour['labour_force'] else 0

    kpis = [
        {'label': 'Employed (thousands)', 'value': f"{labour['employed']:,.0f}"},
        {'label': 'Unemployment rate', 'value': f"{labour['unemp_rate']:.1f}%"},
        {'label': 'GDP growth', 'value': f"{gdp['growth']:.1f}%"},
        {'label': 'Inflation (YoY)', 'value': f"{cpi['yoy']:.1f}%"},
        {'label': 'Informal sector', 'value': f"{informal_pct:.1f}%"},
        {'label': 'Youth NEET rate', 'value': f"{neet_pct:.1f}%"},
        {'label': 'Trade balance', 'value': f"${trade['balance']:,.0f}M"},
        {'label': 'GDP per capita', 'value': f"${gdp['per_capita']:,.0f}"},
    ]

    # Get time series data (filters don't apply to time series, show all years)
    gdp_years, gdp_values = query_gdp_timeseries()
    if not gdp_years:
        gdp_years = ['2020', '2021', '2022', '2023', '2024']
        gdp_values = [32.0, 33.5, 34.2, 35.1, gdp['gdp']]

    main_chart = {
        'title': 'GDP Trend (US$ Billions)',
        'type': 'line',
        'labels': gdp_years,
        'data': gdp_values
    }

    prov_labels, prov_data = query_labour_by_province(filters)
    side_chart = {
        'title': 'Employment by province',
        'type': 'doughnut',
        'labels': prov_labels,
        'data': prov_data
    }

    columns = ['Indicator', 'Current', 'Previous', 'Change']
    rows = [
        {'Indicator': 'Employed (k)', 'Current': f"{labour['employed']:,.0f}", 'Previous': 'N/A', 'Change': 'N/A'},
        {'Indicator': 'Unemployment rate', 'Current': f"{labour['unemp_rate']:.1f}%", 'Previous': 'N/A', 'Change': 'N/A'},
        {'Indicator': 'GDP (US$B)', 'Current': f"{gdp['gdp']:.1f}", 'Previous': 'N/A', 'Change': f"{gdp['growth']:.1f}%"},
        {'Indicator': 'Inflation (YoY)', 'Current': f"{cpi['yoy']:.1f}%", 'Previous': 'N/A', 'Change': 'N/A'},
        {'Indicator': 'Informal sector', 'Current': f"{informal_pct:.1f}%", 'Previous': 'N/A', 'Change': 'N/A'},
        {'Indicator': 'Trade balance', 'Current': f"${trade['balance']:,.0f}M", 'Previous': 'N/A', 'Change': 'N/A'},
    ]

    insights = [
        f"Total employment: {labour['employed']:,.0f} thousand people",
        f"GDP growth rate: {gdp['growth']:.1f}% (GDP: ${gdp['gdp']:.1f}B)",
        f"Inflation rate: {cpi['yoy']:.1f}% year-on-year",
        f"Informal sector accounts for {informal_pct:.1f}% of employment",
        f"Youth NEET rate: {neet_pct:.1f}%",
        f"Trade balance: ${trade['balance']:,.0f} million"
    ]

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': None},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'Top‑Level National Analytics Dashboard'
    }

def _build_from_upload(domain, data_rows, filters):
    # (unchanged – your existing generic upload builder)
    import pandas as pd
    df = pd.DataFrame(data_rows)
    search_term = filters.get('search', '').strip()
    if search_term:
        mask = df.astype(str).apply(lambda row: row.str.contains(search_term, case=False).any(), axis=1)
        df_filtered = df[mask]
    else:
        df_filtered = df
    columns = list(df_filtered.columns)
    rows = df_filtered.head(100).to_dict('records')
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    kpis = []
    for i, col in enumerate(numeric_cols[:4]):
        total = df[col].sum()
        kpis.append({'label': col, 'value': f'{total:,.0f}'})
    while len(kpis) < 4:
        kpis.append({'label': 'No data', 'value': '0'})
    main_chart = {
        'title': f'{numeric_cols[0] if numeric_cols else "Value"} trend (first 10 rows)',
        'type': 'line',
        'labels': list(range(1, min(11, len(df)+1))),
        'data': df[numeric_cols[0]].head(10).tolist() if numeric_cols else []
    }
    cat_cols = df.select_dtypes(include=['object']).columns.tolist()
    if cat_cols:
        counts = df[cat_cols[0]].value_counts().head(5)
        side_chart = {
            'title': f'{cat_cols[0]} distribution',
            'type': 'doughnut',
            'labels': counts.index.tolist(),
            'data': counts.values.tolist()
        }
    else:
        side_chart = {
            'title': 'No categorical data',
            'type': 'doughnut',
            'labels': ['No data'],
            'data': [1]
        }
    imports_chart = None
    if domain == 'trade' and 'Imports' in df.columns:
        imports_chart = {
            'labels': df['Province'].head(5).tolist() if 'Province' in df else ['N/A'],
            'data': df['Imports'].head(5).tolist() if 'Imports' in df else []
        }
    insights = [
        f"Uploaded: {len(df)} rows, {len(df.columns)} columns",
        f"Numeric columns: {len(numeric_cols)}",
        f"Total {numeric_cols[0]}: {df[numeric_cols[0]].sum():,.0f}" if numeric_cols else "",
    ]
    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': imports_chart},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': f'{domain.title()} – using uploaded data',
    }

def fallback_data():
    return {
        'kpis': [{'label': 'No data', 'value': '0'}]*4,
        'charts': {
            'main': {'title': 'No data', 'type': 'line', 'labels': [], 'data': []},
            'side': {'title': 'No data', 'type': 'doughnut', 'labels': [], 'data': []},
            'imports': None
        },
        'table': {'columns': ['No data'], 'rows': []},
        'insights': ['No insights available'],
        'title': 'Data not found',
    }

# ----------------------------------------------------------------------
# API endpoints (unchanged)
# ----------------------------------------------------------------------
@app.route('/api/filters')
def api_filters():
    years = distinct_from_table(['Year', 'year', 'YEAR'])
    if not years:
        years = ['2025', '2024', '2023']
    provinces = distinct_from_table(['Province', 'PROVINCE', 'province', 'Region'])
    if not provinces:
        provinces = ['Harare', 'Bulawayo', 'Manicaland', 'Mashonaland East',
                     'Mashonaland West', 'Mashonaland Central', 'Matabeleland North',
                     'Matabeleland South', 'Midlands', 'Masvingo']
    genders = distinct_from_table(['Sex', 'sex', 'Gender', 'gender'])
    if not genders:
        genders = ['Male', 'Female']
    age_groups = distinct_from_table(['Age Group', 'AgeGroup', 'age_group', 'Age'])
    if not age_groups:
        age_groups = ['15-24', '25-34', '35-44', '45-54', '55+']
    return jsonify({
        'years': years,
        'regions': provinces,
        'genders': genders,
        'ages': age_groups
    })

@app.route('/api/dashboard', methods=['POST'])
def api_dashboard():
    filters = request.get_json()
    domain = filters.get('domain', 'dashboard')
    data = get_dashboard_data(domain, filters)
    return jsonify(data)

@app.route('/api/login', methods=['POST'])
def api_login():
    creds = request.get_json()
    if creds.get('username') == 'admin' and creds.get('password') == 'admin':
        session['user'] = {'name': 'Admin User', 'role': 'Admin'}
        return jsonify(session['user'])
    else:
        return jsonify({'error': 'Invalid credentials'}), 401

@app.route('/api/logout', methods=['GET', 'POST'])
def api_logout():
    session.pop('user', None)
    return jsonify({'status': 'ok'})

def sanitize_table_name(name):
    """Convert a name to a valid SQLite table name."""
    # Remove or replace invalid characters
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Remove leading numbers
    name = re.sub(r'^\d+', '', name)
    # Ensure it starts with a letter or underscore
    if not name or name[0].isdigit():
        name = 'table_' + name
    # Limit length
    return name[:50] if len(name) <= 50 else name[:47] + '_' + str(hash(name))[-3:]

def create_table_from_dataframe(df, table_name, domain):
    """Create a SQLite table from a pandas DataFrame."""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Drop table if exists (or use IF NOT EXISTS)
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    
    # Create table with appropriate column types
    columns = []
    for col in df.columns:
        col_clean = sanitize_table_name(str(col))
        # Determine SQLite type based on pandas dtype
        if pd.api.types.is_integer_dtype(df[col]):
            col_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(df[col]):
            col_type = 'REAL'
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            col_type = 'TEXT'
        else:
            col_type = 'TEXT'
        columns.append(f'"{col_clean}" {col_type}')
    
    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
    cursor.execute(create_sql)
    
    # Insert data
    df_clean = df.copy()
    # Clean column names for insertion
    df_clean.columns = [sanitize_table_name(str(col)) for col in df_clean.columns]
    # Convert datetime to strings
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df[df.columns[list(df_clean.columns).index(col)]]):
            df_clean[col] = df_clean[col].astype(str)
    
    # Replace NaN with None for SQLite
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    # Insert rows
    placeholders = ', '.join(['?' for _ in df_clean.columns])
    col_names_quoted = ', '.join([f'"{col}"' for col in df_clean.columns])
    insert_sql = f'INSERT INTO "{table_name}" ({col_names_quoted}) VALUES ({placeholders})'
    
    for _, row in df_clean.iterrows():
        cursor.execute(insert_sql, tuple(row))
    
    conn.commit()
    conn.close()
    return len(df_clean)

@app.route('/api/data/upload', methods=['POST'])
def api_upload():
    if session.get('user', {}).get('role') not in ['Admin', 'Editor']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    file = request.files.get('file')
    domain = request.form.get('domain', 'dashboard')
    sheet_name = request.form.get('sheet_name', None)  # Optional: specific sheet
    create_table = request.form.get('create_table', 'true').lower() == 'true'
    
    if not file:
        return jsonify({'error': 'Missing file'}), 400
    
    try:
        filename = file.filename
        file_ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        
        # Handle Excel files
        if file_ext in ['xlsx', 'xls']:
            # Read all sheets if no specific sheet requested
            if sheet_name:
                df = pd.read_excel(file, sheet_name=sheet_name)
                sheets = {sheet_name: df}
            else:
                # Read all sheets
                excel_file = pd.ExcelFile(file)
                sheets = {sheet: pd.read_excel(excel_file, sheet_name=sheet) for sheet in excel_file.sheet_names}
        elif file_ext == 'csv':
            df = pd.read_csv(file)
            sheets = {'Sheet1': df}
        else:
            return jsonify({'error': f'Unsupported file type: {file_ext}'}), 400
        
        upload_results = []
        
        for sheet, df in sheets.items():
            if df.empty:
                continue
            
            # Clean the DataFrame
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.dropna(axis=1, how='all')  # Remove completely empty columns
            
            if df.empty:
                continue
            
            # Generate table name
            base_name = sanitize_table_name(f"upload_{domain}_{filename.rsplit('.', 1)[0]}_{sheet}")
            table_name = base_name
            counter = 1
            # Ensure unique table name
            while True:
                try:
                    conn = sqlite3.connect(DATABASE)
                    cursor = conn.cursor()
                    cursor.execute(f'SELECT name FROM sqlite_master WHERE type="table" AND name="{table_name}"')
                    if not cursor.fetchone():
                        conn.close()
                        break
                    table_name = f"{base_name}_{counter}"
                    counter += 1
                    conn.close()
                except:
                    break
            
            # Store JSON for backward compatibility
            data_json = df.to_json(orient='records', date_format='iso')
            
            # Create database table if requested
            rows_inserted = 0
            if create_table:
                try:
                    rows_inserted = create_table_from_dataframe(df, table_name, domain)
                except Exception as e:
                    return jsonify({'error': f'Failed to create table: {str(e)}'}), 400
            
            # Store metadata
            upload_id = execute_db(
                """INSERT INTO data_uploads 
                   (domain, upload_time, filename, data_json, table_name, sheet_name, rows_count, columns_count) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (domain, datetime.now(), filename, data_json, table_name, sheet, len(df), len(df.columns))
            )
            
            # Store detailed metadata
            columns_info = json.dumps({
                'columns': list(df.columns),
                'dtypes': {str(k): str(v) for k, v in df.dtypes.items()},
                'numeric_columns': list(df.select_dtypes(include=['number']).columns),
                'categorical_columns': list(df.select_dtypes(include=['object']).columns)
            })
            
            execute_db(
                """INSERT INTO upload_metadata 
                   (upload_id, table_name, sheet_name, domain, filename, upload_time, rows_count, columns_count, columns_info)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (upload_id, table_name, sheet, domain, filename, datetime.now(), len(df), len(df.columns), columns_info)
            )
            
            upload_results.append({
                'sheet': sheet,
                'table_name': table_name,
                'rows': len(df),
                'columns': list(df.columns),
                'rows_inserted': rows_inserted
            })
        
        return jsonify({
            'status': 'uploaded',
            'results': upload_results,
            'total_sheets': len(upload_results)
        })
        
    except Exception as e:
        return jsonify({'error': f'Could not process file: {str(e)}'}), 400

@app.route('/api/data/uploads', methods=['GET'])
def api_list_uploads():
    """List all uploaded files with metadata."""
    if session.get('user', {}).get('role') not in ['Admin', 'Editor']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    domain = request.args.get('domain', None)
    
    query = """
        SELECT id, domain, filename, upload_time, table_name, sheet_name, 
               rows_count, columns_count
        FROM data_uploads
    """
    params = []
    if domain:
        query += " WHERE domain = ?"
        params.append(domain)
    query += " ORDER BY upload_time DESC"
    
    rows = query_db(query, tuple(params))
    uploads = []
    for row in rows:
        uploads.append({
            'id': row['id'],
            'domain': row['domain'],
            'filename': row['filename'],
            'upload_time': row['upload_time'],
            'table_name': row['table_name'],
            'sheet_name': row['sheet_name'],
            'rows_count': row['rows_count'],
            'columns_count': row['columns_count']
        })
    
    return jsonify({'uploads': uploads})

@app.route('/api/data/upload/<int:upload_id>', methods=['DELETE'])
def api_delete_upload(upload_id):
    """Delete an uploaded file and its table."""
    if session.get('user', {}).get('role') not in ['Admin', 'Editor']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    # Get upload info
    row = query_db("SELECT table_name FROM data_uploads WHERE id = ?", (upload_id,), one=True)
    if not row:
        return jsonify({'error': 'Upload not found'}), 404
    
    table_name = row['table_name']
    
    # Drop the table if it exists
    if table_name:
        try:
            execute_db(f'DROP TABLE IF EXISTS "{table_name}"')
        except:
            pass
    
    # Delete metadata
    execute_db("DELETE FROM upload_metadata WHERE upload_id = ?", (upload_id,))
    
    # Delete upload record
    execute_db("DELETE FROM data_uploads WHERE id = ?", (upload_id,))
    
    return jsonify({'status': 'deleted'})

@app.route('/api/data/upload/preview', methods=['POST'])
def api_preview_upload():
    """Preview Excel file before uploading."""
    if session.get('user', {}).get('role') not in ['Admin', 'Editor']:
        return jsonify({'error': 'Unauthorized'}), 403
    
    file = request.files.get('file')
    if not file:
        return jsonify({'error': 'Missing file'}), 400
    
    try:
        filename = file.filename
        file_ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        
        if file_ext in ['xlsx', 'xls']:
            excel_file = pd.ExcelFile(file)
            sheets_info = {}
            for sheet in excel_file.sheet_names:
                df = pd.read_excel(excel_file, sheet_name=sheet, nrows=5)  # Preview first 5 rows
                sheets_info[sheet] = {
                    'columns': list(df.columns),
                    'preview': df.head(5).to_dict('records'),
                    'total_rows': len(pd.read_excel(excel_file, sheet_name=sheet))  # Get full count
                }
            return jsonify({'sheets': sheets_info, 'filename': filename})
        elif file_ext == 'csv':
            df = pd.read_csv(file, nrows=5)
            return jsonify({
                'sheets': {
                    'Sheet1': {
                        'columns': list(df.columns),
                        'preview': df.head(5).to_dict('records'),
                        'total_rows': len(pd.read_csv(file))  # Get full count
                    }
                },
                'filename': filename
            })
        else:
            return jsonify({'error': f'Unsupported file type: {file_ext}'}), 400
    except Exception as e:
        return jsonify({'error': f'Could not preview file: {str(e)}'}), 400

@app.route('/api/export')
def api_export():
    filters = {
        'domain': request.args.get('domain', 'dashboard'),
        'year': request.args.get('year'),
        'region': request.args.get('region'),
        'gender': request.args.get('gender'),
        'age': request.args.get('age'),
        'search': request.args.get('search', '')
    }
    data = get_dashboard_data(filters['domain'], filters)
    table = data['table']
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(table['columns'])
    for row in table['rows']:
        writer.writerow([row.get(col, '') for col in table['columns']])
    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{filters["domain"]}_export.csv'
    )

@app.route('/api/data/create', methods=['POST'])
def api_create():
    if session.get('user', {}).get('role') not in ['Admin', 'Editor']:
        return jsonify({'error': 'Unauthorized'}), 403
    return jsonify({'status': 'created (mock)'})

@app.route('/api/data/delete', methods=['POST'])
def api_delete():
    if session.get('user', {}).get('role') not in ['Admin', 'Editor']:
        return jsonify({'error': 'Unauthorized'}), 403
    return jsonify({'status': 'deleted (mock)'})

@app.route('/')
def index():
    return render_template('Dashboard.html')

# ----------------------------------------------------------------------
# Helper functions for dynamic filters (same as before)
# ----------------------------------------------------------------------
def get_all_table_names():
    rows = query_db("SELECT name FROM sqlite_master WHERE type='table'")
    return [row['name'] for row in rows]

def guess_column_names(table_name):
    try:
        cur = get_db().execute(f'SELECT * FROM "{table_name}" LIMIT 1')
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        return colnames
    except:
        return []

def distinct_from_table(column_hints, table_pattern=None):
    values = set()
    tables = get_all_table_names()
    if table_pattern:
        tables = [t for t in tables if re.search(table_pattern, t, re.IGNORECASE)]
    for tbl in tables:
        cols = guess_column_names(tbl)
        for hint in column_hints:
            if hint in cols:
                try:
                    rows = query_db(f'SELECT DISTINCT "{hint}" FROM "{tbl}" WHERE "{hint}" IS NOT NULL')
                    for r in rows:
                        val = r[hint]
                        if val:
                            values.add(str(val).strip())
                except:
                    continue
    return sorted(values)

# ----------------------------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True)