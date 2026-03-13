#!/usr/bin/env python3
"""
Migration script to populate fact and dimension tables from existing data
"""

import sqlite3
import re
from datetime import datetime

def create_connection():
    """Create database connection"""
    return sqlite3.connect('zimstats.sqlite')

def initialize_fact_tables(conn):
    """Create the new fact and dimension tables"""
    cursor = conn.cursor()
    
    # Read schema file
    with open('database_schema.sql', 'r') as f:
        schema_sql = f.read()
    
    # Execute schema
    cursor.executescript(schema_sql)
    conn.commit()
    print("Fact and dimension tables created successfully")

def populate_dim_date(conn):
    """Populate date dimension from existing data"""
    cursor = conn.cursor()
    
    # Since most tables don't have year columns, create a basic date structure
    # We'll create entries for recent years (2020-2025)
    years = [2020, 2021, 2022, 2023, 2024, 2025]
    quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    months = ['January', 'February', 'March', 'April', 'May', 'June', 
              'July', 'August', 'September', 'October', 'November', 'December']
    
    date_id = 1
    for year in years:
        # Create year records
        cursor.execute("INSERT OR IGNORE INTO dim_date (date_id, year, period_label) VALUES (?, ?, ?)", 
                     (date_id, year, str(year)))
        date_id += 1
        
        # Create quarter records
        for quarter in quarters:
            cursor.execute("INSERT OR IGNORE INTO dim_date (date_id, year, quarter, period_label) VALUES (?, ?, ?, ?)", 
                         (date_id, year, quarter, f"{year} {quarter}"))
            date_id += 1
        
        # Create month records
        for i, month in enumerate(months, 1):
            cursor.execute("INSERT OR IGNORE INTO dim_date (date_id, year, month_name, month_number, period_label) VALUES (?, ?, ?, ?, ?)", 
                         (date_id, year, month, i, f"{year}-{i:02d}"))
            date_id += 1
    
    conn.commit()
    print(f"Populated dim_date with {date_id-1} records")

def populate_dim_geography(conn):
    """Populate geography dimension"""
    cursor = conn.cursor()
    
    # Get provinces from existing tables
    cursor.execute("SELECT DISTINCT province FROM employment_by_province_and_sex_in_zimbabwe_2025_q2_qlfs_empl WHERE province IS NOT NULL")
    provinces = [row[0] for row in cursor.fetchall()]
    
    # Add known provinces if not found
    known_provinces = ['Harare', 'Bulawayo', 'Manicaland', 'Mashonaland Central', 'Mashonaland East', 
                      'Mashonaland West', 'Masvingo', 'Matabeleland North', 'Matabeleland South', 'Midlands']
    
    all_provinces = list(set(provinces + known_provinces))
    
    geo_id = 1
    for province in all_provinces:
        if province and province.strip():
            cursor.execute("INSERT OR IGNORE INTO dim_geography (geo_id, province, country, region) VALUES (?, ?, ?, ?)", 
                         (geo_id, province.strip(), 'Zimbabwe', 'National'))
            geo_id += 1
    
    # Add countries for trade data
    countries = ['Zimbabwe', 'South Africa', 'Botswana', 'Mozambique', 'Zambia', 'China', 'USA', 'EU', 'UK']
    for country in countries:
        cursor.execute("INSERT OR IGNORE INTO dim_geography (geo_id, country, region) VALUES (?, ?, ?)", 
                     (geo_id, country, 'International'))
        geo_id += 1
    
    conn.commit()
    print(f"Populated dim_geography with {geo_id-1} records")

def populate_dim_industry(conn):
    """Populate industry dimension"""
    cursor = conn.cursor()
    
    # Get industries from employment data
    cursor.execute("SELECT DISTINCT Industry FROM distribution_of_currently_employed_population_by_industry_an WHERE Industry IS NOT NULL LIMIT 50")
    industries = [row[0] for row in cursor.fetchall()]
    
    # Add known industries
    known_industries = [
        ('Agriculture', 'Agriculture'),
        ('Mining & Quarrying', 'Mining'),
        ('Manufacturing', 'Manufacturing'),
        ('Electricity & Water', 'Utilities'),
        ('Construction', 'Construction'),
        ('Wholesale & Retail Trade', 'Trade'),
        ('Transport & Storage', 'Transport'),
        ('Accommodation & Food Service', 'Tourism'),
        ('Information & Communication', 'ICT'),
        ('Financial & Insurance', 'Finance'),
        ('Real Estate', 'Real Estate'),
        ('Professional Services', 'Services'),
        ('Public Administration', 'Government'),
        ('Education', 'Education'),
        ('Health', 'Health'),
        ('Other Services', 'Services')
    ]
    
    industry_id = 1
    for industry_name, sector in known_industries:
        cursor.execute("INSERT OR IGNORE INTO dim_industry (industry_id, industry_name, sector_group) VALUES (?, ?, ?)", 
                     (industry_id, industry_name, sector))
        industry_id += 1
    
    # Add industries from data
    for industry in industries:
        if industry and industry.strip() and len(industry.strip()) < 100:
            cursor.execute("INSERT OR IGNORE INTO dim_industry (industry_id, industry_name, sector_group) VALUES (?, ?, ?)", 
                         (industry_id, industry.strip(), 'Other'))
            industry_id += 1
    
    conn.commit()
    print(f"Populated dim_industry with {industry_id-1} records")

def populate_dim_demographics(conn):
    """Populate demographics dimension"""
    cursor = conn.cursor()
    
    demo_id = 1
    
    # Sex categories
    sexes = ['Male', 'Female', 'Both']
    for sex in sexes:
        cursor.execute("INSERT OR IGNORE INTO dim_demographics (demo_id, sex) VALUES (?, ?)", (demo_id, sex))
        demo_id += 1
    
    # Age groups
    age_groups = ['15-24', '25-34', '35-44', '45-54', '55-64', '65+', '15-35', 'All Ages']
    for age_group in age_groups:
        cursor.execute("INSERT OR IGNORE INTO dim_demographics (demo_id, age_group) VALUES (?, ?)", (demo_id, age_group))
        demo_id += 1
    
    # Employment status
    statuses = ['Employed', 'Unemployed', 'Inactive', 'NEET', 'Underemployed']
    for status in statuses:
        cursor.execute("INSERT OR IGNORE INTO dim_demographics (demo_id, employment_status) VALUES (?, ?)", (demo_id, status))
        demo_id += 1
    
    # Occupations
    occupations = ['Managers', 'Professionals', 'Technicians', 'Clerical Support', 'Service Workers', 
                  'Skilled Agricultural', 'Craft Workers', 'Plant Operators', 'Elementary Occupations']
    for occupation in occupations:
        cursor.execute("INSERT OR IGNORE INTO dim_demographics (demo_id, occupation) VALUES (?, ?)", (demo_id, occupation))
        demo_id += 1
    
    conn.commit()
    print(f"Populated dim_demographics with {demo_id-1} records")

def populate_dim_currency(conn):
    """Populate currency dimension"""
    cursor = conn.cursor()
    
    currencies = [
        (1, 'USD', 'US Dollar'),
        (2, 'ZWL', 'Zimbabwe Dollar'),
        (3, 'ZIG', 'Zimbabwe Gold'),
        (4, 'EUR', 'Euro')
    ]
    
    cursor.executemany("INSERT OR IGNORE INTO dim_currency (currency_id, currency_code, currency_name) VALUES (?, ?, ?)", currencies)
    conn.commit()
    print("Populated dim_currency with currency records")

def populate_dim_trade_group(conn):
    """Populate trade group dimension"""
    cursor = conn.cursor()
    
    trade_groups = [
        (1, 'COMESA', 'Common Market for Eastern and Southern Africa'),
        (2, 'ECCAS', 'Economic Community of Central African States'),
        (3, 'EU', 'European Union'),
        (4, 'AfCFTA', 'African Continental Free Trade Area'),
        (5, 'SADC', 'Southern African Development Community')
    ]
    
    cursor.executemany("INSERT OR IGNORE INTO dim_trade_group (trade_group_id, trade_group_name, trade_group_code) VALUES (?, ?, ?)", trade_groups)
    conn.commit()
    print("Populated dim_trade_group with trade group records")

def migrate_labour_data(conn):
    """Migrate labour data to fact_labour table"""
    cursor = conn.cursor()
    
    # Get mapping for dimensions
    date_map = get_date_mapping(conn)
    geo_map = get_geography_mapping(conn)
    industry_map = get_industry_mapping(conn)
    demo_map = get_demographics_mapping(conn)
    
    labour_fact_id = 1
    
    # Migrate employment by province and sex (using the actual table structure)
    try:
        cursor.execute("SELECT province, item, value FROM employment_by_province_and_sex_in_zimbabwe_2025_q2_qlfs_empl WHERE province IS NOT NULL")
        rows = cursor.fetchall()
        
        for row in rows:
            province, item, value = row
            if not province or not item or value is None:
                continue
            
            # Use 2025 as default year (Q2 data)
            date_id = date_map.get('2025')
            province_id = geo_map.get(province)
            
            # Map item to demographic and variable
            if item.lower() == 'male':
                sex_id = demo_map.get(('Male', None, None, None))
                variable_name = 'employed_population'
            elif item.lower() == 'female':
                sex_id = demo_map.get(('Female', None, None, None))
                variable_name = 'employed_population'
            else:
                sex_id = None
                variable_name = item.lower().replace(' ', '_')
            
            if date_id and province_id and sex_id:
                try:
                    cursor.execute("""
                        INSERT INTO fact_labour (labour_fact_id, date_id, province_id, sex_id, variable_name, value)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (labour_fact_id, date_id, province_id, sex_id, variable_name, float(value)))
                    labour_fact_id += 1
                except (ValueError, TypeError):
                    pass
    except Exception as e:
        print(f"Error migrating employment data: {e}")
    
    # Migrate industry employment data
    try:
        cursor.execute("SELECT * FROM distribution_of_currently_employed_population_by_industry_an LIMIT 100")
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        
        for row in rows:
            row_dict = dict(zip(cols, row))
            industry = row_dict.get('Industry') or row_dict.get('industry')
            value = row_dict.get('Value') or row_dict.get('value')
            
            if industry and value is not None:
                date_id = date_map.get('2025')  # Default to 2025
                industry_id = industry_map.get(industry)
                
                if date_id and industry_id:
                    try:
                        cursor.execute("""
                            INSERT INTO fact_labour (labour_fact_id, date_id, industry_id, variable_name, value)
                            VALUES (?, ?, ?, ?, ?)
                        """, (labour_fact_id, date_id, industry_id, 'employed_by_industry', float(value)))
                        labour_fact_id += 1
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        print(f"Error migrating industry employment data: {e}")
    
    conn.commit()
    print(f"Migrated labour data with {labour_fact_id-1} records")

def migrate_prices_data(conn):
    """Migrate prices data to fact_prices table"""
    cursor = conn.cursor()
    
    date_map = get_date_mapping(conn)
    geo_map = get_geography_mapping(conn)
    currency_map = get_currency_mapping(conn)
    
    price_fact_id = 1
    
    # Example: Migrate CPI data
    try:
        cursor.execute("SELECT * FROM cpi_weighted_index_sheet1 LIMIT 100")
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        
        for row in rows:
            row_dict = dict(zip(cols, row))
            
            # Extract year and try to find date_id
            year = row_dict.get('Year') or row_dict.get('year')
            if year:
                date_id = date_map.get(str(int(float(year))))
                if date_id:
                    # Insert CPI index
                    cpi_value = row_dict.get('All Items') or row_dict.get('CPI')
                    if cpi_value:
                        try:
                            cursor.execute("""
                                INSERT INTO fact_prices (price_fact_id, date_id, currency_id, variable_name, value)
                                VALUES (?, ?, ?, ?, ?)
                            """, (price_fact_id, date_id, 1, 'cpi_index', float(cpi_value)))
                            price_fact_id += 1
                        except (ValueError, TypeError):
                            pass
    except Exception as e:
        print(f"Error migrating prices data: {e}")
    
    conn.commit()
    print(f"Migrated prices data with {price_fact_id-1} records")

def migrate_national_accounts_data(conn):
    """Migrate national accounts data to fact_national_accounts table"""
    cursor = conn.cursor()
    
    date_map = get_date_mapping(conn)
    geo_map = get_geography_mapping(conn)
    industry_map = get_industry_mapping(conn)
    
    accounts_fact_id = 1
    
    # Example: Migrate GDP data
    try:
        cursor.execute("SELECT * FROM prov_gdp_all_years_all_years LIMIT 100")
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        
        for row in rows:
            row_dict = dict(zip(cols, row))
            
            year = row_dict.get('Year') or row_dict.get('year')
            province = row_dict.get('Province') or row_dict.get('province')
            gdp_value = row_dict.get('Value') or row_dict.get('GDP') or row_dict.get('Gdp At Market Prices Usd')
            
            if year and province and gdp_value:
                date_id = date_map.get(str(int(float(year))))
                province_id = geo_map.get(province)
                
                if date_id and province_id:
                    try:
                        cursor.execute("""
                            INSERT INTO fact_national_accounts (accounts_fact_id, date_id, province_id, variable_name, value)
                            VALUES (?, ?, ?, ?, ?)
                        """, (accounts_fact_id, date_id, province_id, 'gdp_constant', float(gdp_value)))
                        accounts_fact_id += 1
                    except (ValueError, TypeError):
                        pass
    except Exception as e:
        print(f"Error migrating national accounts data: {e}")
    
    conn.commit()
    print(f"Migrated national accounts data with {accounts_fact_id-1} records")

def migrate_trade_data(conn):
    """Migrate trade data to fact_trade table"""
    cursor = conn.cursor()
    
    date_map = get_date_mapping(conn)
    geo_map = get_geography_mapping(conn)
    trade_group_map = get_trade_group_mapping(conn)
    
    trade_fact_id = 1
    
    # Example: Migrate trade summary data
    try:
        cursor.execute("SELECT * FROM trade_summary_sheet_1 LIMIT 100")
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        
        for row in rows:
            row_dict = dict(zip(cols, row))
            
            year = row_dict.get('Year') or row_dict.get('year')
            exports = row_dict.get('Total Exports') or row_dict.get('Exports')
            imports = row_dict.get('Imports')
            
            if year:
                date_id = date_map.get(str(int(float(year))))
                if date_id:
                    # Insert exports data
                    if exports:
                        try:
                            cursor.execute("""
                                INSERT INTO fact_trade (trade_fact_id, date_id, country_id, variable_name, value)
                                VALUES (?, ?, ?, ?, ?)
                            """, (trade_fact_id, date_id, geo_map.get('Zimbabwe'), 'export_value', float(exports)))
                            trade_fact_id += 1
                        except (ValueError, TypeError):
                            pass
                    
                    # Insert imports data
                    if imports:
                        try:
                            cursor.execute("""
                                INSERT INTO fact_trade (trade_fact_id, date_id, country_id, variable_name, value)
                                VALUES (?, ?, ?, ?, ?)
                            """, (trade_fact_id, date_id, geo_map.get('Zimbabwe'), 'import_value', float(imports)))
                            trade_fact_id += 1
                        except (ValueError, TypeError):
                            pass
    except Exception as e:
        print(f"Error migrating trade data: {e}")
    
    conn.commit()
    print(f"Migrated trade data with {trade_fact_id-1} records")

# Helper functions for mapping
def get_date_mapping(conn):
    """Get mapping of year/period to date_id"""
    cursor = conn.cursor()
    cursor.execute("SELECT date_id, year, quarter, month_number, period_label FROM dim_date")
    mapping = {}
    for row in cursor.fetchall():
        date_id, year, quarter, month_num, period_label = row
        if year:
            mapping[str(year)] = date_id
        if period_label:
            mapping[period_label] = date_id
    return mapping

def get_geography_mapping(conn):
    """Get mapping of province/country to geo_id"""
    cursor = conn.cursor()
    cursor.execute("SELECT geo_id, province, country FROM dim_geography")
    mapping = {}
    for row in cursor.fetchall():
        geo_id, province, country = row
        if province:
            mapping[province] = geo_id
        if country:
            mapping[country] = geo_id
    return mapping

def get_industry_mapping(conn):
    """Get mapping of industry name to industry_id"""
    cursor = conn.cursor()
    cursor.execute("SELECT industry_id, industry_name FROM dim_industry")
    mapping = {}
    for row in cursor.fetchall():
        industry_id, industry_name = row
        if industry_name:
            mapping[industry_name] = industry_id
    return mapping

def get_demographics_mapping(conn):
    """Get mapping of demographics to demo_id"""
    cursor = conn.cursor()
    cursor.execute("SELECT demo_id, sex, age_group, occupation, employment_status FROM dim_demographics")
    mapping = {}
    for row in cursor.fetchall():
        demo_id, sex, age_group, occupation, status = row
        key = (sex, age_group, occupation, status)
        mapping[key] = demo_id
    return mapping

def get_currency_mapping(conn):
    """Get mapping of currency code to currency_id"""
    cursor = conn.cursor()
    cursor.execute("SELECT currency_id, currency_code FROM dim_currency")
    mapping = {}
    for row in cursor.fetchall():
        currency_id, currency_code = row
        mapping[currency_code] = currency_id
    return mapping

def get_trade_group_mapping(conn):
    """Get mapping of trade group to trade_group_id"""
    cursor = conn.cursor()
    cursor.execute("SELECT trade_group_id, trade_group_name, trade_group_code FROM dim_trade_group")
    mapping = {}
    for row in cursor.fetchall():
        trade_group_id, trade_group_name, trade_group_code = row
        mapping[trade_group_name] = trade_group_id
        mapping[trade_group_code] = trade_group_id
    return mapping

def main():
    """Main migration function"""
    conn = create_connection()
    
    try:
        print("Starting migration to fact and dimension tables...")
        
        # Initialize tables
        initialize_fact_tables(conn)
        
        # Populate dimensions
        populate_dim_date(conn)
        populate_dim_geography(conn)
        populate_dim_industry(conn)
        populate_dim_demographics(conn)
        populate_dim_currency(conn)
        populate_dim_trade_group(conn)
        
        # Migrate fact tables
        migrate_labour_data(conn)
        migrate_prices_data(conn)
        migrate_national_accounts_data(conn)
        migrate_trade_data(conn)
        
        print("Migration completed successfully!")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
