"""
Fact and Dimension table models for the Data Analytics Dashboard
"""

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import ForeignKey, Integer, String, Text, Numeric, Float
from sqlalchemy.orm import relationship

db = SQLAlchemy()

# DIMENSION TABLES

class DimDate(db.Model):
    """Date dimension table"""
    __tablename__ = 'dim_date'
    
    date_id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer)
    quarter = db.Column(db.String(10))
    month_name = db.Column(db.String(20))
    month_number = db.Column(db.Integer)
    period_label = db.Column(db.String(50))  # "2025 Q2", "2023-07"

class DimGeography(db.Model):
    """Geography dimension table"""
    __tablename__ = 'dim_geography'
    
    geo_id = db.Column(db.Integer, primary_key=True)
    province = db.Column(db.String(100))
    country = db.Column(db.String(100))
    region = db.Column(db.String(100))

class DimIndustry(db.Model):
    """Industry dimension table"""
    __tablename__ = 'dim_industry'
    
    industry_id = db.Column(db.Integer, primary_key=True)
    industry_name = db.Column(db.String(200))
    sector_group = db.Column(db.String(100))

class DimDemographics(db.Model):
    """Demographics dimension table"""
    __tablename__ = 'dim_demographics'
    
    demo_id = db.Column(db.Integer, primary_key=True)
    sex = db.Column(db.String(10))
    age_group = db.Column(db.String(20))
    occupation = db.Column(db.String(100))
    employment_status = db.Column(db.String(50))

class DimCurrency(db.Model):
    """Currency dimension table"""
    __tablename__ = 'dim_currency'
    
    currency_id = db.Column(db.Integer, primary_key=True)
    currency_code = db.Column(db.String(10))
    currency_name = db.Column(db.String(50))

class DimTradeGroup(db.Model):
    """Trade group dimension table"""
    __tablename__ = 'dim_trade_group'
    
    trade_group_id = db.Column(db.Integer, primary_key=True)
    trade_group_name = db.Column(db.String(100))
    trade_group_code = db.Column(db.String(20))

# FACT TABLES

class FactLabour(db.Model):
    """Labour fact table"""
    __tablename__ = 'fact_labour'
    
    labour_fact_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    date_id = db.Column(db.Integer, ForeignKey('dim_date.date_id'))
    industry_id = db.Column(db.Integer, ForeignKey('dim_industry.industry_id'))
    occupation_id = db.Column(db.Integer, ForeignKey('dim_demographics.demo_id'))
    sex_id = db.Column(db.Integer, ForeignKey('dim_demographics.demo_id'))
    age_group_id = db.Column(db.Integer, ForeignKey('dim_demographics.demo_id'))
    province_id = db.Column(db.Integer, ForeignKey('dim_geography.geo_id'))
    variable_name = db.Column(db.String(100))  # e.g. "employed_population", "informal_employment"
    value = db.Column(Numeric)
    
    # Relationships
    date = relationship('DimDate', foreign_keys=[date_id])
    industry = relationship('DimIndustry', foreign_keys=[industry_id])
    occupation = relationship('DimDemographics', foreign_keys=[occupation_id])
    sex = relationship('DimDemographics', foreign_keys=[sex_id])
    age_group = relationship('DimDemographics', foreign_keys=[age_group_id])
    province = relationship('DimGeography', foreign_keys=[province_id])

class FactPrices(db.Model):
    """Prices fact table"""
    __tablename__ = 'fact_prices'
    
    price_fact_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    date_id = db.Column(db.Integer, ForeignKey('dim_date.date_id'))
    province_id = db.Column(db.Integer, ForeignKey('dim_geography.geo_id'))
    currency_id = db.Column(db.Integer, ForeignKey('dim_currency.currency_id'))
    variable_name = db.Column(db.String(100))  # e.g. "cpi_index", "monthly_inflation"
    value = db.Column(Numeric)
    
    # Relationships
    date = relationship('DimDate', foreign_keys=[date_id])
    province = relationship('DimGeography', foreign_keys=[province_id])
    currency = relationship('DimCurrency', foreign_keys=[currency_id])

class FactNationalAccounts(db.Model):
    """National accounts fact table"""
    __tablename__ = 'fact_national_accounts'
    
    accounts_fact_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    date_id = db.Column(db.Integer, ForeignKey('dim_date.date_id'))
    province_id = db.Column(db.Integer, ForeignKey('dim_geography.geo_id'))
    industry_id = db.Column(db.Integer, ForeignKey('dim_industry.industry_id'))
    variable_name = db.Column(db.String(100))  # "gdp_constant", "earnings_usd"
    value = db.Column(Numeric)
    
    # Relationships
    date = relationship('DimDate', foreign_keys=[date_id])
    province = relationship('DimGeography', foreign_keys=[province_id])
    industry = relationship('DimIndustry', foreign_keys=[industry_id])

class FactTrade(db.Model):
    """Trade fact table"""
    __tablename__ = 'fact_trade'
    
    trade_fact_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    date_id = db.Column(db.Integer, ForeignKey('dim_date.date_id'))
    country_id = db.Column(db.Integer, ForeignKey('dim_geography.geo_id'))
    trade_group_id = db.Column(db.Integer, ForeignKey('dim_trade_group.trade_group_id'))
    variable_name = db.Column(db.String(100))  # "export_value", "import_share"
    value = db.Column(Numeric)
    
    # Relationships
    date = relationship('DimDate', foreign_keys=[date_id])
    country = relationship('DimGeography', foreign_keys=[country_id])
    trade_group = relationship('DimTradeGroup', foreign_keys=[trade_group_id])

# Helper functions for querying fact tables

def query_labour_kpis_fact(filters=None):
    """Extract labour KPIs from fact_labour table"""
    if filters is None:
        filters = {}
    
    print("=== LABOUR KPI INVESTIGATION ===")
    
    year = filters.get('year')
    region = filters.get('region')
    gender = filters.get('gender')
    
    # Base query
    query = db.session.query(FactLabour)
    
    # Apply filters
    if year:
        query = query.join(DimDate).filter(DimDate.year == int(float(year)))
    
    if region and region != 'All':
        query = query.join(DimGeography).filter(DimGeography.province == region)
    
    # First, let's see what variable names actually exist
    all_records = query.all()
    variable_names = set([record.variable_name for record in all_records])
    print(f"Available variable names in fact_labour: {variable_names}")
    
    # Get employed population - try multiple variable names
    employed_total = 0
    for var_name in ['employed_population', 'employed', 'employment', 'value']:
        employed_query = query.filter(FactLabour.variable_name == var_name)
        employed_records = employed_query.all()
        if employed_records:
            employed_total = sum([float(row.value) for row in employed_records if row.value])
            print(f"Found {len(employed_records)} records for '{var_name}' = {employed_total}")
            break
    
    # Get unemployed population - try multiple variable names
    unemployed_total = 0
    for var_name in ['unemployed_population', 'unemployed', 'unemployment', 'value']:
        unemployed_query = query.filter(FactLabour.variable_name == var_name)
        unemployed_records = unemployed_query.all()
        if unemployed_records:
            unemployed_total = sum([float(row.value) for row in unemployed_records if row.value])
            print(f"Found {len(unemployed_records)} records for '{var_name}' = {unemployed_total}")
            break
    
    # Calculate derived metrics
    labour_force = employed_total + unemployed_total
    unemp_rate = (unemployed_total / labour_force * 100) if labour_force > 0 else 0
    
    result = {
        'labour_force': labour_force,
        'employed': employed_total,
        'unemployed': unemployed_total,
        'unemp_rate': unemp_rate
    }
    
    print(f"Labour KPI result: {result}")
    print("=== END LABOUR KPI INVESTIGATION ===\n")
    
    return result

def query_prices_kpis_fact(filters=None):
    """Extract prices KPIs from fact_prices table"""
    if filters is None:
        filters = {}
    
    year = filters.get('year')
    
    # Base query
    query = db.session.query(FactPrices).join(DimDate)
    
    # Apply year filter
    if year:
        query = query.filter(DimDate.year == int(float(year)))
    
    # Get CPI index
    cpi_query = query.filter(FactPrices.variable_name == 'cpi_index')
    cpi_value = None
    cpi_row = cpi_query.first()
    if cpi_row and cpi_row.value:
        cpi_value = float(cpi_row.value)
    
    # Get inflation rates
    yoy_query = query.filter(FactPrices.variable_name == 'annual_inflation')
    yoy_row = yoy_query.first()
    yoy_value = float(yoy_row.value) if yoy_row and yoy_row.value else 0
    
    mom_query = query.filter(FactPrices.variable_name == 'monthly_inflation')
    mom_row = mom_query.first()
    mom_value = float(mom_row.value) if mom_row and mom_row.value else 0
    
    return {
        'cpi': cpi_value or 0,
        'mom': mom_value,
        'yoy': yoy_value,
        'food': 0  # Would need specific food inflation query
    }

def query_gdp_kpis_fact(filters=None):
    """Extract GDP KPIs from fact_national_accounts table"""
    if filters is None:
        filters = {}
    
    print("=== GDP KPI INVESTIGATION ===")
    
    year = filters.get('year')
    
    # Base query
    query = db.session.query(FactNationalAccounts).join(DimDate)
    
    # Apply year filter
    if year:
        query = query.filter(DimDate.year == int(float(year)))
    
    # First, let's see what variable names actually exist
    all_records = query.all()
    variable_names = set([record.variable_name for record in all_records])
    print(f"Available variable names in fact_national_accounts: {variable_names}")
    
    # Get GDP - try multiple variable names
    gdp_total = 0
    for var_name in ['gdp', 'provincial_gdp', 'gdp_constant', 'value']:
        gdp_query = query.filter(FactNationalAccounts.variable_name == var_name)
        gdp_records = gdp_query.all()
        if gdp_records:
            gdp_total = sum([float(row.value) for row in gdp_records if row.value])
            print(f"Found {len(gdp_records)} records for '{var_name}' = {gdp_total}")
            break
    
    # Get growth rate
    growth_total = 0
    for var_name in ['gdp_growth', 'growth', 'growth_rate']:
        growth_query = query.filter(FactNationalAccounts.variable_name == var_name)
        growth_records = growth_query.all()
        if growth_records:
            growth_total = sum([float(row.value) for row in growth_records if row.value]) / len(growth_records)
            print(f"Found {len(growth_records)} records for '{var_name}' = {growth_total}")
            break
    
    # Calculate per capita (assuming population of 15 million)
    per_capita = (gdp_total * 1000000) / 15000000 if gdp_total > 0 else 0
    
    # Calculate agriculture share (simplified)
    agri_share = 15.0  # Default value
    
    result = {
        'gdp': gdp_total / 1000,  # Convert to billions
        'growth': growth_total,
        'per_capita': per_capita,
        'agri_share': agri_share
    }
    
    print(f"GDP KPI result: {result}")
    print("=== END GDP KPI INVESTIGATION ===\n")
    
    return result

def query_trade_kpis_fact(filters=None):
    """Extract trade KPIs from fact_trade table"""
    if filters is None:
        filters = {}
    
    year = filters.get('year')
    
    # Base query
    query = db.session.query(FactTrade).join(DimDate)
    
    # Apply year filter
    if year:
        query = query.filter(DimDate.year == int(float(year)))
    
    # Get exports
    exports_query = query.filter(FactTrade.variable_name == 'export_value')
    exports_total = sum([float(row.value) for row in exports_query.all() if row.value])
    
    # Get imports
    imports_query = query.filter(FactTrade.variable_name == 'import_value')
    imports_total = sum([float(row.value) for row in imports_query.all() if row.value])
    
    # Calculate balance and cover ratio
    balance = exports_total - imports_total
    cover = (exports_total / imports_total * 100) if imports_total > 0 else 0
    
    return {
        'exports': exports_total / 1e6 if exports_total > 1000000 else exports_total,
        'imports': imports_total / 1e6 if imports_total > 1000000 else imports_total,
        'balance': balance / 1e6 if abs(balance) > 1000000 else balance,
        'cover': cover
    }

def get_dimension_mappings():
    """Get all dimension mappings for easy lookup"""
    return {
        'dates': {d.date_id: {'year': d.year, 'quarter': d.quarter, 'period_label': d.period_label} 
                 for d in DimDate.query.all()},
        'geography': {g.geo_id: {'province': g.province, 'country': g.country, 'region': g.region} 
                     for g in DimGeography.query.all()},
        'industries': {i.industry_id: {'name': i.industry_name, 'sector': i.sector_group} 
                      for i in DimIndustry.query.all()},
        'demographics': {d.demo_id: {'sex': d.sex, 'age_group': d.age_group, 'occupation': d.occupation, 'status': d.employment_status} 
                        for d in DimDemographics.query.all()},
        'currencies': {c.currency_id: {'code': c.currency_code, 'name': c.currency_name} 
                       for c in DimCurrency.query.all()},
        'trade_groups': {t.trade_group_id: {'name': t.trade_group_name, 'code': t.trade_group_code} 
                        for t in DimTradeGroup.query.all()}
    }
