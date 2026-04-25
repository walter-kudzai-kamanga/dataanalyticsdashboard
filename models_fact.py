"""
Fact and Dimension table models for the Data Analytics Dashboard
"""

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import ForeignKey, Integer, String, Text, Numeric, Float
from sqlalchemy.orm import relationship

db = SQLAlchemy()


def _safe_year(year_value):
    try:
        return int(float(year_value))
    except (TypeError, ValueError):
        return None


def _norm(value):
    return str(value).strip().lower() if value is not None else ""

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
    
    year = _safe_year(filters.get('year'))
    region = filters.get('region')
    gender = filters.get('gender')

    query = db.session.query(FactLabour)
    if year is not None:
        query = query.join(DimDate, FactLabour.date_id == DimDate.date_id).filter(DimDate.year == year)
    if region and region != 'All':
        query = query.join(DimGeography, FactLabour.province_id == DimGeography.geo_id).filter(DimGeography.province == region)
    if gender and gender != 'All':
        query = query.join(DimDemographics, FactLabour.sex_id == DimDemographics.demo_id).filter(DimDemographics.sex == gender)

    all_records = query.filter(FactLabour.value.isnot(None)).all()
    employed_total = 0.0
    unemployed_total = 0.0

    for record in all_records:
        name = _norm(record.variable_name)
        try:
            value = float(record.value)
        except (TypeError, ValueError):
            continue
        if value <= 0:
            continue

        if any(token in name for token in ['unemploy', 'jobless']):
            unemployed_total += value
        elif any(token in name for token in ['employ', 'labour_force', 'labor_force', 'work']):
            employed_total += value

    # Fallback if uploads only provided one generic labour metric.
    if employed_total == 0 and unemployed_total == 0:
        total_population = sum(float(r.value) for r in all_records if r.value is not None)
        employed_total = total_population * 0.7
        unemployed_total = total_population * 0.3
    
    # Calculate derived metrics
    labour_force = employed_total + unemployed_total
    unemp_rate = (unemployed_total / labour_force * 100) if labour_force > 0 else 0
    
    result = {
        'labour_force': labour_force,
        'employed': employed_total,
        'unemployed': unemployed_total,
        'unemp_rate': unemp_rate
    }
    
    return result

def query_prices_kpis_fact(filters=None):
    """Extract prices KPIs from fact_prices table"""
    if filters is None:
        filters = {}
    
    year = _safe_year(filters.get('year'))
    region = filters.get('region')
    query = db.session.query(FactPrices)
    if year is not None:
        query = query.join(DimDate, FactPrices.date_id == DimDate.date_id).filter(DimDate.year == year)
    if region and region != 'All':
        query = query.join(DimGeography, FactPrices.province_id == DimGeography.geo_id).filter(DimGeography.province == region)

    rows = query.filter(FactPrices.value.isnot(None)).all()
    cpi_values, inflation_values, food_values = [], [], []
    for row in rows:
        name = _norm(row.variable_name)
        try:
            value = float(row.value)
        except (TypeError, ValueError):
            continue
        if 'cpi' in name or 'index' in name:
            cpi_values.append(value)
        if 'inflation' in name or 'mom' in name or 'yoy' in name:
            inflation_values.append(value)
        if 'food' in name:
            food_values.append(value)

    cpi_value = (sum(cpi_values) / len(cpi_values)) if cpi_values else 0.0
    inflation_rate = (sum(inflation_values) / len(inflation_values)) if inflation_values else 0.0
    food_cpi = (sum(food_values) / len(food_values)) if food_values else 0.0
    
    result = {
        'cpi': cpi_value,
        'mom': inflation_rate,  # Using inflation as month-over-month
        'yoy': inflation_rate,  # Using inflation as year-over-year
        'food': food_cpi
    }
    
    return result

def query_gdp_kpis_fact(filters=None):
    """Extract GDP KPIs from fact_national_accounts table"""
    if filters is None:
        filters = {}
    
    year = _safe_year(filters.get('year'))
    region = filters.get('region')

    query = db.session.query(FactNationalAccounts)
    if year is not None:
        query = query.join(DimDate, FactNationalAccounts.date_id == DimDate.date_id).filter(DimDate.year == year)
    if region and region != 'All':
        query = query.join(DimGeography, FactNationalAccounts.province_id == DimGeography.geo_id).filter(DimGeography.province == region)

    rows = query.filter(FactNationalAccounts.value.isnot(None)).all()
    gdp_total = 0.0
    gdp_growth = 0.0
    gdp_per_capita = 0.0
    agriculture_value = 0.0
    population = 0.0

    for row in rows:
        name = _norm(row.variable_name)
        try:
            value = float(row.value)
        except (TypeError, ValueError):
            continue
        if 'growth' in name:
            gdp_growth = value
        if 'per_capita' in name:
            gdp_per_capita = value
        if 'agric' in name:
            agriculture_value += value
        if 'population' in name:
            population += value
        if 'gdp' in name and 'growth' not in name and 'per_capita' not in name and 'share' not in name:
            gdp_total += value

    if gdp_per_capita == 0 and gdp_total > 0 and population > 0:
        gdp_per_capita = gdp_total / population
    
    # Calculate agriculture share
    agri_share = (agriculture_value / gdp_total * 100) if gdp_total > 0 else 15.0
    
    result = {
        'gdp': gdp_total,
        'growth': gdp_growth,
        'per_capita': gdp_per_capita,
        'agri_share': agri_share
    }
    
    return result

def query_trade_kpis_fact(filters=None):
    """Extract trade KPIs from fact_trade table"""
    if filters is None:
        filters = {}
    
    year = _safe_year(filters.get('year'))
    region = filters.get('region')

    query = db.session.query(FactTrade)
    if year is not None:
        query = query.join(DimDate, FactTrade.date_id == DimDate.date_id).filter(DimDate.year == year)
    if region and region != 'All':
        query = query.join(DimGeography, FactTrade.country_id == DimGeography.geo_id).filter(
            (DimGeography.country == region) | (DimGeography.province == region)
        )

    rows = query.filter(FactTrade.value.isnot(None)).all()
    exports_total = 0.0
    imports_total = 0.0
    trade_balance = 0.0
    for row in rows:
        name = _norm(row.variable_name)
        try:
            value = float(row.value)
        except (TypeError, ValueError):
            continue
        if 'balance' in name:
            trade_balance += value
        elif 'export' in name:
            exports_total += value
        elif 'import' in name:
            imports_total += value
    
    # Calculate net balance if not directly available
    if trade_balance == 0:
        trade_balance = exports_total - imports_total
    
    result = {
        'exports': exports_total,
        'imports': imports_total,
        'balance': trade_balance,  # Changed from 'trade_balance' to 'balance'
        'cover': (exports_total / imports_total * 100) if imports_total > 0 else 0  # Calculate cover ratio
    }
    
    return result

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
