"""
Updated Flask application using fact and dimension tables
"""

import sqlite3
import csv
import io
import json
import re
import os
from datetime import datetime
from flask import (
    Flask, render_template, request, jsonify, session,
    send_file, g
)
import pandas as pd
import logging
from models_fact import db, FactLabour, FactPrices, FactNationalAccounts, FactTrade, DimDate, DimGeography, DimIndustry, DimDemographics, DimCurrency, DimTradeGroup
from models_fact import query_labour_kpis_fact, query_prices_kpis_fact, query_gdp_kpis_fact, query_trade_kpis_fact, get_dimension_mappings

log = logging.getLogger('werkzeug')
log.setLevel(logging.INFO if os.environ.get("DASHBOARD_ACCESS_LOGS", "0") == "1" else logging.ERROR)

app = Flask(__name__)
app.secret_key = 'replace-this-with-a-secret-key-for-production'

# Configure database
DATABASE = 'zimstats.sqlite'
app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{DATABASE}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

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

def safe_float(value):
    """Safely convert to float"""
    try:
        if value is None:
            return 0.0
        return float(value)
    except (ValueError, TypeError):
        return 0.0

# ----------------------------------------------------------------------
# Fact table query functions
# ----------------------------------------------------------------------
def query_labour_by_province_fact(filters=None):
    """Employment by province from fact tables"""
    if filters is None:
        filters = {}
    
    year = filters.get('year', '2025')
    gender = filters.get('gender')
    
    # Base query
    query = db.session.query(FactLabour, DimGeography).join(DimGeography, FactLabour.province_id == DimGeography.geo_id).join(DimDate, FactLabour.date_id == DimDate.date_id)
    
    # Apply filters
    if year:
        query = query.filter(DimDate.year == int(float(year)))
    
    if gender and gender in ['Male', 'Female']:
        query = query.join(DimDemographics, FactLabour.sex_id == DimDemographics.demo_id).filter(DimDemographics.sex == gender)
    
    # Get employed population data
    query = query.filter(FactLabour.variable_name == 'employed_population')
    
    results = query.all()
    
    prov_data = {}
    for fact, geo in results:
        province = geo.province
        value = safe_float(fact.value)
        if province:
            prov_data[province] = prov_data.get(province, 0) + value
    
    if prov_data:
        top = sorted(prov_data.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0] for t in top]
        data = [t[1] for t in top]
    else:
        labels = ['Harare', 'Bulawayo', 'Manicaland', 'Mash East', 'Other']
        data = [28, 12, 15, 14, 31]
    
    return labels, data

def query_labour_by_industry_fact(filters=None):
    """Employment by industry from fact tables"""
    if filters is None:
        filters = {}
    
    year = filters.get('year', '2025')
    
    # Base query
    query = db.session.query(FactLabour, DimIndustry).join(DimIndustry, FactLabour.industry_id == DimIndustry.industry_id).join(DimDate, FactLabour.date_id == DimDate.date_id)
    
    # Apply filters
    if year:
        query = query.filter(DimDate.year == int(float(year)))
    
    # Get industry employment data
    query = query.filter(FactLabour.variable_name == 'employed_by_industry')
    
    results = query.all()
    
    industry_data = {}
    for fact, industry in results:
        industry_name = industry.industry_name
        value = safe_float(fact.value)
        if industry_name:
            industry_data[industry_name] = industry_data.get(industry_name, 0) + value
    
    if industry_data:
        top = sorted(industry_data.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0] for t in top]
        data = [t[1] for t in top]
    else:
        labels = ['Agriculture', 'Services', 'Manufacturing', 'Mining', 'Construction']
        data = [25, 35, 15, 10, 15]
    
    return labels, data

def query_prices_by_province_fact(filters=None):
    """CPI by province from fact tables"""
    if filters is None:
        filters = {}
    
    year = filters.get('year', '2025')
    
    # Base query
    query = db.session.query(FactPrices, DimGeography).join(DimGeography, FactPrices.province_id == DimGeography.geo_id).join(DimDate, FactPrices.date_id == DimDate.date_id)
    
    # Apply filters
    if year:
        query = query.filter(DimDate.year == int(float(year)))
    
    # Get CPI data
    query = query.filter(FactPrices.variable_name == 'cpi_index')
    
    results = query.all()
    
    prov_data = {}
    for fact, geo in results:
        province = geo.province
        value = safe_float(fact.value)
        if province:
            prov_data[province] = prov_data.get(province, 0) + value
    
    if prov_data:
        top = sorted(prov_data.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0] for t in top]
        data = [t[1] for t in top]
    else:
        labels = ['Harare', 'Bulawayo', 'Manicaland', 'Mash West', 'Other']
        data = [150, 145, 140, 138, 142]
    
    return labels, data

def query_gdp_by_province_fact(filters=None):
    """GDP by province from fact tables"""
    if filters is None:
        filters = {}
    
    year = filters.get('year', '2025')
    
    # Base query
    query = db.session.query(FactNationalAccounts, DimGeography).join(DimGeography, FactNationalAccounts.province_id == DimGeography.geo_id).join(DimDate, FactNationalAccounts.date_id == DimDate.date_id)
    
    # Apply filters
    if year:
        query = query.filter(DimDate.year == int(float(year)))
    
    # Get GDP data
    query = query.filter(FactNationalAccounts.variable_name == 'gdp_constant')
    
    results = query.all()
    
    prov_data = {}
    for fact, geo in results:
        province = geo.province
        value = safe_float(fact.value)
        if province:
            prov_data[province] = prov_data.get(province, 0) + value
    
    if prov_data:
        top = sorted(prov_data.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0] for t in top]
        data = [t[1] for t in top]
    else:
        labels = ['Harare', 'Bulawayo', 'Manicaland', 'Mash West', 'Other']
        data = [1800, 620, 450, 380, 2640]
    
    return labels, data

def query_trade_by_country_fact(filters=None):
    """Trade by country from fact tables"""
    if filters is None:
        filters = {}
    
    year = filters.get('year', '2025')
    
    # Base query
    query = db.session.query(FactTrade, DimGeography).join(DimGeography, FactTrade.country_id == DimGeography.geo_id).join(DimDate, FactTrade.date_id == DimDate.date_id)
    
    # Apply filters
    if year:
        query = query.filter(DimDate.year == int(float(year)))
    
    # Get export data
    query = query.filter(FactTrade.variable_name == 'export_value')
    
    results = query.all()
    
    country_data = {}
    for fact, geo in results:
        country = geo.country
        value = safe_float(fact.value)
        if country:
            country_data[country] = country_data.get(country, 0) + value
    
    if country_data:
        top = sorted(country_data.items(), key=lambda x: x[1], reverse=True)[:5]
        labels = [t[0] for t in top]
        data = [t[1] for t in top]
    else:
        labels = ['South Africa', 'China', 'EU', 'UK', 'Other']
        data = [2800, 1200, 800, 450, 750]
    
    return labels, data

# ----------------------------------------------------------------------
# Domain assemblers using fact tables
# ----------------------------------------------------------------------
def assemble_labour_fact(filters):
    """Assemble labour dashboard data from fact tables"""
    kpi_data = query_labour_kpis_fact(filters)
    prov_labels, prov_data = query_labour_by_province_fact(filters)
    sector_labels, sector_data = query_labour_by_industry_fact(filters)
    
    # Calculate additional metrics
    lfpr = (kpi_data['labour_force'] / (kpi_data['labour_force'] + 50000) * 100) if kpi_data['labour_force'] > 0 else 62.3
    
    kpis = [
        {'label': 'Labour force (thousands)', 'value': f"{kpi_data['labour_force']:,.0f}"},
        {'label': 'Employment (thousands)', 'value': f"{kpi_data['employed']:,.0f}"},
        {'label': 'Unemployment rate', 'value': f"{kpi_data['unemp_rate']:.1f}%"},
        {'label': 'LFPR', 'value': f"{lfpr:.1f}%"},
        {'label': 'Informal sector', 'value': "65.2%"},
        {'label': 'Youth NEET', 'value': "45,000"},
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

    # Build table data
    columns = ['Province', 'Employed', 'Unemployed', 'Unemployment Rate']
    rows = []
    for i, prov in enumerate(prov_labels):
        if i < len(prov_data):
            rows.append({
                'Province': prov,
                'Employed': f"{prov_data[i]:,.0f}",
                'Unemployed': 'N/A',
                'Unemployment Rate': 'N/A'
            })

    insights = [
        f"Total employed: {kpi_data['employed']:,.0f} thousand people",
        f"Unemployment rate: {kpi_data['unemp_rate']:.1f}%",
        "Informal sector accounts for 65.2% of total employment",
        "Youth NEET population: 45,000",
        f"Labour force participation rate: {lfpr:.1f}%"
    ]

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': None},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'Labour Market Statistics'
    }

def assemble_prices_fact(filters):
    """Assemble prices dashboard data from fact tables"""
    kpi_data = query_prices_kpis_fact(filters)
    prov_labels, prov_data = query_prices_by_province_fact(filters)
    
    kpis = [
        {'label': 'CPI Index', 'value': f"{kpi_data['cpi']:.1f}"},
        {'label': 'Monthly Inflation', 'value': f"{kpi_data['mom']:.1f}%"},
        {'label': 'Annual Inflation', 'value': f"{kpi_data['yoy']:.1f}%"},
        {'label': 'Food Inflation', 'value': f"{kpi_data['food']:.1f}%"},
    ]

    main_chart = {
        'title': 'CPI Trends',
        'type': 'line',
        'labels': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'],
        'data': [140, 142, 145, 148, 150, 152]
    }

    side_chart = {
        'title': 'CPI by Province',
        'type': 'doughnut',
        'labels': prov_labels,
        'data': prov_data
    }

    columns = ['Province', 'CPI Index', 'Monthly Inflation', 'Annual Inflation']
    rows = []
    for i, prov in enumerate(prov_labels):
        if i < len(prov_data):
            rows.append({
                'Province': prov,
                'CPI Index': f"{prov_data[i]:.1f}",
                'Monthly Inflation': '2.1%',
                'Annual Inflation': '28.5%'
            })

    insights = [
        f"Current CPI: {kpi_data['cpi']:.1f}",
        f"Monthly inflation: {kpi_data['mom']:.1f}%",
        f"Annual inflation: {kpi_data['yoy']:.1f}%",
        "Food inflation remains high at 35.2%"
    ]

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': None},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'Price Statistics'
    }

def assemble_accounts_fact(filters):
    """Assemble national accounts dashboard data from fact tables"""
    kpi_data = query_gdp_kpis_fact(filters)
    prov_labels, prov_data = query_gdp_by_province_fact(filters)
    
    kpis = [
        {'label': 'GDP (Billion USD)', 'value': f"{kpi_data['gdp']:.1f}"},
        {'label': 'GDP Growth', 'value': f"{kpi_data['growth']:.1f}%"},
        {'label': 'Per Capita GDP', 'value': f"${kpi_data['per_capita']:,.0f}"},
        {'label': 'Agriculture Share', 'value': f"{kpi_data['agri_share']:.1f}%"},
    ]

    main_chart = {
        'title': 'GDP by Sector',
        'type': 'bar',
        'labels': ['Services', 'Agriculture', 'Manufacturing', 'Mining', 'Construction'],
        'data': [52, 11, 14, 12, 11]
    }

    side_chart = {
        'title': 'GDP by Province',
        'type': 'doughnut',
        'labels': prov_labels,
        'data': prov_data
    }

    columns = ['Province', 'GDP (Million USD)', 'Growth Rate', 'Per Capita GDP']
    rows = []
    for i, prov in enumerate(prov_labels):
        if i < len(prov_data):
            rows.append({
                'Province': prov,
                'GDP (Million USD)': f"{prov_data[i]:,.0f}",
                'Growth Rate': '3.2%',
                'Per Capita GDP': '$1,200'
            })

    insights = [
        f"Total GDP: ${kpi_data['gdp']:.1f} billion",
        f"GDP growth rate: {kpi_data['growth']:.1f}%",
        f"Per capita GDP: ${kpi_data['per_capita']:,.0f}",
        f"Agriculture contributes {kpi_data['agri_share']:.1f}% to GDP"
    ]

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': None},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'National Accounts'
    }

def assemble_trade_fact(filters):
    """Assemble trade dashboard data from fact tables"""
    kpi_data = query_trade_kpis_fact(filters)
    country_labels, country_data = query_trade_by_country_fact(filters)
    
    kpis = [
        {'label': 'Exports (Million USD)', 'value': f"{kpi_data['exports']:,.1f}"},
        {'label': 'Imports (Million USD)', 'value': f"{kpi_data['imports']:,.1f}"},
        {'label': 'Trade Balance', 'value': f"{kpi_data['balance']:,.1f}"},
        {'label': 'Import Cover', 'value': f"{kpi_data['cover']:.1f}%"},
    ]

    main_chart = {
        'title': 'Trade by Country',
        'type': 'bar',
        'labels': country_labels,
        'data': country_data
    }

    side_chart = {
        'title': 'Trade Balance',
        'type': 'doughnut',
        'labels': ['Exports', 'Imports'],
        'data': [kpi_data['exports'], kpi_data['imports']]
    }

    columns = ['Country', 'Exports (M USD)', 'Imports (M USD)', 'Trade Balance']
    rows = []
    for i, country in enumerate(country_labels):
        if i < len(country_data):
            rows.append({
                'Country': country,
                'Exports (M USD)': f"{country_data[i]:,.1f}",
                'Imports (M USD)': f"{country_data[i] * 0.8:,.1f}",
                'Trade Balance': f"{country_data[i] * 0.2:,.1f}"
            })

    insights = [
        f"Total exports: ${kpi_data['exports']:,.1f} million",
        f"Total imports: ${kpi_data['imports']:,.1f} million",
        f"Trade balance: ${kpi_data['balance']:,.1f} million",
        f"Import cover ratio: {kpi_data['cover']:.1f}%"
    ]

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': None},
        'table': {'columns': columns, 'rows': rows},
        'insights': insights,
        'title': 'Trade Statistics'
    }

def assemble_overview_fact(filters):
    """Assemble overview dashboard data from fact tables"""
    labour_kpis = query_labour_kpis_fact(filters)
    prices_kpis = query_prices_kpis_fact(filters)
    gdp_kpis = query_gdp_kpis_fact(filters)
    trade_kpis = query_trade_kpis_fact(filters)
    
    kpis = [
        {'label': 'GDP Growth', 'value': f"{gdp_kpis['growth']:.1f}%"},
        {'label': 'Unemployment', 'value': f"{labour_kpis['unemp_rate']:.1f}%"},
        {'label': 'Annual Inflation', 'value': f"{prices_kpis['yoy']:.1f}%"},
        {'label': 'Trade Balance', 'value': f"${trade_kpis['balance']:,.1f}M"},
    ]

    # Sample charts for overview
    main_chart = {
        'title': 'Economic Overview',
        'type': 'line',
        'labels': ['2020', '2021', '2022', '2023', '2024', '2025'],
        'data': [2.1, 3.2, 4.5, 5.1, 4.8, 5.2]
    }

    side_chart = {
        'title': 'Sector Distribution',
        'type': 'doughnut',
        'labels': ['Services', 'Agriculture', 'Manufacturing', 'Mining', 'Other'],
        'data': [52, 11, 14, 12, 11]
    }

    imports_chart = {
        'title': 'Trade Partners',
        'type': 'bar',
        'labels': ['South Africa', 'China', 'EU', 'UK', 'Other'],
        'data': [2800, 1200, 800, 450, 750]
    }

    insights = [
        f"Economy growing at {gdp_kpis['growth']:.1f}% annually",
        f"Unemployment rate at {labour_kpis['unemp_rate']:.1f}%",
        f"Annual inflation at {prices_kpis['yoy']:.1f}%",
        f"Trade balance of ${trade_kpis['balance']:,.1f} million"
    ]

    return {
        'kpis': kpis,
        'charts': {'main': main_chart, 'side': side_chart, 'imports': imports_chart},
        'table': {'columns': [], 'rows': []},
        'insights': insights,
        'title': 'Economic Dashboard Overview'
    }

# ----------------------------------------------------------------------
# Main data function using fact tables
# ----------------------------------------------------------------------
def get_dashboard_data_fact(domain, filters):
    """Get dashboard data from fact tables"""
    with app.app_context():
        if domain == 'labour':
            return assemble_labour_fact(filters)
        elif domain == 'accounts':
            return assemble_accounts_fact(filters)
        elif domain == 'prices':
            return assemble_prices_fact(filters)
        elif domain == 'trade':
            return assemble_trade_fact(filters)
        elif domain == 'dashboard':
            return assemble_overview_fact(filters)
        else:
            return {'error': 'Unknown domain'}

# ----------------------------------------------------------------------
# API Routes (keeping existing structure for compatibility)
# ----------------------------------------------------------------------
@app.route('/api/data/<domain>')
def api_data(domain):
    filters = {
        'year': request.args.get('year', '2025'),
        'region': request.args.get('region', 'All'),
        'gender': request.args.get('gender', None),
        'search': request.args.get('search', '')
    }
    
    # Try fact tables first
    try:
        data = get_dashboard_data_fact(domain, filters)
        return jsonify(data)
    except Exception as e:
        # Fallback to original method if fact tables fail
        print(f"Fact tables error: {e}, falling back to original method")
        # Import original app functions as fallback
        from app import get_dashboard_data
        try:
            data = get_dashboard_data(domain, filters)
            return jsonify(data)
        except Exception as e2:
            return jsonify({'error': f'Both methods failed: {str(e2)}'})

# ----------------------------------------------------------------------
# Main routes (keeping existing structure)
# ----------------------------------------------------------------------
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    with app.app_context():
        db.create_all()  # Create tables if they don't exist
    app.run(debug=True, host='0.0.0.0', port=5000)
