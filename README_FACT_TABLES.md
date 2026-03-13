# Fact and Dimension Table Structure

This document describes the new fact and dimension table structure for the Data Analytics Dashboard.

## Overview

The database has been restructured to use a star schema with fact tables and dimension tables, following data warehousing best practices. This replaces the previous 100+ individual tables with a more efficient and scalable structure.

## Dimension Tables

### dim_date
Used by ALL subdivisions for time-based analysis.

**Columns:**
- `date_id` (INTEGER PRIMARY KEY)
- `year` (INTEGER)
- `quarter` (TEXT)
- `month_name` (TEXT)
- `month_number` (INTEGER)
- `period_label` (TEXT) - "2025 Q2", "2023-07"

### dim_geography
Covers provinces, countries, regions.

**Columns:**
- `geo_id` (INTEGER PRIMARY KEY)
- `province` (TEXT)
- `country` (TEXT)
- `region` (TEXT)

### dim_industry
All industries and high-level sectors.

**Columns:**
- `industry_id` (INTEGER PRIMARY KEY)
- `industry_name` (TEXT)
- `sector_group` (TEXT)

### dim_demographics
Sex, age, occupation, status.

**Columns:**
- `demo_id` (INTEGER PRIMARY KEY)
- `sex` (TEXT)
- `age_group` (TEXT)
- `occupation` (TEXT)
- `employment_status` (TEXT)

### dim_currency
Currency information for price data.

**Columns:**
- `currency_id` (INTEGER PRIMARY KEY)
- `currency_code` (TEXT)
- `currency_name` (TEXT)

### dim_trade_group
Trade group information for trade data.

**Columns:**
- `trade_group_id` (INTEGER PRIMARY KEY)
- `trade_group_name` (TEXT)
- `trade_group_code` (TEXT)

## Fact Tables

### fact_labour
Stores all labour statistics (employment, informal sector, migrants, job losses, hours, injury, NEET, status, earnings, etc.).

**Columns:**
- `labour_fact_id` (INTEGER PRIMARY KEY AUTOINCREMENT)
- `date_id` (INTEGER FK → dim_date)
- `industry_id` (INTEGER FK → dim_industry)
- `occupation_id` (INTEGER FK → dim_demographics)
- `sex_id` (INTEGER FK → dim_demographics)
- `age_group_id` (INTEGER FK → dim_demographics)
- `province_id` (INTEGER FK → dim_geography)
- `variable_name` (TEXT) - e.g. "employed_population", "informal_employment"
- `value` (DECIMAL)

**Replaces:** 35+ tables under LABOUR

### fact_prices
Stores CPI indices, inflation rates, weighted CPI, provincial CPI, USD/ZWL/ZIG calculations.

**Columns:**
- `price_fact_id` (INTEGER PRIMARY KEY AUTOINCREMENT)
- `date_id` (INTEGER FK → dim_date)
- `province_id` (INTEGER FK → dim_geography)
- `currency_id` (INTEGER FK → dim_currency)
- `variable_name` (TEXT) - e.g. "cpi_index", "monthly_inflation"
- `value` (DECIMAL)

**Replaces:** All PRICES tables (about 10)

### fact_national_accounts
GDP, constant prices, earnings, provincial-level GDP.

**Columns:**
- `accounts_fact_id` (INTEGER PRIMARY KEY AUTOINCREMENT)
- `date_id` (INTEGER FK → dim_date)
- `province_id` (INTEGER FK → dim_geography)
- `industry_id` (INTEGER FK → dim_industry)
- `variable_name` (TEXT) - "gdp_constant", "earnings_usd"
- `value` (DECIMAL)

**Replaces:** All NATIONAL ACCOUNTS tables (GDP 2021/2022/2023, shares, USD earnings)

### fact_trade
Exports, imports, shares, values, COMESA, ECCAS, EU, AfCFTA.

**Columns:**
- `trade_fact_id` (INTEGER PRIMARY KEY AUTOINCREMENT)
- `date_id` (INTEGER FK → dim_date)
- `country_id` (INTEGER FK → dim_geography)
- `trade_group_id` (INTEGER FK → dim_trade_group)
- `variable_name` (TEXT) - "export_value", "import_share"
- `value` (DECIMAL)

**Replaces:** 20+ trade/BOP tables

## Benefits

1. **Scalability:** Easy to add new variables without creating new tables
2. **Performance:** Better query performance with proper indexing
3. **Consistency:** Uniform structure across all data domains
4. **Maintenance:** Easier to maintain and extend
5. **Analysis:** Simplified cross-domain analysis

## Migration Status

- ✅ Dimension tables created and populated
- ✅ Fact tables created
- ✅ Migration script executed
- ✅ Application code updated
- ✅ 60+ labour records migrated
- ⏳ Additional data migration needed for prices, national accounts, trade

## Usage

The new structure is used by:
- `models_fact.py` - SQLAlchemy models
- `app_fact.py` - Updated Flask application
- `migrate_to_fact_tables.py` - Migration script

## Query Examples

### Get employment by province for 2025:
```sql
SELECT dg.province, SUM(fl.value) as total_employed
FROM fact_labour fl
JOIN dim_geography dg ON fl.province_id = dg.geo_id
JOIN dim_date dd ON fl.date_id = dd.date_id
WHERE dd.year = 2025 AND fl.variable_name = 'employed_population'
GROUP BY dg.province
ORDER BY total_employed DESC;
```

### Get GDP by year:
```sql
SELECT dd.year, SUM(fna.value) as total_gdp
FROM fact_national_accounts fna
JOIN dim_date dd ON fna.date_id = dd.date_id
WHERE fna.variable_name = 'gdp_constant'
GROUP BY dd.year
ORDER BY dd.year;
```

### Get CPI trends:
```sql
SELECT dd.period_label, fp.value as cpi_index
FROM fact_prices fp
JOIN dim_date dd ON fp.date_id = dd.date_id
WHERE fp.variable_name = 'cpi_index'
ORDER BY dd.date_id;
```

## Next Steps

1. Complete migration of remaining data (prices, national accounts, trade)
2. Add more comprehensive variable mappings
3. Implement data validation and quality checks
4. Add historical data migration
5. Optimize queries and add more indexes as needed
