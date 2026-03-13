-- Database Schema for Data Analytics Dashboard
-- Fact and Dimension Table Structure

-- DIMENSION TABLES

-- DIMENSION 1 — dim_date
-- Used by ALL subdivisions
CREATE TABLE IF NOT EXISTS dim_date (
    date_id INTEGER PRIMARY KEY,
    year INTEGER,
    quarter TEXT,
    month_name TEXT,
    month_number INTEGER,
    period_label TEXT   -- "2025 Q2", "2023-07"
);

-- DIMENSION 2 — dim_geography
-- Covers provinces, countries, regions
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_id INTEGER PRIMARY KEY,
    province TEXT,
    country TEXT,
    region TEXT
);

-- DIMENSION 3 — dim_industry
-- All industries and high-level sectors
CREATE TABLE IF NOT EXISTS dim_industry (
    industry_id INTEGER PRIMARY KEY,
    industry_name TEXT,
    sector_group TEXT
);

-- DIMENSION 4 — dim_demographics
-- Sex, age, occupation, status
CREATE TABLE IF NOT EXISTS dim_demographics (
    demo_id INTEGER PRIMARY KEY,
    sex TEXT,
    age_group TEXT,
    occupation TEXT,
    employment_status TEXT
);

-- Additional dimension for currency
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_id INTEGER PRIMARY KEY,
    currency_code TEXT,
    currency_name TEXT
);

-- Additional dimension for trade groups
CREATE TABLE IF NOT EXISTS dim_trade_group (
    trade_group_id INTEGER PRIMARY KEY,
    trade_group_name TEXT,
    trade_group_code TEXT
);

-- FACT TABLES

-- FACT TABLE 1 — LABOUR
-- Stores all labour statistics (employment, informal sector, migrants, job losses, hours, injury, NEET, status, earnings, etc.)
CREATE TABLE IF NOT EXISTS fact_labour (
    labour_fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_id INTEGER,
    industry_id INTEGER,
    occupation_id INTEGER,
    sex_id INTEGER,
    age_group_id INTEGER,
    province_id INTEGER,
    variable_name TEXT,   -- e.g. "employed_population", "informal_employment"
    value DECIMAL,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (industry_id) REFERENCES dim_industry(industry_id),
    FOREIGN KEY (occupation_id) REFERENCES dim_demographics(demo_id),
    FOREIGN KEY (sex_id) REFERENCES dim_demographics(demo_id),
    FOREIGN KEY (age_group_id) REFERENCES dim_demographics(demo_id),
    FOREIGN KEY (province_id) REFERENCES dim_geography(geo_id)
);

-- FACT TABLE 2 — PRICES
-- Stores CPI indices, inflation rates, weighted CPI, provincial CPI, USD/ZWL/ZIG calculations
CREATE TABLE IF NOT EXISTS fact_prices (
    price_fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_id INTEGER,
    province_id INTEGER,
    currency_id INTEGER,
    variable_name TEXT,    -- e.g. "cpi_index", "monthly_inflation"
    value DECIMAL,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (province_id) REFERENCES dim_geography(geo_id),
    FOREIGN KEY (currency_id) REFERENCES dim_currency(currency_id)
);

-- FACT TABLE 3 — NATIONAL ACCOUNTS
-- GDP, constant prices, earnings, provincial-level GDP
CREATE TABLE IF NOT EXISTS fact_national_accounts (
    accounts_fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_id INTEGER,
    province_id INTEGER,
    industry_id INTEGER,
    variable_name TEXT,   -- "gdp_constant", "earnings_usd"
    value DECIMAL,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (province_id) REFERENCES dim_geography(geo_id),
    FOREIGN KEY (industry_id) REFERENCES dim_industry(industry_id)
);

-- FACT TABLE 4 — TRADE (BOP & FINANCE)
-- Exports, imports, shares, values, COMESA, ECCAS, EU, AfCFTA
CREATE TABLE IF NOT EXISTS fact_trade (
    trade_fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date_id INTEGER,
    country_id INTEGER,
    trade_group_id INTEGER,  -- COMESA, ECCAS, EU, AfCFTA
    variable_name TEXT,   -- "export_value", "import_share"
    value DECIMAL,
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (country_id) REFERENCES dim_geography(geo_id),
    FOREIGN KEY (trade_group_id) REFERENCES dim_trade_group(trade_group_id)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_fact_labour_date ON fact_labour(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_labour_variable ON fact_labour(variable_name);
CREATE INDEX IF NOT EXISTS idx_fact_labour_province ON fact_labour(province_id);
CREATE INDEX IF NOT EXISTS idx_fact_prices_date ON fact_prices(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_prices_variable ON fact_prices(variable_name);
CREATE INDEX IF NOT EXISTS idx_fact_accounts_date ON fact_national_accounts(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_accounts_variable ON fact_national_accounts(variable_name);
CREATE INDEX IF NOT EXISTS idx_fact_trade_date ON fact_trade(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_trade_variable ON fact_trade(variable_name);
CREATE INDEX IF NOT EXISTS idx_dim_date_year ON dim_date(year);
CREATE INDEX IF NOT EXISTS idx_dim_geography_province ON dim_geography(province);
CREATE INDEX IF NOT EXISTS idx_dim_industry_name ON dim_industry(industry_name);
