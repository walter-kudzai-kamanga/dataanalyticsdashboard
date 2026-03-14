"""
Upload Data Mapper - Converts CSV/Excel files to Fact and Dimension Table Structure
Maps existing table formats to new fact table schema
"""

import pandas as pd
import sqlite3
from datetime import datetime
import re
from models_fact import db, FactLabour, FactPrices, FactNationalAccounts, FactTrade, DimDate, DimGeography, DimIndustry, DimDemographics, DimCurrency, DimTradeGroup

# Fix pandas warnings
pd.set_option('future.no_silent_downcasting', True)

class UploadMapper:
    def __init__(self):
        """Initialize with SQLAlchemy database connection"""
        self.setup_dimension_mappings()
    
    def setup_dimension_mappings(self):
        """Setup dimension key mappings"""
        # Geography mappings
        self.geo_mappings = {
            # Provinces
            'harare': 1, 'bulawayo': 2, 'manicaland': 3, 'mashonaland central': 4,
            'mashonaland east': 5, 'mashonaland west': 6, 'mashonaland north': 7,
            'matabeland north': 8, 'matabeland south': 9, 'midlands': 10,
            'masvingo': 11, 'matabeleland': 12, 'mashonaland': 13,
            # Countries
            'south africa': 14, 'china': 15, 'eu': 16, 'uk': 17, 'usa': 18, 'zambia': 19
        }
        
        # Industry mappings
        self.industry_mappings = {
            'agriculture': 1, 'mining': 2, 'manufacturing': 3, 'construction': 4,
            'transport': 5, 'finance': 6, 'services': 7, 'government': 8,
            'education': 9, 'health': 10, 'retail': 11, 'tourism': 12,
            'accommodation_and_food_service_activities': 13,
            'activities_of_households_as_employers': 14,
            'administrative_and_support_service_activities': 15,
            'arts_entertainment_and_recreation': 16,
            'electricity_gas_steam_and_air_conditioning_supply': 17,
            'financial_and_insurance_activities': 18,
            'human_health_and_social_work_activities': 19,
            'information_and_communication': 20,
            'mining_and_quarrying': 21,
            'other_service_activities': 22,
            'professional_scientific_and_technical_activities': 23,
            'public_administration_and_defence': 24,
            'real_estate_activities': 25,
            'transportation_and_storage': 26,
            'wholesale_and_retail_trade': 27,
            'water_supply_sewerage_waste_management_and_remediation': 28
        }
        
        # Sex mappings
        self.sex_mappings = {
            'male': 1, 'female': 2, 'both': 3, 'total': 4
        }
        
        # Age group mappings
        self.age_mappings = {
            '15-24': 1, '25-34': 2, '35-44': 3, '45-54': 4, '55+': 5
        }
    
    def get_date_id(self, year=None, quarter=None, month=None):
        """Get or create date_id for given year/quarter/month"""
        if year:
            year = int(str(year).replace(',', ''))
        else:
            year = 2025  # default
            
        # Check if date exists
        existing_date = db.session.query(DimDate).filter(
            DimDate.year == year,
            DimDate.quarter == quarter
        ).first()
        
        if existing_date:
            return existing_date.date_id
        
        # Create new date entry
        period_label = f"{year} Q{quarter}" if quarter else str(year)
        new_date = DimDate(
            year=year,
            quarter=quarter,
            period_label=period_label
        )
        db.session.add(new_date)
        db.session.commit()
        
        return new_date.date_id
    
    def get_geo_id(self, location):
        """Get geo_id for province or country"""
        if not location:
            return None
            
        location = str(location).lower().strip()
        return self.geo_mappings.get(location)
    
    def get_industry_id(self, industry):
        """Get industry_id for industry name"""
        if not industry:
            return None
            
        industry = str(industry).lower().strip()
        return self.industry_mappings.get(industry)
    
    def get_demo_id(self, sex=None, age_group=None, occupation=None):
        """Get demo_id for demographics"""
        # For now, return sex_id as primary demo_id
        # In future, can expand to handle age/occupation combinations
        if sex:
            sex = str(sex).lower().strip()
            return self.sex_mappings.get(sex)
        return None
    
    def map_labour_data(self, df, table_name):
        """Map labour data to fact_labour table"""
        mapped_data = []
        
        for _, row in df.iterrows():
            mapped_row = {
                'date_id': self.get_date_id(year=self.extract_year(row)),
                'variable_name': self.determine_labour_variable(table_name, row),
                'value': self.extract_value(row)
            }
            
            # Map demographics if available
            if 'sex' in row and pd.notna(row['sex']):
                mapped_row['sex_id'] = self.get_demo_id(sex=row['sex'])
            
            if 'age_group' in row and pd.notna(row['age_group']):
                mapped_row['age_group_id'] = self.get_demo_id(age_group=row['age_group'])
            
            # Handle specific column mappings for youth population data
            if 'male_youth_population' in row and pd.notna(row['male_youth_population']):
                mapped_row['variable_name'] = 'male_youth_population'
                mapped_row['sex_id'] = 1  # Male
                mapped_row['age_group_id'] = 1  # Youth (15-24)
                mapped_row['value'] = float(row['male_youth_population']) if pd.notna(row['male_youth_population']) else 0
            
            if 'female_youth_population' in row and pd.notna(row['female_youth_population']):
                mapped_row['variable_name'] = 'female_youth_population'
                mapped_row['sex_id'] = 2  # Female
                mapped_row['age_group_id'] = 1  # Youth (15-24)
                mapped_row['value'] = float(row['female_youth_population']) if pd.notna(row['female_youth_population']) else 0
            
            # Only include rows with actual data
            if mapped_row.get('value') not in [None, 0, '']:
                mapped_data.append(mapped_row)
        
        return mapped_data
    
    def determine_labour_variable(self, table_name, row):
        """Determine variable_name based on table name and row content"""
        # If specific columns exist, use them directly
        if 'male_youth_population' in row and pd.notna(row['male_youth_population']):
            return 'male_youth_population'
        if 'female_youth_population' in row and pd.notna(row['female_youth_population']):
            return 'female_youth_population'
        
        table_lower = table_name.lower()
        
        # Map based on table name patterns
        if 'average income' in table_lower:
            return 'average_income_by_industry'
        elif 'discouraged job seekers' in table_lower:
            return 'discouraged_job_seekers'
        elif 'job losses' in table_lower:
            return 'job_losses_by_industry'
        elif 'work related illness' in table_lower:
            return 'work_related_illness'
        elif 'employed population' in table_lower:
            if 'informal employment' in table_lower:
                return 'informal_employment'
            elif 'occupation' in table_lower:
                return 'employment_by_occupation'
            elif 'industry' in table_lower:
                return 'employment_by_industry'
            elif 'province' in table_lower:
                return 'employment_by_province'
            elif 'status of employment' in table_lower:
                return 'employment_by_status'
            elif 'actual hours worked' in table_lower:
                return 'employment_by_hours_worked'
            elif 'area' in table_lower:
                return 'employment_by_area'
            else:
                return 'employed_population'
        elif 'time-related under-employment' in table_lower:
            return 'under_employment'
        elif 'population of youth' in table_lower:
            return 'youth_neet_population'
        elif 'labour migrants' in table_lower:
            return 'labour_migrants'
        elif 'migrants by industry' in table_lower:
            if 'percent' in table_lower:
                return 'industry_migrants_percent'
            else:
                return 'industry_migrants_number'
        elif 'employee annual average' in table_lower:
            return 'employee_annual_average'
        elif 'employee annual earnings' in table_lower:
            if 'zwl' in table_lower:
                return 'annual_earnings_zwl'
            elif 'usd' in table_lower:
                return 'annual_earnings_usd'
            else:
                return 'annual_earnings'
        elif 'employee quarterly' in table_lower:
            if 'average' in table_lower:
                return 'quarterly_average'
            elif 'earnings' in table_lower:
                if 'zwl' in table_lower or 'zig' in table_lower:
                    return 'quarterly_earnings_zwl'
                elif 'usd' in table_lower:
                    return 'quarterly_earnings_usd'
                else:
                    return 'quarterly_earnings'
            else:
                return 'quarterly_employee_data'
        elif 'employment by' in table_lower:
            if 'province' in table_lower:
                return 'employment_by_province'
            elif 'area' in table_lower:
                return 'employment_by_area'
            elif 'sex' in table_lower:
                return 'employment_by_sex'
            else:
                return 'employment'
            return 'employee_average'
        elif 'earnings' in table_lower:
            return 'employee_earnings'
        else:
            # Try to determine from variable column
            if 'variable' in row and pd.notna(row['variable']):
                return str(row['variable']).lower().replace(' ', '_')
            return 'unknown_variable'
    
    def map_prices_data(self, df, table_name):
        """Map prices data to fact_prices table"""
        mapped_data = []
        
        for _, row in df.iterrows():
            # Extract date from period column for prices data
            date_id = self.get_date_id(year=2025)  # Default
            if 'period' in row and pd.notna(row['period']):
                try:
                    # Try to extract year from period like '2024-04-01'
                    period_str = str(row['period'])
                    if '-' in period_str:
                        year_part = period_str.split('-')[0]
                        date_id = self.get_date_id(year=int(year_part))
                        print(f"DEBUG: Extracted year {year_part} from period {period_str}, date_id={date_id}")
                except:
                    pass
            
            mapped_row = {
                'date_id': date_id,
                'variable_name': self.determine_prices_variable(table_name, row),
                'value': self.extract_value(row),
                'currency_id': 1  # Default to USD
            }
            
            # Map province if available
            if 'province' in row and pd.notna(row['province']):
                mapped_row['province_id'] = self.get_geo_id(row['province'])
            
            # Include rows with actual data
            if mapped_row.get('value') not in [None, 0, '']:
                mapped_data.append(mapped_row)
        
        return mapped_data
    
    def determine_prices_variable(self, table_name, row):
        """Determine prices variable based on table name"""
        # First priority: use 'variable' column if it exists and has valid data
        if 'variable' in row and pd.notna(row['variable']):
            variable_value = str(row['variable']).lower().replace(' ', '_')
            print(f"DEBUG: Using variable column value: {variable_value}")
            return variable_value
        
        # Fallback: map based on table name patterns
        table_lower = table_name.lower()
        
        # Map based on table name patterns
        if 'cpi usd index' in table_lower:
            return 'cpi_usd_index'
        elif 'cpi usd monthly and yearly inflation' in table_lower:
            return 'cpi_usd_monthly_yearly_inflation'
        elif 'cpi usd year-on-year inflation' in table_lower:
            return 'cpi_usd_year_on_year_inflation'
        elif 'cpi weighted annual summary' in table_lower:
            return 'cpi_weighted_annual_summary'
        elif 'cpi weighted index' in table_lower:
            return 'cpi_weighted_index'
        elif 'cpi weighted monthly and yearly inflation' in table_lower:
            return 'cpi_weighted_monthly_yearly_inflation'
        elif 'cpi zwg index' in table_lower:
            return 'cpi_zwg_index'
        elif 'provincial cpi index usd' in table_lower:
            return 'provincial_cpi_usd_index'
        elif 'provincial cpi zwg index' in table_lower:
            return 'provincial_cpi_zwg_index'
        elif 'provincial monthly usd inflation rates' in table_lower:
            return 'provincial_monthly_usd_inflation_rates'
        elif 'cpi index' in table_lower:
            if 'usd' in table_lower:
                return 'cpi_usd'
            elif 'zwg' in table_lower:
                return 'cpi_zwg'
            elif 'weighted' in table_lower:
                return 'cpi_weighted'
            elif 'provincial' in table_lower:
                return 'provincial_cpi'
            else:
                return 'cpi_index'
        elif 'inflation' in table_lower:
            if 'monthly' in table_lower:
                return 'monthly_inflation'
            elif 'yearly' in table_lower:
                return 'annual_inflation'
            elif 'year-on-year' in table_lower:
                return 'year_on_year_inflation'
            elif 'provincial' in table_lower:
                return 'provincial_inflation'
            else:
                return 'inflation_rate'
        elif 'weighted' in table_lower:
            if 'index' in table_lower:
                return 'cpi_weighted_index'
            elif 'inflation' in table_lower:
                return 'cpi_weighted_inflation'
            elif 'annual' in table_lower:
                return 'cpi_weighted_annual'
            else:
                return 'cpi_weighted'
        elif 'provincial cpi' in table_lower:
            return 'provincial_cpi_index'
        else:
            return 'unknown_price_variable'
    
    def map_accounts_data(self, df, table_name):
        """Map national accounts data to fact_national_accounts table"""
        mapped_data = []
        
        for _, row in df.iterrows():
            mapped_row = {
                'date_id': self.get_date_id(year=self.extract_year(row)),
                'variable_name': self.determine_accounts_variable(table_name, row),
                'value': self.extract_value(row)
            }
            
            # Map province if available
            if 'province' in row and pd.notna(row['province']):
                mapped_row['province_id'] = self.get_geo_id(row['province'])
            
            # Map industry if available
            if 'industry' in row and pd.notna(row['industry']):
                mapped_row['industry_id'] = self.get_industry_id(row['industry'])
            
            # Include rows with actual data
            if mapped_row.get('value') not in [None, 0, '']:
                mapped_data.append(mapped_row)
        
        return mapped_data
    
    def determine_accounts_variable(self, table_name, row):
        """Determine national accounts variable"""
        table_lower = table_name.lower()
        
        # Map based on table name patterns
        if 'annual usd earnings' in table_lower:
            return 'annual_usd_earnings'
        elif 'current prices gdp shares' in table_lower:
            return 'gdp_shares_current_prices'
        elif 'provincial gdp at constant prices' in table_lower:
            if '2023' in table_lower:
                return 'provincial_gdp_constant_2023'
            elif '2021' in table_lower and 'zwl' in table_lower:
                return 'provincial_gdp_constant_zwl_2021'
            elif '2022' in table_lower and 'zwl' in table_lower:
                return 'provincial_gdp_constant_zwl_2022'
            elif '2023' in table_lower and 'zwl' in table_lower:
                return 'provincial_gdp_constant_zwl_2023'
            else:
                return 'provincial_gdp_constant'
        elif 'earnings' in table_lower and 'industrial sector' in table_lower:
            return 'earnings_by_industrial_sector'
        elif 'gdp' in table_lower and 'constant prices' in table_lower:
            return 'gdp_constant'
        elif 'gdp' in table_lower and 'shares' in table_lower:
            return 'gdp_shares'
        elif 'provincial gdp' in table_lower:
            return 'provincial_gdp'
        else:
            # If 'variable' column exists, use it directly
            if 'variable' in row and pd.notna(row['variable']):
                return str(row['variable']).lower().replace(' ', '_')
            return 'unknown_accounts_variable'
    
    def map_trade_data(self, df, table_name):
        """Map trade data to fact_trade table"""
        mapped_data = []
        
        for _, row in df.iterrows():
            mapped_row = {
                'date_id': self.get_date_id(year=self.extract_year(row)),
                'variable_name': self.determine_trade_variable(table_name, row),
                'value': self.extract_value(row)
            }
            
            # Map trade group if available
            if 'comesa' in table_name.lower():
                mapped_row['trade_group_id'] = 1
            elif 'eccas' in table_name.lower():
                mapped_row['trade_group_id'] = 2
            elif 'eu' in table_name.lower():
                mapped_row['trade_group_id'] = 3
            elif 'afcfta' in table_name.lower():
                mapped_row['trade_group_id'] = 4
            
            # Include rows with actual data
            if mapped_row.get('value') not in [None, 0, '']:
                mapped_data.append(mapped_row)
        
        return mapped_data
    
    def determine_trade_variable(self, table_name, row):
        """Determine trade variable"""
        # If 'variable' column exists, use it directly
        if 'variable' in row and pd.notna(row['variable']):
            return str(row['variable']).lower().replace(' ', '_')
        
        # Fallback to table name
        table_lower = table_name.lower()
        
        # Map based on table name patterns
        if 'comesa exports' in table_lower:
            if 'net weight' in table_lower:
                return 'comesa_exports_net_weight'
            elif 'share' in table_lower:
                return 'comesa_exports_share'
            elif 'value' in table_lower:
                return 'comesa_exports_value'
            else:
                return 'comesa_exports'
        elif 'eccas exports' in table_lower:
            if 'net weight' in table_lower:
                return 'eccas_exports_net_weight'
            elif 'share' in table_lower:
                return 'eccas_exports_share'
            elif 'value' in table_lower:
                return 'eccas_exports_value'
            else:
                return 'eccas_exports'
        elif 'exports bec' in table_lower:
            return 'exports_bec'
        elif 'imports country share' in table_lower:
            return 'imports_country_share'
        elif 'imports from eu net weight' in table_lower:
            return 'imports_from_eu_net_weight'
        elif 'imports country value' in table_lower:
            return 'imports_country_value'
        elif 'imports from eu share' in table_lower:
            return 'imports_from_eu_share'
        elif 'imports from eu value' in table_lower:
            return 'imports_from_eu_value'
        elif 'trade export share by country' in table_lower:
            return 'export_share_by_country'
        elif 'trade export value by country' in table_lower:
            return 'export_value_by_country'
        elif 'trade exports by net weight to afcfta' in table_lower:
            return 'exports_net_weight_afcfta'
        elif 'trade summary' in table_lower:
            return 'trade_summary'
        elif 'exports' in table_lower:
            if 'net weight' in table_lower:
                return 'exports_net_weight'
            elif 'share' in table_lower:
                return 'exports_share'
            elif 'value' in table_lower:
                return 'exports_value'
            else:
                return 'exports'
        elif 'imports' in table_lower:
            if 'net weight' in table_lower:
                return 'imports_net_weight'
            elif 'share' in table_lower:
                return 'imports_share'
            elif 'value' in table_lower:
                return 'imports_value'
            else:
                return 'imports'
        elif 'trade_balance' in table_lower:
            return 'trade_balance'
        else:
            if 'variable' in row and pd.notna(row['variable']):
                return str(row['variable']).lower().replace(' ', '_')
            return 'unknown_trade_variable'
    
    def extract_year(self, row):
        """Extract year from row data"""
        if 'year' in row and pd.notna(row['year']):
            return int(str(row['year']).replace(',', ''))
        elif 'period' in row and pd.notna(row['period']):
            # Try to extract year from period
            period_str = str(row['period'])
            year_match = re.search(r'20\d{2}', period_str)
            if year_match:
                return int(year_match.group())
        return 2025  # Default
    
    def extract_value(self, row):
        """Extract numeric value from row"""
        if 'value' in row and pd.notna(row['value']):
            try:
                return float(str(row['value']).replace(',', ''))
            except:
                return 0.0
        return 0.0
    
    def save_to_fact_table(self, data, table_name):
        """Save mapped data to fact table using SQLAlchemy"""
        print(f"DEBUG: Saving {len(data)} rows to {table_name}")
        
        if table_name == 'fact_labour':
            for row in data:
                fact = FactLabour(
                    date_id=row.get('date_id'),
                    industry_id=row.get('industry_id'),
                    occupation_id=row.get('occupation_id'),
                    sex_id=row.get('sex_id'),
                    age_group_id=row.get('age_group_id'),
                    province_id=row.get('province_id'),
                    variable_name=row.get('variable_name'),
                    value=row.get('value')
                )
                db.session.add(fact)
        
        elif table_name == 'fact_prices':
            for row in data:
                fact = FactPrices(
                    date_id=row.get('date_id'),
                    province_id=row.get('province_id'),
                    currency_id=row.get('currency_id'),
                    variable_name=row.get('variable_name'),
                    value=row.get('value')
                )
                db.session.add(fact)
        
        elif table_name == 'fact_national_accounts':
            for row in data:
                fact = FactNationalAccounts(
                    date_id=row.get('date_id'),
                    province_id=row.get('province_id'),
                    industry_id=row.get('industry_id'),
                    variable_name=row.get('variable_name'),
                    value=row.get('value')
                )
                db.session.add(fact)
        
        elif table_name == 'fact_trade':
            for row in data:
                fact = FactTrade(
                    date_id=row.get('date_id'),
                    country_id=row.get('country_id'),
                    trade_group_id=row.get('trade_group_id'),
                    variable_name=row.get('variable_name'),
                    value=row.get('value')
                )
                db.session.add(fact)
        
        db.session.commit()
        print(f"DEBUG: Successfully saved {len(data)} rows to {table_name}")
        return len(data)
    
    def process_upload(self, df, table_name, domain):
        """Main processing function for uploaded data"""
        # Clean table name
        clean_table_name = table_name.strip().lower()
        
        print(f"DEBUG: Processing upload - Domain: {domain}, Table: {clean_table_name}")
        print(f"DEBUG: DataFrame shape: {df.shape}")
        print(f"DEBUG: DataFrame columns: {list(df.columns)}")
        print(f"DEBUG: First few rows:\n{df.head(3)}")
        
        # Map based on domain
        if domain == 'labour':
            mapped_data = self.map_labour_data(df, table_name)
            result = self.save_to_fact_table(mapped_data, 'fact_labour')
        elif domain == 'prices':
            mapped_data = self.map_prices_data(df, table_name)
            result = self.save_to_fact_table(mapped_data, 'fact_prices')
        elif domain == 'accounts':
            mapped_data = self.map_accounts_data(df, table_name)
            result = self.save_to_fact_table(mapped_data, 'fact_national_accounts')
        elif domain == 'trade':
            mapped_data = self.map_trade_data(df, table_name)
            result = self.save_to_fact_table(mapped_data, 'fact_trade')
        elif domain == 'bop & finance':
            mapped_data = self.map_trade_data(df, table_name)  # BOP & Finance uses trade structure
            result = self.save_to_fact_table(mapped_data, 'fact_trade')
        else:
            print(f"DEBUG: Unknown domain: {domain}")
            result = 0
        
        print(f"DEBUG: Mapped {len(mapped_data)} rows, Result: {result}")
        return result
