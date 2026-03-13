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
                'date_id': self.get_date_id(year=2025, quarter=2),  # Default to 2025 Q2
                'variable_name': self.determine_labour_variable(table_name, row),
                'value': self.extract_value(row)
            }
            
            # Map dimensions based on available columns
            if 'province' in row and pd.notna(row['province']):
                mapped_row['province_id'] = self.get_geo_id(row['province'])
            
            if 'industry' in row and pd.notna(row['industry']):
                mapped_row['industry_id'] = self.get_industry_id(row['industry'])
            
            if 'sex' in row and pd.notna(row['sex']):
                mapped_row['sex_id'] = self.get_demo_id(sex=row['sex'])
            
            if 'occupation' in row and pd.notna(row['occupation']):
                mapped_row['occupation_id'] = self.get_demo_id(occupation=row['occupation'])
            
            if 'age_group' in row and pd.notna(row['age_group']):
                mapped_row['age_group_id'] = self.get_demo_id(age_group=row['age_group'])
            
            # Only include rows with actual data (not all None)
            has_valid_data = any([
                mapped_row.get('province_id'),
                mapped_row.get('industry_id'),
                mapped_row.get('sex_id'),
                mapped_row.get('occupation_id'),
                mapped_row.get('age_group_id')
            ])
            
            # Only include rows with valid mappings OR valid data
            if has_valid_data and mapped_row.get('value') not in [None, 0, '']:
                mapped_data.append(mapped_row)
        
        return mapped_data
    
    def determine_labour_variable(self, table_name, row):
        """Determine variable_name based on table name and row content"""
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
        elif 'employed population' in table_lower and 'informal' not in table_lower:
            return 'employed_population'
        elif 'informal employment' in table_lower:
            return 'informal_employment'
        elif 'actual hours worked' in table_lower:
            return 'actual_hours_worked'
        elif 'employment by area' in table_lower:
            return 'employment_by_area'
        elif 'labour migrants' in table_lower:
            return 'labour_migrants'
        elif 'time related under employment' in table_lower:
            return 'time_related_underemployment'
        elif 'youth not in education' in table_lower:
            return 'youth_neet_population'
        elif 'annual average' in table_lower or 'quarterly average' in table_lower:
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
            mapped_row = {
                'date_id': self.get_date_id(year=2025),  # Default year
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
        table_lower = table_name.lower()
        
        if 'cpi index' in table_lower:
            return 'cpi_index'
        elif 'inflation' in table_lower and 'monthly' in table_lower:
            return 'monthly_inflation'
        elif 'inflation' in table_lower and 'year' in table_lower:
            return 'annual_inflation'
        elif 'weighted' in table_lower and 'index' in table_lower:
            return 'cpi_weighted_index'
        elif 'weighted' in table_lower and 'inflation' in table_lower:
            return 'cpi_weighted_inflation'
        elif 'provincial cpi' in table_lower:
            return 'provincial_cpi_index'
        else:
            if 'variable' in row and pd.notna(row['variable']):
                return str(row['variable']).lower().replace(' ', '_')
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
        
        if 'earnings' in table_lower and 'industrial sector' in table_lower:
            return 'earnings_by_industrial_sector'
        elif 'gdp' in table_lower and 'constant prices' in table_lower:
            return 'gdp_constant'
        elif 'gdp' in table_lower and 'shares' in table_lower:
            return 'gdp_shares'
        elif 'provincial gdp' in table_lower:
            return 'provincial_gdp'
        else:
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
        table_lower = table_name.lower()
        
        if 'exports' in table_lower and 'weight' in table_lower:
            return 'exports_net_weight'
        elif 'exports' in table_lower and 'share' in table_lower:
            return 'exports_share'
        elif 'exports' in table_lower and 'value' in table_lower:
            return 'exports_value'
        elif 'imports' in table_lower and 'share' in table_lower:
            return 'imports_share'
        elif 'imports' in table_lower and 'value' in table_lower:
            return 'imports_value'
        elif 'trade summary' in table_lower:
            return 'trade_summary'
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
        """Save mapped data to appropriate fact table"""
        if not data:
            return 0
            
        cursor = self.conn.cursor()
        
        if table_name == 'fact_labour':
            for row in data:
                cursor.execute('''
                    INSERT INTO fact_labour (date_id, industry_id, occupation_id, sex_id, age_group_id, province_id, variable_name, value)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (row.get('date_id'), row.get('industry_id'), row.get('occupation_id'),
                      row.get('sex_id'), row.get('age_group_id'), row.get('province_id'),
                      row.get('variable_name'), row.get('value')))
        
        elif table_name == 'fact_prices':
            for row in data:
                cursor.execute('''
                    INSERT INTO fact_prices (date_id, province_id, currency_id, variable_name, value)
                    VALUES (?, ?, ?, ?, ?)
                ''', (row.get('date_id'), row.get('province_id'), row.get('currency_id'),
                      row.get('variable_name'), row.get('value')))
        
        elif table_name == 'fact_national_accounts':
            for row in data:
                cursor.execute('''
                    INSERT INTO fact_national_accounts (date_id, province_id, industry_id, variable_name, value)
                    VALUES (?, ?, ?, ?, ?)
                ''', (row.get('date_id'), row.get('province_id'), row.get('industry_id'),
                      row.get('variable_name'), row.get('value')))
        
        elif table_name == 'fact_trade':
            for row in data:
                cursor.execute('''
                    INSERT INTO fact_trade (date_id, country_id, trade_group_id, variable_name, value)
                    VALUES (?, ?, ?, ?, ?)
                ''', (row.get('date_id'), row.get('country_id'), row.get('trade_group_id'),
                      row.get('variable_name'), row.get('value')))
        
        self.conn.commit()
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
        else:
            print(f"DEBUG: Unknown domain: {domain}")
            result = 0
        
        print(f"DEBUG: Mapped {len(mapped_data)} rows, Result: {result}")
        return result
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
