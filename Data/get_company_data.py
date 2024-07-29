
import sqlalchemy
import pandas as pd
import numpy as np
from polygon import RESTClient
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, Company_Financials
from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import logging
from sqlalchemy.dialects.postgresql import JSONB
from log_config import setup_logging
import pytz
import json

class Company_Financials_updater:
    def __init__(self, tickers, engine):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
    
    def transform_data(self, df):
        # Convert to datetime
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])
        df['filing_date'] = pd.to_datetime(df['filing_date'])

        # Replace NaT values with None
        df['start_date'] = df['start_date'].replace({pd.NaT: None})
        df['end_date'] = df['end_date'].replace({pd.NaT: None})
        df['filing_date'] = df['filing_date'].replace({pd.NaT: None})
        
        # Drop unnecessary columns
        df.drop(columns=['cik', 'source_filing_file_url', 'source_filing_url'], inplace=True)

        # Convert financials to JSON string
        df['financials'] = df['financials'].apply(json.dumps)
        
        # Convert DataFrame to list of dictionaries
        transformed = df.to_dict(orient='records')
        
        return transformed
    
    def need_for_update(self):
        pass
        
    def update_data(self,client):
        with self.engine.connect() as conn:
            all_data = []
            logging.info(f"Updating company financials for {self.tickers}")
            for ticker in self.tickers:
                resp = client.vx.list_stock_financials(ticker)
                ticker_df = pd.DataFrame(resp)
                
                if not ticker_df.empty:
                    transformed_data = self.transform_data(ticker_df)
                    all_data.extend(transformed_data)
                else:
                    print(f"No data for {ticker}")
                
                    
                 
            if all_data:
                
                try:
                    conn.execute(Company_Financials.__table__.insert(), all_data)
                    conn.commit()
                    logging.info("Data insert completed successfully.")

                except Exception as e:
                        logging.error(f"Error updating company data for {ticker}: {e}")
                        



