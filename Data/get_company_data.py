
import sqlalchemy
import pandas as pd
import numpy as np
from polygon import RESTClient
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, CompanyFinancials
from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import logging
from sqlalchemy.dialects.postgresql import JSONB, insert
from log_config import setup_logging
import pytz
import json
import asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession


class CompanyFinancialsupdater:
    def __init__(self, tickers, engine, key):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.key = key
    
    async def transform_data(self, df):
        # Convert to datetime
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])
        df['filing_date'] = pd.to_datetime(df['filing_date'])
        df['acceptance_datetime'] = pd.to_datetime(df['acceptance_datetime'])

        # Replace NaT values with None
        df['start_date'] = df['start_date'].replace({pd.NaT: None})
        df['end_date'] = df['end_date'].replace({pd.NaT: None})
        df['filing_date'] = df['filing_date'].replace({pd.NaT: None})
        df['acceptance_datetime'] = df['acceptance_datetime'].replace({pd.NaT: None})

       
        df['tickers'] = df['tickers'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)

        # Drop unnecessary columns
        df.drop(columns=['cik', 'source_filing_file_url', 'source_filing_url'], inplace=True)

        # Convert financials to JSON string
        df['financials'] = df['financials'].apply(json.dumps)
        
        # Convert DataFrame to list of dictionaries
        transformed_df = df.to_dict(orient='records')
        
        return transformed_df
    
    def need_for_update(self):
        pass
    
    async def fetch_data(self, ticker):
        url = f"https://api.polygon.io/vX/reference/financials?ticker={ticker}"
        params = {"apiKey": self.key}
        async with httpx.AsyncClient() as async_client:
            response = await async_client.get(url, params=params)
            return response.json().get('results', [])

      
  
    async def update_data(self, client):
        all_data = []
        logging.info(f"Updating company financials for {self.tickers}")

        async with self.engine.connect() as conn:
            tasks = []
            for ticker in self.tickers:
                tasks.append(self.fetch_data(ticker))
            
            responses = await asyncio.gather(*tasks)
            
            for response, ticker in zip(responses, self.tickers):
                if response:
                    ticker_df = pd.DataFrame(response)
                    if not ticker_df.empty:
                        transformed_data = await self.transform_data(ticker_df)
                        all_data.extend(transformed_data)
                    else:
                        logging.info(f"No data for {ticker}")

            if all_data:
                try:
                    table = CompanyFinancials.__table__
                    stmt = insert(table).values(all_data)
                    update_dict = {c.name: c for c in stmt.excluded if c.name not in ['tickers', 'start_date']}
                    stmt = stmt.on_conflict_do_update(index_elements=['tickers', 'start_date'], set_=update_dict)
                    await conn.execute(stmt)
                    await conn.commit()
                    logging.info("Data insert completed successfully.")
                except Exception as e:
                    logging.error(f"Error updating company data: {e}")
                    await conn.rollback()

      
      
        
    # def update_data(self,client):
    #     with self.engine.connect() as conn:
    #         all_data = []
    #         logging.info(f"Updating company financials for {self.tickers}")
    #         for ticker in self.tickers:
    #             resp = client.vx.list_stock_financials(ticker)
    #             ticker_df = pd.DataFrame(resp)
                
    #             if not ticker_df.empty:
    #                 transformed_data = self.transform_data(ticker_df)
    #                 all_data.extend(transformed_data)
    #             else:
    #                 print(f"No data for {ticker}")
                
                    
                 
    #         if all_data:
                
    #             try:
    #                 conn.execute(Company_Financials.__table__.insert(), all_data)
    #                 conn.commit()
    #                 logging.info("Data insert completed successfully.")

    #             except Exception as e:
    #                     logging.error(f"Error updating company data for {ticker}: {e}")
                        



