import sqlalchemy
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, CompanyFinancials
from sqlalchemy import select
from sqlalchemy.sql import func
import logging
from sqlalchemy.dialects.postgresql import JSONB, insert
from log_config import setup_logging
import json
import asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession


setup_logging()

load_dotenv()

class CompanyFinancialsupdater:
    def __init__(self, tickers, engine, key):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.key = key
    
    async def transform_data(self, df):
        logging.info(f"Transforming data for {len(df)} records")

        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
        df['filing_date'] = pd.to_datetime(df['filing_date'], errors='coerce')
        df['acceptance_datetime'] = pd.to_datetime(df['acceptance_datetime'], format='%Y%m%d%H%M%S', errors='coerce')

        # Replacing NaT values with None
        for col in ['start_date', 'end_date', 'filing_date', 'acceptance_datetime']:
            df[col] = df[col].replace({pd.NaT: None})


        # Ensuring tickers is a comma-separated string
        df['tickers'] = df['tickers'].apply(lambda x: ','.join(x) if isinstance(x, list) else x)

        # Ensuring sic is converted to integer if present
        if 'sic' in df.columns:
            df['sic'] = pd.to_numeric(df['sic'], errors='coerce').astype('Int64', errors='ignore')

        # Drop unnecessary columns
        columns_to_drop = ['cik', 'source_filing_file_url', 'source_filing_url']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

        logging.info(f"Data after dropping unnecessary columns: {df.head()}")

        # Convert financials to JSON string
        if 'financials' in df.columns:
            df['financials'] = df['financials'].apply(json.dumps)

        df = df.drop_duplicates(subset=['start_date', 'tickers'], keep='last')

        df = df.dropna(subset=['start_date'])
        
        transformed_df = df.to_dict(orient='records')

        
        return transformed_df

    async def fetch_data(self, ticker):
        url = f"https://api.polygon.io/vX/reference/financials?ticker={ticker}"
        params = {"apiKey": self.key}
        async with httpx.AsyncClient() as async_client:
            response = await async_client.get(url, params=params)
            data = response.json().get('results', [])
            logging.info(f"Fetched {len(data)} records for {ticker}")
            return data

    async def update_data(self):
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
                logging.debug(f'Inserting data into the database for {len(all_data)} records.')
                try:
                    table = CompanyFinancials.__table__
                    batch_size = 1000
                    for i in range(0, len(all_data), batch_size):
                        batch_data = all_data[i:i + batch_size]
                        stmt = insert(table).values(batch_data)
                        update_dict = {c.name: c for c in stmt.excluded if c.name not in ['tickers', 'start_date']}
                        await conn.execute(stmt)
                    await conn.commit()
                    logging.info(f"Data successfully updated for {len(all_data)} records")
                except Exception as e:
                    logging.error(f"Error updating company data: {e}")
                    await conn.rollback()
            else:
                logging.info("No data to update.")

# if __name__ == '__main__':
#     tickers = ['AAPL', 'MSFT'] 
#     key = os.getenv("API_KEY")

#     company_financials_updater = CompanyFinancialsupdater(tickers, engine, key)

#     async def main():
#         await company_financials_updater.update_data()

#     asyncio.run(main())