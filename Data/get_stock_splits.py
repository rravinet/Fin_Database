import sqlalchemy
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, StockSplits
from log_config import setup_logging
from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import logging
import asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

# Setup logging
setup_logging()

# Load environment variables
load_dotenv()

class StockSplitsupdate:
    "Class to update stock split data from Polygon.io"

    def __init__(self, tickers, engine, key, limit=1000):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.key = key
        self.limit = limit
        
    async def transform_data(self, df, ticker):
        df['ticker'] = ticker
        df['execution_date'] = pd.to_datetime(df['execution_date'], errors='coerce').dt.tz_localize(None)
        return df.to_dict(orient='records')
    
    async def fetch_data(self, ticker):
        url = f"https://api.polygon.io/v3/reference/splits?ticker={ticker}"
        params = {"limit": self.limit, "apiKey": self.key}
        async with httpx.AsyncClient() as async_client:
            response = await async_client.get(url, params=params)
            response.raise_for_status()
            return response.json().get('results', [])
     
    async def update_data(self):
        all_data = []
        logging.info(f"Updating stock splits for {self.tickers}")

        async with self.engine.connect() as conn:
            tasks = [self.fetch_data(ticker) for ticker in self.tickers]
            responses = await asyncio.gather(*tasks)
            
            for response, ticker in zip(responses, self.tickers):
                if response:
                    splits_df = pd.DataFrame(response)
                    if not splits_df.empty:
                        transformed_data = await self.transform_data(splits_df, ticker)
                        all_data.extend(transformed_data)
                    else:
                        logging.info(f"No data for {ticker}")

            if all_data:
                try:
                    table = StockSplits.__table__
                    stmt = insert(table).values(all_data)
                    update_dict = {c.name: c for c in stmt.excluded if c.name not in ['ticker', 'execution_date']}
                    stmt = stmt.on_conflict_do_update(index_elements=['ticker', 'execution_date'], set_=update_dict)
                    await conn.execute(stmt)
                    await conn.commit()
                    logging.info("Data insert completed successfully.")
                except Exception as e:
                    logging.error(f"Error updating stock splits data: {e}")
                    await conn.rollback()


# if __name__ == '__main__':
#     tickers = ['AAPL', 'MSFT']  
#     key = os.getenv("API_KEY")

#     updater = StockSplitsupdate(tickers, engine, key)
#     asyncio.run(updater.update_data())
