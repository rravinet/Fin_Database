import sqlalchemy
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, DailyStockData, HourlyStockData, MinuteStockData
from sqlalchemy import select
from sqlalchemy.sql import func
import logging
from log_config import setup_logging
import pytz
from sqlalchemy.dialects.postgresql import insert
import asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession

# Setup logging
setup_logging()

# Load environment variables
load_dotenv()


class MarketDataUpdater:
    def __init__(self, tickers, engine, key, start_date='2005-01-01', end_date=dt.date.today(), multiplier=1, timespan='day', limit=50000):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.key = key
        self.start_date = start_date
        self.end_date = end_date
        self.multiplier = multiplier
        self.timespan = timespan
        self.limit = limit
        logging.info(f"Initialized MarketDataUpdater with {len(self.tickers)} tickers.")

    async def fetch_data(self, ticker, start_date):
        all_results = []
        current_start_date = start_date
        async with httpx.AsyncClient() as async_client:
            while True:
                url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{self.multiplier}/{self.timespan}/{current_start_date}/{self.end_date}"
                params = {"limit": self.limit, "apiKey": self.key}
                try:
                    response = await async_client.get(url, params=params)
                    response.raise_for_status()
                    data = response.json()
                    results = data.get('results', [])
                    
                    if not results:
                        break

                    all_results.extend(results)

                    if len(results) < self.limit:
                        break

                    last_result_date = datetime.utcfromtimestamp(results[-1]['t'] / 1000)
                    current_start_date = (last_result_date + timedelta(minutes=1)).strftime('%Y-%m-%d')

                except httpx.HTTPStatusError as e:
                    logging.error(f"HTTP error occurred: {e}")
                    break
                except Exception as e:
                    logging.error(f"An error occurred: {e}")
                    break

        return all_results

    def get_table_name(self):
        table_map = {'day': DailyStockData, 'hour': HourlyStockData, 'minute': MinuteStockData}
        if self.timespan not in table_map:
            logging.error(f'Invalid timespan {self.timespan}. Valid timespans are day, hour, minute.')
            raise ValueError(f"Timespan {self.timespan} is not valid.")
        return table_map[self.timespan]

    async def update_data(self):
        all_data = []
        logging.debug(f'Starting data update process for {len(self.tickers)} tickers.')

        StockDataClass = self.get_table_name()

        async with self.engine.connect() as conn:
            tasks = []
            for ticker in self.tickers:
                try:
                    logging.debug(f"Processing ticker {ticker}.")
                    query = select(func.max(StockDataClass.date)).where(StockDataClass.ticker == ticker)
                    result = await conn.execute(query)
                    last_date = result.scalar()

                    if last_date is not None:
                        eastern = pytz.timezone('US/Eastern')
                        if last_date.tzinfo is None:
                            last_date = eastern.localize(last_date)

                        start_date = (last_date + timedelta(minutes=1)).strftime('%Y-%m-%d')
                    else:
                        start_date = self.start_date

                    tasks.append(self.fetch_data(ticker, start_date))
                except Exception as e:
                    logging.error(f"Error preparing data for ticker {ticker}: {e}")

            responses = await asyncio.gather(*tasks)

            for response, ticker in zip(responses, self.tickers):
                if response:
                    ticker_df = pd.DataFrame(response)
                    if not ticker_df.empty:
                        ticker_df = ticker_df.drop_duplicates(subset=['t'], keep='last')
                        transformed_data = await self.transform_data(ticker_df, ticker)
                        all_data.extend(transformed_data)
                    else:
                        logging.info(f"No new data available for ticker {ticker}.")

            if all_data:
                logging.debug(f'Inserting data into the database for {len(all_data)} records.')
                try:
                    table = StockDataClass.__table__
                    batch_size = 1000
                    for i in range(0, len(all_data), batch_size):
                        batch_data = all_data[i:i + batch_size]
                        stmt = insert(table).values(batch_data)
                        update_dict = {c.name: c for c in stmt.excluded if c.name not in ['date', 'ticker']}
                        stmt = stmt.on_conflict_do_update(index_elements=['date', 'ticker'], set_=update_dict)
                        await conn.execute(stmt)
                    await conn.commit()
                    logging.info(f'Data successfully updated for {len(all_data)} records')
                except Exception as e:
                    logging.error(f"Error updating stock data in bulk: {e}")
                    await conn.rollback()
            else:
                logging.info("No data to update.")



    async def transform_data(self, df, ticker):
        logging.info(f"Transforming data for ticker {ticker}")
        df['date'] = pd.to_datetime(df['t'], unit='ms', utc=True).dt.tz_convert('US/Eastern').dt.tz_localize(None)
        if 'otc' in df.columns:
            df = df.drop(columns=['otc'])
        df['ticker'] = ticker
        df.rename(columns={
            't': 'timestamp',
            'v': 'volume',
            'vw': 'vwap',
            'o': 'open',
            'c': 'close',
            'h': 'high',
            'l': 'low',
            'n': 'transactions'
        }, inplace=True)
        transformed_data = df.to_dict(orient='records')
        logging.info(f'Data transformed for ticker {ticker}')
        return transformed_data

# if __name__ == '__main__':
#     tickers = ['AAPL', 'MSFT']  
#     key = os.getenv("API_KEY")

#     updater = MarketDataUpdater(tickers, engine, key, start_date='2020-01-01', timespan='minute')
#     asyncio.run(updater.update_data())


