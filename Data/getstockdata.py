# %%

import sqlalchemy
import pandas as pd
import numpy as np
from polygon import RESTClient
from datetime import datetime, timedelta
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, DailyStockData, HourlyStockData, MinuteStockData
from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import logging
from log_config import setup_logging
import pytz
from sqlalchemy.dialects.postgresql import insert
import asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession
from concurrent.futures import ThreadPoolExecutor


# %%
setup_logging()

# %%
load_dotenv()

# %%
# key = os.getenv("API_KEY")
# client = RESTClient(api_key=key)


class MarketDataUpdater:
    " Class to update stock data from Polygon.io"
    
    def __init__(self, tickers, engine,key, start_date='2005-01-01', end_date=dt.date.today(), multiplier=1, timespan='day', limit=50000):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.key = key
        self.start_date = start_date
        self.end_date = end_date
        self.multiplier = multiplier
        self.timespan = timespan
        self.limit = limit
        logging.info(f"Initialized Market_Data_Updater with {len(self.tickers)} tickers.")

    async def transform_data(self, df, ticker):    
        logging.info(f"Transforming data for ticker {ticker}")
        df['date'] = pd.to_datetime(df['t'], unit='ms', utc=True).dt.tz_convert('US/Eastern').dt.tz_localize(None)
        df = df.drop(columns=['otc']) if 'otc' in df.columns else df
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

    def get_table_name(self):
        table_map = {'day': DailyStockData,
                     'hour': HourlyStockData,
                     'minute': MinuteStockData}
        if self.timespan not in table_map:
            logging.error(f'Invalid timespan {self.timespan}. Valid timespans are day, hour, minute.')
            raise ValueError(f"Timespan {self.timespan} is not valid.")
        return table_map[self.timespan]

    def calculate_next_start_date(self, last_date, timespan):
        logging.info(f"Calculating next start date for {timespan} data.")
        eastern = pytz.timezone('US/Eastern')
        current_time = datetime.now().astimezone(eastern)
        market_close = current_time.replace(hour=20, minute=0, second=0, microsecond=0)

        if timespan == 'day':
            next_valid_time = last_date if current_time < market_close else last_date + timedelta(days=1)
        elif timespan == 'hour':
            if current_time.minute > 0:
                next_valid_time = current_time.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
            else:
                next_valid_time = current_time.replace(minute=0, second=0, microsecond=0)
        elif timespan == 'minute':
            if current_time.second > 0:
                next_valid_time = current_time.replace(second=0, microsecond=0) - timedelta(minutes=1)
            else:
                next_valid_time = current_time.replace(second=0, microsecond=0)

        logging.info(f"Next valid start date for {timespan} data is {next_valid_time}")
        return next_valid_time

    async def fetch_data(self, ticker, start_date):
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{self.multiplier}/{self.timespan}/{start_date}/{self.end_date}"
        params = {"limit": self.limit, "apiKey": self.key}
        async with httpx.AsyncClient() as async_client:
            response = await async_client.get(url, params=params)
            return response.json().get('results', [])

    async def update_data(self, client):
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
                        start_date = self.calculate_next_start_date(last_date, self.timespan)
                        if start_date <= last_date:
                            logging.info(f"No new data available for {ticker}.")
                            continue
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
                        transformed_data = await self.transform_data(ticker_df, ticker)
                        all_data.extend(transformed_data)
                    else:
                        logging.info(f"No new data available for ticker {ticker}.")

            if all_data:
                try:
                    table = StockDataClass.__table__
                    batch_size = 1000  # Adjust the batch size to avoid exceeding the limit
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


