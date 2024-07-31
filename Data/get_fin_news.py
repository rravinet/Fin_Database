
import sqlalchemy
import pandas as pd
import numpy as np
from polygon import RESTClient
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, StockNews
from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func
import logging
from log_config import setup_logging
import pytz
import asyncio
import httpx
from sqlalchemy.ext.asyncio import AsyncSession


class NewsUpdate:
    "Class to update stock news data from Polygon.io"

    def __init__(self, tickers, engine, key, limit=1000):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.key = key
        self.limit = limit
        
    async def transformed_data(self, ticker_df, ticker):
        ticker_df['published_utc'] = pd.to_datetime(ticker_df['published_utc'])
        ticker_df['ticker_queried'] = ticker
        ticker_df.rename(columns={"id": "id_polygon"}, inplace=True)
        ticker_df.drop(columns=['amp_url', 'image_url', 'publisher'], inplace=True)
        return ticker_df.to_dict(orient='records')
    
    async def fetch_data(self, ticker, last_date):
        params = {"limit": self.limit, "apiKey": self.key}
        if last_date:
            params["published_utc.gt"] = last_date.isoformat()

        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}"
        async with httpx.AsyncClient() as async_client:
            response = await async_client.get(url, params=params)
            return response.json().get('results', [])

    async def update_data(self, client):
        all_data = []
        logging.info(f"Updating stock news for {self.tickers}")

        async with self.engine.connect() as conn:
            tasks = []
            for ticker in self.tickers:
                try:
                    query = select(func.max(StockNews.published_utc)).where(StockNews.ticker_queried == ticker)
                    result = await conn.execute(query)
                    last_date = result.scalar()
                    tasks.append(self.fetch_data(ticker, last_date))
                except Exception as e:
                    logging.error(f"Error preparing data for ticker {ticker}: {e}")

            responses = await asyncio.gather(*tasks)

            for response, ticker in zip(responses, self.tickers):
                if response:
                    ticker_df = pd.DataFrame(response)
                    if not ticker_df.empty:
                        transformed_data = await self.transformed_data(ticker_df, ticker)
                        all_data.extend(transformed_data)
                    else:
                        logging.info(f"No new data for {ticker}")

            if all_data:
                try:
                    table = StockNews.__table__
                    # Calculate the maximum number of rows per batch
                    num_columns = len(all_data[0])  # Get the number of columns from the first record
                    max_batch_size = 32767 // num_columns  # Ensure we don't exceed the argument limit

                    for i in range(0, len(all_data), max_batch_size):
                        batch_data = all_data[i:i + max_batch_size]
                        stmt = insert(table).values(batch_data)
                        update_dict = {c.name: c for c in stmt.excluded if c.name not in ['published_utc', 'ticker_queried']}
                        stmt = stmt.on_conflict_do_update(index_elements=['published_utc', 'ticker_queried'], set_=update_dict)
                        await conn.execute(stmt)
                    await conn.commit()
                    logging.info("Data insert completed successfully.")
                except Exception as e:
                    logging.error(f"Error bulk updating stock news: {e}")
                    await conn.rollback()
            else:
                logging.info("No data to update.")

    
