
import sqlalchemy
import pandas as pd
import numpy as np
from polygon import RESTClient
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, Stock_News
from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import logging
from log_config import setup_logging
import pytz


class News_Update:
    " Class to update stock news data from Polygon.io"

    def __init__(self, tickers, engine, limit = 1000):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.limit = limit
        
    def transformed_data(self, ticker_df, ticker):
        ticker_df['published_utc'] = pd.to_datetime(ticker_df['published_utc'])
        ticker_df['ticker_queried'] = ticker
        ticker_df.rename(columns={"id": "id_polygon"}, inplace=True)
        ticker_df.drop(columns=['amp_url', 'image_url', 'publisher'], inplace=True)
        return ticker_df.to_dict(orient='records')
    
    def update_data(self, client):
        all_data = []
        logging.info(f"Updating stock news for {self.tickers}")
        
        with self.engine.begin() as conn:
            for ticker in self.tickers:
                try:
                    query = select(func.max(Stock_News.published_utc)).where(Stock_News.ticker_queried == ticker)
                    last_date = conn.execute(query).scalar()                
                    
                    if last_date is not None:
                        resp = client.list_ticker_news(ticker, published_utc_gt=last_date, limit=self.limit)
                    else:
                        resp = client.list_ticker_news(ticker, limit=self.limit)                    
                    
                
                    ticker_df = pd.DataFrame(resp)
                    
                    if not ticker_df.empty:
                        transformed_data = self.transformed_data(ticker_df, ticker)
                        all_data.extend(transformed_data)
                    else:
                        logging.info(f"No new data for {ticker}")
                except Exception as e:
                    logging.error(f"Error updating stock news for {ticker}: {e}")
                    
            
            if all_data:
                try:
                    conn.execute(Stock_News.__table__.insert(),all_data)
                except Exception as e:
                    logging.error(f"Error bulk updating stock news for {ticker}: {e}")
                        
                        
            

