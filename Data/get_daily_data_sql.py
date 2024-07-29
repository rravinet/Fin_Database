
import sqlalchemy
import pandas as pd
import numpy as np
from polygon import RESTClient
from datetime import datetime, timedelta
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, Daily_Stock_Data, Hourly_Stock_Data, Minute_Stock_Data
from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import logging
from log_config import setup_logging
import pytz

class Market_Data_updater:
    " Class to update stock data from Polygon.io"
    
    def __init__(self, tickers, engine, start_date = '2005-01-01', end_date = dt.date.today(),multiplier = 1, timespan = 'day',limit = 50000):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.start_date = start_date
        self.end_date = end_date
        self.multiplier = multiplier
        self.timespan = timespan
        self.limit = limit
        logging.info(f"Initialized Market_Data_updater with {len(self.tickers)} tickers.")

    
    
    def transform_data(self, df,ticker):
        logging.info(f"Transforming data for ticker {ticker}")
        df['date'] = pd.to_datetime(df['timestamp'], unit = 'ms',utc=True).dt.tz_convert('US/Eastern').dt.tz_localize(None)
        df = df.drop(columns = ['otc'])
        df['ticker'] = ticker
        transformed_data = df.to_dict(orient = 'records')
        logging.info(f'Data transformed for ticker {ticker}')
        return transformed_data
        
        
    def get_table_name(self):
        table_map = {'day': Daily_Stock_Data,
                     'hour': Hourly_Stock_Data,
                     'minute': Minute_Stock_Data
                     }
        if self.timespan not in table_map:
            logging.error(f'Invalid timespan {self.timespan}. Valid timespans are day, hour, minute.')
            raise ValueError(f"Timespan {self.timespan} is not valid.")
        return table_map[self.timespan]
    
    def calculate_next_start_date(self, last_date, timespan):
        logging.info(f"Calculating next start date for {timespan} data. Last date: {last_date}")
        eastern = pytz.timezone('US/Eastern')
        current_time = datetime.now().astimezone(eastern)
        market_close = current_time.replace(hour=20, minute=0, second=0, microsecond=0)

        if timespan == 'day':
            next_valid_time = last_date + timedelta(days=1)
            next_valid_time = next_valid_time.replace(hour=0, minute=0, second=0, microsecond=0)
        elif timespan == 'hour':
            next_valid_time = last_date + timedelta(hours=1)
            next_valid_time = next_valid_time.replace(minute=0, second=0, microsecond=0)
        elif timespan == 'minute':
            next_valid_time = last_date + timedelta(minutes=1)
            next_valid_time = next_valid_time.replace(second=0, microsecond=0)

        logging.info(f"Next valid start date for {timespan} data is {next_valid_time}")
        return next_valid_time


    
    


    
    def update_data(self, client):
        all_data = []
        logging.debug(f'Starting data update process for {len(self.tickers)} tickers.')
        
        with self.engine.connect() as conn:
            StockDataClass = self.get_table_name()
            for ticker in self.tickers:
                try:
                    logging.debug(f"Processing ticker {ticker}.")
                    query = select(func.max(StockDataClass.date)).where(StockDataClass.ticker == ticker)
                    last_date = conn.execute(query).scalar()
            
                    if last_date is not None:
                        start_date = self.calculate_next_start_date(last_date, self.timespan)
                        if start_date <= last_date:
                            logging.info(f"No new data available for {ticker}.")
                            continue
                    else:
                        start_date = self.start_date
                    
                    resp = client.list_aggs(
                        ticker, 
                        multiplier = self.multiplier,
                        timespan = self.timespan,
                        from_ = start_date, 
                        to = self.end_date,
                        limit = self.limit)
                
                    ticker_df = pd.DataFrame(resp)
                    
                    if not ticker_df.empty:
                        transformed_data = self.transform_data(ticker_df, ticker)
                        all_data.extend(transformed_data)
                    else:
                        logging.info(f"No new data available for ticker {ticker}.")
                except Exception as e:
                    logging.error(f"Error updating data for ticker {ticker}: {e}")
                    
                 
            if all_data:   
                try:
                    conn.execute(StockDataClass.__table__.insert(),all_data)
                    conn.commit()
                    logging.info(f'Data successfully updated for {len(all_data)} records')
                except Exception as e:
                    logging.error(f"Error updating stock data in bulk: {e}")
            else:
                logging.info("No data to update.")




