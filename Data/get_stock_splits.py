# %%
import sqlalchemy
import pandas as pd
import numpy as np
from polygon import RESTClient
import datetime as dt
import os
from dotenv import load_dotenv
from connect import engine, Base, Stock_Splits
from log_config import setup_logging
from sqlalchemy import select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import logging


class Stock_Splits_update:
    " Class to update stock split data from Polygon.io"

    def __init__ (self,tickers, engine, limit = 1000):
        self.tickers = tickers if isinstance(tickers, list) else [tickers]
        self.engine = engine
        self.limit = limit
        
    def transform_data(self,df,ticker):
        df['ticker '] = ticker
        df['execution_date'] = pd.to_datetime(df['execution_date'])
        return df.to_dict(orient = 'records')
    
    def update_data(self, client):
        with self.engine.connect() as conn:
            for ticker in self.tickers:
                splits = pd.DataFrame(client.list_splits(ticker, limit = self.limit))
                if not splits.empty:
                    transformed_data = self.transform_data(splits, ticker)
                    try:    
                        conn.execute(Stock_Splits.__table__.insert(),transformed_data)
                        conn.commit()
                            
                    except Exception as e:
                        logging.error(f"Error updating stock splits for {ticker}: {e}")
                        continue




