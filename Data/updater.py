# %%
from connect import engine, DailyStockData, HourlyStockData, MinuteStockData, StockSplits, StockNews, CompanyFinancials
from log_config import setup_logging
from get_company_data import CompanyFinancialsupdater
from getstockdata import MarketDataUpdater
from get_fin_news import NewsUpdate
from get_stock_splits import StockSplitsupdate
import pandas as pd
import logging
from datetime import datetime, timedelta
import datetime as dt
import pytz
from dotenv import load_dotenv
import os
import asyncio

# %%
setup_logging()

# %%
load_dotenv()
key = os.getenv("API_KEY")

# %%
wiki = 'http://en.wikipedia.org/wiki'
djia_ticker_list = wiki + '/Dow_Jones_Industrial_Average'
sp500_tickers_list = wiki + '/List_of_S%26P_500_companies'
tickersSP500 = pd.read_html(sp500_tickers_list)[0].Symbol.to_list()
djia_tickers = pd.read_html(djia_ticker_list)[1].Symbol.to_list()

# %%
company_financials_updater = CompanyFinancialsupdater(tickers=djia_tickers, engine=engine, key=key)
market_data_updater = MarketDataUpdater(tickers=djia_tickers, engine=engine, key=key, start_date='2020-01-05')
news_update = NewsUpdate(tickers=djia_tickers, engine=engine, key=key)
stock_splits_update = StockSplitsupdate(tickers=djia_tickers, engine=engine, key=key)

# %%
async def main():
    await asyncio.gather(
        company_financials_updater.update_data(),
        market_data_updater.update_data(),
        news_update.update_data(),
        stock_splits_update.update_data()
    )

# %%
if __name__ == '__main__':
    asyncio.run(main())

# %%



