from sqlalchemy import create_engine, text, Integer, String, Float, DateTime, BigInteger, UniqueConstraint
from dotenv import load_dotenv
import os
import asyncio
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker


# Load environment variables
load_dotenv()

# Database connection details
username = os.getenv("DATABASE_USERNAME")
password = os.getenv("DATABASE_PASSWORD")
host = os.getenv("DATABASE_HOST")
port = os.getenv("DATABASE_PORT")
database = os.getenv("DATABASE_NAME")

# Create an async engine
engine = create_async_engine(f'postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}')

# Define sessionmaker
AsyncSessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False)

# Base class for the models
class Base(DeclarativeBase):
    pass

# Define models
class DailyStockData(Base):
    __tablename__ = 'daily_stock_data'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    transactions: Mapped[int] = mapped_column(Integer)
    __table_args__ = (
        UniqueConstraint('ticker', 'date', name='unique_daily_ticker_date'),
    )

class HourlyStockData(Base):
    __tablename__ = 'hourly_stock_data'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    transactions: Mapped[int] = mapped_column(Integer)
    __table_args__ = (
        UniqueConstraint('ticker', 'date', name='unique_hourly_ticker_date'),
    )

class OneMinuteStockData(Base):
    __tablename__ = 'one_minute_stock_data'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    transactions: Mapped[int] = mapped_column(Integer)
    __table_args__ = (
        UniqueConstraint('ticker', 'date', name='unique_one_minute_ticker_date'),
    )

class FiveMinuteStockData(Base):
    __tablename__ = 'five_minute_stock_data'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    transactions: Mapped[int] = mapped_column(Integer)
    __table_args__ = (
        UniqueConstraint('ticker', 'date', name='unique_five_minute_ticker_date'),
    )

class FifteenMinuteStockData(Base):
    __tablename__ = 'fifteen_minute_stock_data'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    timestamp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    transactions: Mapped[int] = mapped_column(Integer)
    __table_args__ = (
        UniqueConstraint('ticker', 'date', name='unique_fifteen_minute_ticker_date'),
    )


class StockSplits(Base):
    __tablename__ = 'stock_splits'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    execution_date: Mapped[DateTime] = mapped_column(DateTime, nullable=False)
    split_from: Mapped[int] = mapped_column(Integer, nullable=False)
    split_to: Mapped[int] = mapped_column(Integer, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    
    __table_args__ = (
        UniqueConstraint('ticker', 'execution_date', name='unique_ticker_execution_date'),
    )

class StockNews(Base):
    __tablename__ = 'stock_news'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    article_url: Mapped[str] = mapped_column(String, nullable=True)
    author: Mapped[str] = mapped_column(String, nullable=True)
    description: Mapped[str] = mapped_column(String, nullable=True)
    id_polygon: Mapped[str] = mapped_column(String, nullable=True)  
    keywords: Mapped[JSONB] = mapped_column(JSONB, nullable=True)  
    published_utc: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    tickers: Mapped[JSONB] = mapped_column(JSONB, nullable=True)  
    ticker_queried: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String, nullable=True)
    insights: Mapped[JSONB] = mapped_column(JSONB, nullable=True)  
    
    __table_args__ = (
        UniqueConstraint('published_utc', 'ticker_queried', name='unique_published_ticker'),
    )

    
class CompanyFinancials(Base):
    __tablename__ = 'company_financials'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    company_name: Mapped[str] = mapped_column(String, index=True)
    start_date: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    end_date: Mapped[DateTime] = mapped_column(DateTime, nullable=False, index=True)
    filing_date: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    fiscal_period: Mapped[str] = mapped_column(String)
    financials: Mapped[JSONB] = mapped_column(JSONB)
    fiscal_year: Mapped[str] = mapped_column(String)
    acceptance_datetime: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    timeframe: Mapped[str] = mapped_column(String)
    tickers: Mapped[str] = mapped_column(String)
    sic: Mapped[int] = mapped_column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint('tickers', 'start_date', name='unique_ticker_start_date'),
    )

class StockTrades(Base):
    __tablename__ = 'stock_trades'
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    conditions: Mapped[str] = mapped_column(String)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    exchange: Mapped[int] = mapped_column(Integer)
    trade_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    participant_timestamp: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    size: Mapped[float] = mapped_column(Float, nullable=False)
    tape: Mapped[int] = mapped_column(Integer)
    trf_id: Mapped[float] = mapped_column(Float)
    correction: Mapped[float] = mapped_column(Float)
    trf_timestamp: Mapped[float] = mapped_column(Float)
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    sip_timestamp: Mapped[int] = mapped_column(Integer, nullable=False)
    tickers: Mapped[str] = mapped_column(String)  # Assuming this is the intended column name
    ticker_queried: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String)

    __table_args__ = (
        UniqueConstraint('trade_id', 'date', name='unique_trade_date'),
    )

# Async functions for dropping and creating tables
async def drop_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Main function to drop and create tables
async def main():
    await drop_tables()
    await create_tables()

# Entry point
if __name__ == '__main__':
    asyncio.run(main())
