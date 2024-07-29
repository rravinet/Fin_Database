# %%
from sqlalchemy import create_engine, text, Integer, String, Float, DateTime, BigInteger,UniqueConstraint
from dotenv import load_dotenv
import os
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from datetime import datetime
from sqlalchemy.dialects.postgresql import JSONB

# %%
load_dotenv()


# %%
username = os.getenv("DATABASE_USERNAME")
password = os.getenv("DATABASE_PASSWORD")
host = os.getenv("DATABASE_HOST")
port = os.getenv("DATABASE_PORT")
database = os.getenv("DATABASE_NAME")


# %%
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')



def test():
    with engine.connect() as connection:
        result = connection.execute(text("SELECT 'Hello'"))
        print(result.all())

if __name__ == '__main__':
    test()


# %%
class Base(DeclarativeBase):
    
    pass

class Daily_Stock_Data(Base):
    
    __tablename__ = 'daily_stock_data'
    id:Mapped[int] = mapped_column(primary_key=True,autoincrement=True)
    date:Mapped[datetime] = mapped_column(DateTime, nullable=False)
    timestamp:Mapped[int] = mapped_column(BigInteger, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    transactions: Mapped[int] = mapped_column(Integer)
    
    __table_args__ = (
        UniqueConstraint('ticker', 'date', name='unique_ticker_date'),
    )



# %%
class Hourly_Stock_Data(Base):
    
    __tablename__ = 'hourly_stock_data'
    id:Mapped[int] = mapped_column(primary_key=True,autoincrement=True)
    date:Mapped[datetime] = mapped_column(DateTime, nullable=False)
    timestamp:Mapped[int] = mapped_column(BigInteger, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    transactions: Mapped[int] = mapped_column(Integer)
    
    __table_args__ = (
        UniqueConstraint('ticker', 'date', name='unique_ticker_date'),
    )

# %%
class Minute_Stock_Data(Base):
    
    __tablename__ = 'minute_stock_data'
    id:Mapped[int] = mapped_column(primary_key=True,autoincrement=True)
    date:Mapped[datetime] = mapped_column(DateTime, nullable=False)
    timestamp:Mapped[int] = mapped_column(BigInteger, nullable=False)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float] = mapped_column(Float)
    transactions: Mapped[int] = mapped_column(Integer)
    
    __table_args__ = (
        UniqueConstraint('ticker', 'date', name='unique_ticker_date'),
    )

# %%
class Stock_Splits(Base):
    __tablename__ = 'stock_splits'
    id:Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    execution_date:Mapped[datetime] = mapped_column(DateTime, nullable=False)
    split_from:Mapped[int] = mapped_column(Integer, nullable=False)
    split_to:Mapped[int] = mapped_column(Integer, nullable=False)
    ticker:Mapped[str] = mapped_column(String(10), nullable=False)


# %%
class Stock_News(Base):
    __tablename__ = 'stock_news'
    id:Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    article_url: Mapped[str] = mapped_column(String, nullable=True)
    author: Mapped[str] = mapped_column(String, nullable=True)
    description: Mapped[str] = mapped_column(String, nullable=True)
    id_polygon: Mapped[str] = mapped_column(String, nullable=True)
    keywords: Mapped[str] = mapped_column(String, nullable=True)
    published_utc: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    tickers: Mapped[str] = mapped_column(String, nullable=True)
    ticker_queried: Mapped[str] = mapped_column(String(10), nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=True)
    


# %%
class Company_Financials(Base):
    
    __tablename__ = 'company_financials'
    id:Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    company_name:Mapped[str] = mapped_column(String)
    start_date:Mapped[datetime] = mapped_column(DateTime, nullable=False)
    end_date:Mapped[datetime] = mapped_column(DateTime, nullable=False)
    filing_date:Mapped[datetime] = mapped_column(DateTime, nullable=True)
    fiscal_period:Mapped[str] = mapped_column(String)
    financials: Mapped[JSONB] = mapped_column(JSONB)
    fiscal_year:Mapped[str] = mapped_column(String)

# %%
class Stock_Trades(Base):
    __tablename__ = 'stock_trades'
    id:Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    conditions: Mapped[str] = mapped_column(String)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    exchange : Mapped[int] = mapped_column(Integer)
    trade_id : Mapped[str] = mapped_column(String, nullable=False)
    participant_timestamp : Mapped[int] = mapped_column(Integer)
    price : Mapped[float] = mapped_column(Float, nullable=False)
    size : Mapped[float] = mapped_column(Float, nullable=False)
    tape : Mapped[int] = mapped_column(Integer)
    trf_id : Mapped[float] = mapped_column(Float)
    correction: Mapped[float] = mapped_column(Float)
    trf_timestamp : Mapped[float] = mapped_column(Float)
    sequence_number : Mapped[int] = mapped_column(Integer, nullable=False)
    sip_timestamp : Mapped[int] = mapped_column(Integer, nullable=False)
    tickers : Mapped[str] = mapped_column(String)
    ticker_queried : Mapped[str] = mapped_column(String(10), nullable=False)
    title : Mapped[str] = mapped_column(String)
    
    
    
    
    



# %%
Base.metadata.create_all(engine)





