# Fin_Database

**Fin_Database** is a Python-based project that automates the retrieval and management of financial data from the **Polygon.io API**. The project leverages `asyncio` to handle concurrent API requests, improving the speed and efficiency of data retrieval. This project aims to streamline data collection, enabling seamless integration of financial information into data analysis workflows, trading algorithms, and financial models. The collected data will serve as a core component for multiple future projects.

## Overview

This repository contains multiple scripts to efficiently collect, process, and store financial data from **Polygon.io** into a **PostgreSQL database**. The data includes **stock prices, financial reports, news, stock splits, and trade logs**, which are essential for analysis, reporting, and model building. The collected data will also be leveraged across multiple projects in the future.

---

## Features

- **Stock Data**: Fetches historical stock prices and real-time data.
- **Financial Reports**: Retrieves key company financials such as earnings, revenue, and balance sheets.
- **Financial News**: Gathers the latest news related to specific companies or stocks.
- **Stock Splits**: Tracks and logs stock split events over time.
- **Data Updater**: Automates the updating process to keep the financial database current.

All collected data is stored in a **PostgreSQL database** for easy retrieval, transformation, and analysis.

---

## Architecture

The key components of this project are:

### 1. **Data Collection Scripts**

- **`getstockdata.py`**: Fetches minute-level, hourly, and daily stock price data.
- **`get_company_data.py`**: Collects financial reports, including earnings. balance sheets, income statements.
- **`get_fin_news.py`**: Retrieves the latest news articles relevant to selected stocks.
- **`get_stock_splits.py`**: Logs stock split events into the database.
- **`updater.py`**: Automates the update of all financial data regularly.

### 2. **API and Database Configuration**

- **`connect.py`**: Handles the connection to the PostgreSQL database.
- **`log_config.py`**: Configures logging for error tracking and monitoring API interactions.
- **AsyncIO Integration**: Uses `asyncio` and `httpx` to run multiple API calls concurrently, enhancing performance during data updates.

### 3. **Database Storage**

- Uses **PostgreSQL** to store and manage all collected financial data. The key tables include:
  - `stock_data`: Historical stock prices.
  - `financial_reports`: Company earnings, revenue, and balance sheets.
  - `news_data`: Financial news articles.
  - `stock_splits`: Stock split history.
  - `trade_data`: Logged trades.
