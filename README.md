# Fin_Database


This project consists of a collection of Python scripts used to gather financial data from an API. The goal of these scripts is to automate the process of collecting and updating financial information for use in various data analysis and trading models.

## Overview

The repository includes scripts that handle different aspects of financial data collection, such as:

* **Stock Data** : Scripts to fetch historical and real-time stock data.
* **Company Information** : Scripts to retrieve company-specific data.
* **Financial News** : Scripts to gather the latest financial news relevant to specific stocks.
* **Stock Splits** : Scripts to track and record stock splits over time.
* **Trades** : Scripts to monitor and log trades for specific stocks.

These scripts are organized to efficiently interact with the Polygon.io API, collect necessary data, and store it in a structured format intoPostgreSQL suitable for further analysis or integration into financial models.

## Structure

* **`Data/`** : Contains all the scripts used to interact with the Polygon.io API.
* **`alembic/`** : Contains migration scripts for managing database schema changes.
* **`README.md`** : Provides an overview of the project and its components.

This repository is a work in progress and serves as a foundational component for automating the retrieval of financial data. The scripts are continually being refined and expanded to accommodate new data sources and requirements.
