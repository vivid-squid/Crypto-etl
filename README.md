# Airflow Snowflake Crypto ETL

A simple end-to-end ETL pipeline that extracts cryptocurrency prices from the CoinGecko API, stores raw data in AWS S3, loads it into Snowflake, and prepares it for analytics.
- API ingestion  
- Data lake → warehouse pipeline  
- Airflow orchestration  
- Snowflake semi-structured processing  
- End-to-end data engineering workflow  

## Overview

This project demonstrates a modern data pipeline using:

- **Apache Airflow** → orchestration  
- **Docker** → reproducible environment  
- **AWS S3** → raw data storage  
- **Snowflake** → data warehouse  
- **Tableau** → visualization


## Data Flow

CoinGecko API → Airflow → S3 (raw JSON) → Snowflake (RAW → ANALYTICS) → Tableau


## Key Features

- Extracts **Bitcoin & Ethereum prices**
- Stores each run as a **timestamped JSON file in S3**
- Loads semi-structured data into **Snowflake (VARIANT)**
- Transforms data into **analytics-ready tables**
- Fully orchestrated using an **Airflow DAG**
- Runs locally using **Docker Compose**


## How to Run

 - docker compose up --build
 - Open Airflow UI
 - Trigger DAG: api_to_s3_crypto
 - Verify data in S3 and Snowflake


## Future Improvements

- Incremental loading (remove full reloads)  
- Add data quality checks  
- Use secrets manager for credentials  
- Add dbt or testing layer  
