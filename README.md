## Overview

This project implements a PySpark ETL pipeline for processing data from a dice game platform.

The pipeline performs:
- Data ingestion from CSV files
- Transformation into dimension and fact tables
- Data quality validation (null checks, date consistency, range checks)
- Insights generation (user engagement, revenue, MAU trends, channel analysis)
- Output storage as Parquet and CSV

## Project Structure

FCC-assessment/
│── data/ # Source CSV files (input datasets)
│ ├── channel_code.csv
│ ├── status_code.csv
│ ├── plan_payment_frequency.csv
│ ├── user_play_session.csv
│ ├── user_registration.csv
│ ├── user_plan.csv
│ ├── user_payment_detail.csv
│ └── plan.csv
│
│── output/ # Generated dimension & fact tables (Parquet)
│── insights/ # Aggregated insights (CSV)
│── main.py # PySpark ETL script (this file)
│── requirements.txt # Dependencies
│── README.md # Documentation
