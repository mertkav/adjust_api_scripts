# Adjust API → BigQuery Pipeline

Python-based ETL project that pulls marketing and monetization data from the Adjust API and loads it into Google BigQuery.

## Overview

This project automates data extraction for key analytics metrics:

- ROAS
- Revenue (IAP + Ad)
- Retention
- LTV
- Cohort Size
- Sessions
- ARPDAU
- Cost / CPI / eCPM

Each script:
1. Calls Adjust API
2. Converts data into pandas DataFrame
3. Cleans and formats data
4. Uploads to BigQuery

## Tech Stack

- Python
- pandas
- requests
- Google BigQuery
- Adjust API

## Example Usage

```bash
python session.py
python retention.py
python roas_cumulative.py