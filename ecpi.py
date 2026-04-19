import sys
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# Ensure adjust directory is in the module search path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from adjust.config import BASE_URL, SERVICE_ACCOUNT_JSON, PROJECT_ID, DATASET_ID, COMMON_DIMENSIONS, COMMON_PARAMS, HEADERS


# Define the table ID
TABLE_ID = "ecpi_all_mixed"

# Define the date range
end_date = datetime.today().strftime("%Y-%m-%d")
start_date = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")

# Define required metrics
metrics = ["ecpi_all" ]

# API request parameters (using COMMON_PARAMS)
params = {**COMMON_PARAMS, 
          "date_period": f"{start_date}:{end_date}",
          "metrics": ",".join(metrics),
          "dimensions": ",".join(COMMON_DIMENSIONS)}

# Headers
headers = HEADERS

# API request
response = requests.get(BASE_URL, headers=headers, params=params)

print("Status Code:", response.status_code)
print("Response Text (first 500 chars):", response.text[:500])

if response.status_code == 200 and response.text.strip():
    try:
        data = response.json()

        # Extract actual data from "rows" key
        if "rows" in data and isinstance(data["rows"], list):
            df = pd.DataFrame(data["rows"])

            # Convert all metric columns to float64
            for metric in metrics:
                df[metric] = pd.to_numeric(df[metric], errors='coerce').fillna(0).astype('float64')

            # Load Data into BigQuery
            credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON)
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite previous data
            )

            client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            print(f"\n✅ Data successfully uploaded to BigQuery Table: {table_ref}")

        else:
            raise ValueError("Unexpected API response format: 'rows' key missing or invalid")

    except requests.exceptions.JSONDecodeError as e:
        print("\n❌ JSON Decode Error:", e)
    except ValueError as e:
        print("\n❌ Value Error:", e)
else:
    print("\n❌ API request failed or returned an empty response.")
