import sys
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

# Add the path to the adjust directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'adjust')))

#import the config module
from config import BASE_URL, SERVICE_ACCOUNT_JSON, PROJECT_ID, DATASET_ID, COMMON_DIMENSIONS, COMMON_PARAMS, HEADERS

# API Key
API_KEY = "hkQpRH8zxL7ZbWEmJyFP"
TABLE_ID = "ltv_cumulative"

# Define the date range
end_date = datetime.today().strftime("%Y-%m-%d")
start_date = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")

# Define required metrics
metrics = [
    "lifetime_value_d0", "lifetime_value_d1", "lifetime_value_d2", "lifetime_value_d3",
    "lifetime_value_d4", "lifetime_value_d5", "lifetime_value_d6", "lifetime_value_d7",
    "lifetime_value_d8", "lifetime_value_d14", "lifetime_value_d30", "lifetime_value_d45",
    "lifetime_value_d75", "cohort_size_d0", "cohort_size_d1", "cohort_size_d2", "cohort_size_d3",
    "cohort_size_d4", "cohort_size_d5", "cohort_size_d6", "cohort_size_d7",
    "cohort_size_d8", "cohort_size_d14", "cohort_size_d30", "cohort_size_d45",
    "cohort_size_d75", "cohort_size_d90", "retained_users_d0", "cost"
]

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

            # Save locally as CSV (Optional)
            filename = "/Users/mertkav/adjust_aplovin_api/logs/adjust_ltv_cumulative.csv"
            df.to_csv(filename, index=False)
            print(f"\n✅ Data successfully fetched and saved as {filename}")

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
