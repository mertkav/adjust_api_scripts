import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

API_KEY = "hkQpRH8zxL7ZbWEmJyFP"

# Google BigQuery Credentials 
SERVICE_ACCOUNT_JSON = "/Users/mertkav/Documents/wixot/jsonkey-airflow/more-than-friends-69eca-b6f2ce7a19c5.json" 
PROJECT_ID = "more-than-friends-69eca"  
DATASET_ID = "adjust"
TABLE_ID = "arpdau"

# Adjust API Base URL
BASE_URL = "https://automate.adjust.com/reports-service/report"

# Define the date range
end_date = datetime.today().strftime("%Y-%m-%d")
start_date = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")

# Define required metrics
metrics = ["arpdau", "daus", "all_revenue", "ad_revenue", "revenue",  "cohort_revenue", "cohort_all_revenue"]

# Dimensions
dimensions = ["day","os_name", "country", "store_id", "app", "currency_code", "campaign", "channel"]

# Parameters
params = {
    "date_period": f"{start_date}:{end_date}",
    "format": "json",
    "metrics": ",".join(metrics),
    "dimensions": ",".join(dimensions),
    "store_id__in": "com.wixot.merge,1635526159",
    "attribution_source" : "first",
    "ad_spend_mode" : "mixed",
    "os_name__in": "ios,android",
    "attribution_status":"all",
    ## "channel__in": "applovin, organic",
    "attribution_type": "all"

}
# Headers
headers = {
    "Authorization": f"Bearer {API_KEY}", 
    "Content-Type": "application/json"
}

# API request
response = requests.get(BASE_URL, headers=headers, params=params)

print("Status Code:", response.status_code)
print("Response Text (first 500 chars):", response.text[:500])  

if response.status_code == 200 and response.text.strip():
    try:
        data = response.json()  # Parse JSON response

        # Extract actual data from "rows" key
        if "rows" in data and isinstance(data["rows"], list):
            df = pd.DataFrame(data["rows"])
            
            # Convert total revenue metrics to float64
            for metric in metrics:
                df[metric] = pd.to_numeric(df[metric], errors='coerce').fillna(0).astype('float64')

            # Save loctotaly as CSV (Optional)
            filename = "adjust_arpdau.csv"
            df.to_csv(filename, index=False)
            print(f"\n✅ Data successfully fetched and saved as {filename}")

            # Load Data into BigQuery
            credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON)
            client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Append data to existing table
                autodetect=True  # Let BigQuery determine schema automatically
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
