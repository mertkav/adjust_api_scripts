import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from config import (
    BASE_URL,
    SERVICE_ACCOUNT_JSON,
    PROJECT_ID,
    DATASET_ID,
    HEADERS,
)

TABLE_ID = "revenue_ad"

metrics = [
    "ad_revenue_d0",
    "ad_revenue_d1",
    "ad_revenue_d2",
    "ad_revenue_d3",
    "ad_revenue_d4",
    "ad_revenue_d5",
    "ad_revenue_d6",
    "ad_revenue_d7",
    "ad_revenue_d8",
    "ad_revenue_d14",
    "ad_revenue_d30",
    "ad_revenue_d45",
    "ad_revenue_d75",
    "ad_revenue_d90",
]

dimensions = [
    "day",
    "os_name",
    "country",
    "store_id",
    "app",
    "currency_code",
    "campaign",
    "channel",
]

params = {
    "format": "json",
    "metrics": ",".join(metrics),
    "dimensions": ",".join(dimensions),
    "store_id__in": "com.wixot.merge,1635526159",
}


def fetch_adjust_data() -> pd.DataFrame:
    response = requests.get(BASE_URL, headers=HEADERS, params=params)
    print("Status Code:", response.status_code)
    print("Response Text (first 500 chars):", response.text[:500])

    if response.status_code != 200 or not response.text.strip():
        raise ValueError("API request failed or returned an empty response.")

    data = response.json()

    if "rows" not in data or not isinstance(data["rows"], list):
        raise ValueError("Unexpected API response format: 'rows' key missing or invalid.")

    df = pd.DataFrame(data["rows"])

    for metric in metrics:
        df[metric] = pd.to_numeric(df[metric], errors="coerce").astype("float64")

    return df


def upload_to_bigquery(df: pd.DataFrame) -> None:
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_JSON
    )
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    print(f"\n✅ Data successfully uploaded to BigQuery Table: {table_ref}")


def main() -> None:
    df = fetch_adjust_data()

    filename = "adjust_ad_revenue.csv"
    df.to_csv(filename, index=False)
    print(f"\n✅ Data successfully fetched and saved as {filename}")

    upload_to_bigquery(df)


if __name__ == "__main__":
    main()