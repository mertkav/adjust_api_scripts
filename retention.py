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
    COMMON_PARAMS,
)

TABLE_ID = "retention"

metrics = [
    "cohort_size_d0",
    "cohort_size_d1",
    "cohort_size_d2",
    "cohort_size_d3",
    "cohort_size_d4",
    "cohort_size_d5",
    "cohort_size_d6",
    "cohort_size_d7",
    "cohort_size_d8",
    "cohort_size_d14",
    "cohort_size_d30",
    "cohort_size_d45",
    "cohort_size_d75",
    "retention_rate_d0",
    "retention_rate_d1",
    "retention_rate_d2",
    "retention_rate_d3",
    "retention_rate_d4",
    "retention_rate_d5",
    "retention_rate_d6",
    "retention_rate_d7",
    "retention_rate_d8",
    "retention_rate_d14",
    "retention_rate_d30",
    "retention_rate_d45",
    "retention_rate_d75",
    "paying_users_retention_rate_d0",
    "paying_users_retention_rate_d1",
    "paying_users_retention_rate_d2",
    "paying_users_retention_rate_d3",
    "paying_users_retention_rate_d4",
    "paying_users_retention_rate_d5",
    "paying_users_retention_rate_d6",
    "paying_users_retention_rate_d7",
    "paying_users_retention_rate_d8",
    "paying_users_retention_rate_d14",
    "paying_users_retention_rate_d30",
    "paying_users_retention_rate_d45",
    "paying_users_retention_rate_d75",
    "paying_user_size_d0",
    "paying_user_size_d1",
    "paying_user_size_d2",
    "paying_user_size_d3",
    "paying_user_size_d4",
    "paying_user_size_d5",
    "paying_user_size_d6",
    "paying_user_size_d7",
    "paying_user_size_d8",
    "paying_user_size_d14",
    "paying_user_size_d30",
    "paying_user_size_d45",
    "paying_user_size_d75",
    "paying_users_d0",
    "paying_users_d1",
    "paying_users_d2",
    "paying_users_d3",
    "paying_users_d4",
    "paying_users_d5",
    "paying_users_d6",
    "paying_users_d7",
    "paying_users_d8",
    "paying_users_d14",
    "paying_users_d30",
    "paying_users_d45",
    "paying_users_d75",
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
    **COMMON_PARAMS,
    "metrics": ",".join(metrics),
    "dimensions": ",".join(dimensions),
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
        df[metric] = pd.to_numeric(df[metric], errors="coerce").fillna(0).astype("float64")

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

    filename = "adjust_retention.csv"
    df.to_csv(filename, index=False)
    print(f"\n✅ Data successfully fetched and saved as {filename}")

    upload_to_bigquery(df)


if __name__ == "__main__":
    main()