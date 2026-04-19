from datetime import datetime, timedelta

# API Key
API_KEY = "ADJUST_API_KEY"

# Google BigQuery Credentials
SERVICE_ACCOUNT_JSON = "GOOGLE_APPLICATION_CREDENTIALS"
PROJECT_ID = "PROJECT_ID"
DATASET_ID = "adjust"

# Adjust API Base URL
BASE_URL = "https://automate.adjust.com/reports-service/report"

# Define the date range
end_date = datetime.today().strftime("%Y-%m-%d")
start_date = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")

# Common dimensions
COMMON_DIMENSIONS = [
    "day", "os_name", "country", "store_id", "app", "campaign", "channel", "store_type"
]

# Common parameters
COMMON_PARAMS = {
    "date_period": f"{start_date}:{end_date}",
    "format": "json",
    "store_id__in": "com.wixot.merge,1635526159",
    "attribution_source": "first",
    "ad_spend_mode": "mixed",
    "os_name__in": "ios,android",
    "attribution_status": "all",
    "attribution_type": "all"
}

# Headers
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}
