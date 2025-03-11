import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import pytz
from urllib.parse import quote
load_dotenv(override=True)
# Constants
PATH = "/open_api/v1.3/report/integrated/get/"
BASE_URL = "https://business-api.tiktok.com"

# Utility function to get today's date in the required format
def msk_date():
    ## Get the current date in Moscow timezone
    return datetime.now(pytz.timezone('Europe/Moscow')).date()

# Function to build the API URL
def build_url(path, query=""):
    return f"{BASE_URL}{path}?{query}"


# Function to send GET request for one page of data
def request_data_onepage(page, advertiser_ids, access_token):
    try:

        service_type = "AUCTION"
        data_level = "AUCTION_ADGROUP"
        report_type = "BASIC"
        query_lifetime = "false"
        page_size = 1000
        filtering = json.dumps([
            {
        "field_name": "adgroup_status",
        "filter_type": "IN",
        "filter_value": "[\"STATUS_ALL\"]"
            }
        ])

        dimensions = json.dumps(["adgroup_id", "stat_time_hour"])
        metrics = json.dumps([
            "real_time_cost_per_conversion", "clicks",
            "real_time_conversion", "real_time_conversion_rate_v2",
            "campaign_id", "campaign_name", "campaign_budget", "bid", "spend", "impressions"
        ])

        # Date range based on the period
        start_date = end_date = msk_date()

        # Query string construction
        query_string = (
            f"advertiser_ids={quote(advertiser_ids)}&service_type={service_type}&data_level={data_level}"
            f"&report_type={report_type}&dimensions={quote(dimensions)}&metrics={quote(metrics)}"
            f"&start_date={start_date}&end_date={end_date}&page={page}&page_size={page_size}"
            f"&query_lifetime={query_lifetime}&filtering={quote(filtering)}"
        )
        # Build and send the request
        url = build_url(PATH, query_string)
        print("Generated URL:", url)
        
        headers = {"Access-Token": access_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        # Parse and return the response
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error during API request: {e}")
        return {}


def retrieve_data(onepage_data):
    data_list = onepage_data.get("data", {}).get("list", [])
    if not data_list:
        return pd.DataFrame()
    
    df = pd.json_normalize(data_list)
    df.columns = df.columns.str.replace('dimensions.', '').str.replace('metrics.', '')
    return df


def fetch_all_pages(advertiser_ids, access_token):
    data = pd.DataFrame()
    page = 1
    
    while True:
        print(f"Fetching page {page}")
        res = request_data_onepage(page, advertiser_ids, access_token)

        # Retrieve and append data from the current page
        data_onepage = retrieve_data(res)

        # Check if data_onepage is a DataFrame and not empty
        if not isinstance(data_onepage, pd.DataFrame):
            print(f"Skipping invalid data (type: {type(data_onepage)}) for page {page}")
            break  # Exit the loop if the data is not valid

        if data_onepage.empty:
            print(f"No data found on page {page}. Exiting...")
            break  # Exit the loop if the page is empty

        data = pd.concat([data, data_onepage], ignore_index = True)

        # Check if all pages are fetched
        total_pages = res.get("data", {}).get("page_info", {}).get("total_page")
        current_page = res.get("data", {}).get("page_info", {}).get("page")

        if total_pages == current_page:
            print("Fetched all pages")
            break

        page += 1

    return data


def clean_tiktok_data(data):
        if not data.empty:
            data["stat_time_hour"] = pd.to_datetime(data["stat_time_hour"], format = '%Y-%m-%d %H:%M:%S', errors='coerce').dt.tz_localize('Europe/Moscow').dt.strftime("%Y-%m-%d %H:00")
            data["stat_time_hour"] = pd.to_datetime(data['stat_time_hour']).dt.tz_localize('Europe/Moscow')
        # Find all ID columns

            id_cols = [col for col in data.columns if "_id" in col]
            for col in id_cols:
                data[col] = data[col].astype(str)
        
            numeric_cols = [col for col in data.columns if col not in id_cols+["stat_time_hour", "campaign_name"]]

            for col in numeric_cols:
                data[col] = pd.to_numeric(data[col], errors = 'coerce')

            data.rename(columns={
                "real_time_conversion_rate_v2": "rt_conversion_rate",
                "real_time_conversion": "rt_conversions",
                "real_time_cost_per_conversion": "rt_cpa"
            }, inplace = True)

            data = data[["stat_time_hour", "advertiser_id", "clicks", "campaign_id", "adgroup_id", "campaign_name",
                          "rt_conversions", "spend", "impressions", "bid", "rt_cpa",
                          "rt_conversion_rate", "campaign_budget"]]

        return data





# Function to fetch and clean raw TikTok data for given advertiser IDs and access tokens
def get_raw_tiktok_data(advertiser_ids, access_token):
    raw_data = fetch_all_pages(advertiser_ids, access_token)
    return clean_tiktok_data(raw_data)




# Main function to aggregate TikTok advertiser data across all accounts
def get_report_tiktok_today():
    all_responses=pd.DataFrame()
    for _, row in account_apis.iterrows(): ## account_apis should be a data frame with columns api-id
        access_token = row["api_keys"]
        advertiser_ids = row["advertiser_ids"]

        res_data = get_raw_tiktok_data(advertiser_ids, access_token)
        # Check the type of res_data
        print(f"res_data type: {type(res_data)}")

        if isinstance(res_data, pd.DataFrame):
            all_responses = pd.concat([all_responses, res_data], ignore_index=True)
        else:
            print(f"Skipping invalid data for advertiser {advertiser_ids}")
    return all_responses



