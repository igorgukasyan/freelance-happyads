
import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import pytz
from urllib.parse import quote
load_dotenv(override=True)
BASE_URL = "https://business-api.tiktok.com/open_api/v1.3/adgroup/get/"
# Function to build the API URL
def build_url(query=""):
    return f"{BASE_URL}?{query}"

# Function to send API request and get data for one page
def request_adgroup_status_onepage(page, advertiser_id, access_token):
    try:
        fields = ['adgroup_id', 'operation_status']
        page_size = 1000
        filtering = {
            "primary_status":"STATUS_DELIVERY_OK" 
        }

        # Construct query string
        query_params = {
            "advertiser_id": advertiser_id,
            "fields": json.dumps(fields),
            "page": page,
            "page_size": page_size,
            "filtering": json.dumps(filtering)
        }
        query_string = "&".join([f"{key}={value}" for key, value in query_params.items()])
        
        # Build and send the request
        url = build_url(query_string)
        print("Generated URL:", url)
        
        headers = {"Access-Token": access_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        if response.json():
            res_data = response.json()
            res_df = pd.json_normalize(res_data.get('data', {}).get('list', []))
            if not res_df.empty:
                return {"data" : res_df,
                        "total_pages": res_data.get('data', {}).get('page_info', []).get('total_page', 0)}
            else:
                print("No data found")
                return {"data": pd.DataFrame(), "total_pages": 0}
        else:
            print("Response was empty") 
            return {"data": pd.DataFrame(), "total_pages": 0}
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch report: {e}")



def get_all_active_adgroups_one_account(advertiser_id, api_key):
    initial_res = request_adgroup_status_onepage(page = 1, advertiser_id = advertiser_id, access_token = api_key)
    total_pages = initial_res.get('total_pages', 0)
    if total_pages == 1:
        return initial_res['data']
    
    elif total_pages > 1: 
        all_data = [initial_res['data']]
        for page in range(2, total_pages+1):
                temp_res = request_adgroup_status_onepage(page = page, advertiser_id = advertiser_id, access_token = api_key)
                all_data.append(temp_res['data'])
        return pd.concat(all_data, ignore_index=True)
    else: 
        return pd.DataFrame()


def get_all_active_adgroups():
    all_adgroup_statuses_list = []
    for _, row in account_apis_separated.iterrows():
          advertiser_id = row['advertiser_ids']
          api_key = row['api_keys']
          res = get_all_active_adgroups_one_account(advertiser_id = advertiser_id, api_key = api_key)
          all_adgroup_statuses_list.append(res)
    return pd.concat(all_adgroup_statuses_list, ignore_index=True)
