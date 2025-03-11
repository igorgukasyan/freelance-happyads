import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import pytz
from urllib.parse import quote
load_dotenv(override=True)

BASE_URL = "https://business-api.tiktok.com"

# Function to build the API URL
def build_url(PATH, query=""):
    return f"{BASE_URL}{PATH}?{query}"

def request_inactive_adgroups(advertiser_id, campaign_ids, access_token):
    try:
        fields = ['adgroup_id', 'operation_status']
        page_size = 1000
        filtering = {
            "primary_status": "STATUS_DISABLE",
            "campaign_ids": campaign_ids
        }

        query_params = {
            "advertiser_id": advertiser_id,
            "fields": json.dumps(fields),
            "page": 1,
            "page_size": page_size,
            "filtering": json.dumps(filtering)
        }

        query_string = "&".join([f"{key}={value}" for key, value in query_params.items()])
        PATH = "/open_api/v1.3/adgroup/get/"
        url = build_url(PATH, query_string)
        print("Generated URL:", url)

        headers = {"Access-Token": access_token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        res_data = response.json()
        res_df = pd.json_normalize(res_data.get('data', {}).get('list', []))
        if not res_df.empty:
            return res_df['adgroup_id']
        else: 
            print("No inactive adgroups")
    except requests.exceptions.RequestException as e: 
        raise Exception(f"Failed to fetch report: {e}")

def batch(adgroups, batch_size = 20):
     return [adgroups.iloc[i:i + batch_size].tolist() for i in range(0, len(adgroups), batch_size)]

def delete_batch(advertiser_id, access_token, batch):
    try:

        payload = {
                        "advertiser_id": advertiser_id,
                        "adgroup_ids": batch,
                        "operation_status": "DELETE",
                    }
        
        PATH = "/open_api/v1.3/adgroup/status/update/"
        url = f'{BASE_URL}{PATH}'
        print("Generated URL:", url)

        headers = {"Access-Token": access_token,
                "Content-Type": "application/json"}
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return print("Adgroups deleted")
    except requests.exceptions.RequestException as e: 
        raise Exception(f"Failed to delete adgroups: {e}")



def delete_inactive_adgroups(advertiser_id, campaign_ids, access_token):

    try: 
        inactive_adgroups = request_inactive_adgroups(advertiser_id, campaign_ids, access_token)
        adgroup_batches = batch(inactive_adgroups)

        if len(adgroup_batches) > 1:
            for b in adgroup_batches:    
                delete_batch(advertiser_id, access_token, b)
        else: 
            delete_batch(advertiser_id, access_token, adgroup_batches[0])
    except requests.exceptions.RequestException as e:
        raise Exception(e)
