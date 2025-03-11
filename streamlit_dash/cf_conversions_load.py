import requests
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import pytz
from urllib.parse import quote
load_dotenv(override=True)
# Utility function to get today's date in the required format
def msk_date():
    ## Get the current date in Moscow timezone
    return datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d')

def get_adset_conversions_onepage(page = 1):
    base_url = 'https://public-api.clickflare.io/api/report'

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "api-key": os.getenv('CLICKFLARE_API_KEY')
    }

    payload = {
        'startDate' : str(msk_date()) + " 00:00:00",
        'endDate' : str(msk_date()) + " 23:59:59",
        'groupBy' : ['trackingField6', 'hourOfDay'],
        'metrics' : ['conversions'],
        'timezone' : 'Europe/Moscow',
        'sortBy' : 'conversions',
        'orderType' : 'asc',
        'page' : page, 
        'pageSize' : 1000,
        'currency' : 'USD',
        'includeAll' : True,
        'metricsFilters' : [
                {
                    'name' : 'conversions',
                    'operator' : '>',
                    'value' : 0
                }
        ]
    }
    try: 
        response = requests.post(base_url, json=payload, headers=headers)
        response.raise_for_status()

        if response.json():
            res_data = response.json()
            res_df = pd.json_normalize(res_data.get('items', []))
            if not res_df.empty:
                res_df = res_df.drop(columns='counter', errors='ignore')
                res_df = res_df.rename(columns = {"trackingField6": "adgroup_id",
                                                  "conversions": "cf_conversions"})
                return {"data" : res_df,
                        "total_rows": res_data.get('totals', {}).get('counter', 0)}
            else:
                return {"data": pd.DataFrame(), "total_rows": 0}
        else: 
            return {"data": pd.DataFrame(), "total_rows": 0}
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch report: {e}")
    

def get_all_conversions():
     initial_res = get_adset_conversions_onepage(page = 1)
     total_rows = initial_res['total_rows']
     total_pages = total_rows // 1000 + (total_rows % 1000 > 0)

     if total_pages == 1:
        return initial_res['data']
     
     elif total_pages > 1: 
        all_data = initial_res['data']
        for page in range(2, total_pages+1):
               temp_res = get_adset_conversions_onepage(page = page)
               all_data = pd.concat([all_data, temp_res['data']], ignore_index=True)
        return all_data.drop_duplicates()
     else: 
          return pd.DataFrame()
