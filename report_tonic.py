# ### Retrieving TONIC data with API
import os
import requests
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import pytz

jwt_cache = {"tiktok":
             {
                 "token": None, 
                 "expires_at": None
             },
             "facebook":
             {
                 "token": None, 
                 "expires_at": None
   
             }
}

def msk_date():
    ## Get the current date in Moscow timezone
    return datetime.now(pytz.timezone('Europe/Moscow')).date()


def get_new_jwt(platform = "tiktok"):
    # Fetch a new JWT token
    url = "https://api.publisher.tonic.com/jwt/authenticate"
    # Define body based on platform
    if platform == "tiktok":
        body = {
        "consumer_key": CONSUMER_KEY_TIKTOK,
        "consumer_secret": CONSUMER_SECRET_TIKTOK
        }
    elif platform == "facebook":
        body = {
        "consumer_key": CONSUMER_KEY_FACEBOOK,
        "consumer_secret": CONSUMER_SECRET_FACEBOOK
    } 
    else: raise ValueError("Invalid platform selected.")
        
    # Try to get the JWT
    try: 
        response = requests.post(url, json = body)
        response.raise_for_status()

        content = response.json()
        jwt_cache[platform]["token"] = content["token"]
        jwt_cache[platform]["expires_at"] = datetime.fromtimestamp(content["expires"], timezone.utc)
        return content["token"]
    
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to obtain a new access token: {e}")


def get_valid_jwt(platform = "tiktok"):
    if jwt_cache[platform]["token"] and jwt_cache[platform]["expires_at"] and datetime.now(timezone.utc) < jwt_cache[platform]["expires_at"]-timedelta(seconds = 5):
        print("Previously fetched token still valid.")
        return jwt_cache[platform]["token"]
    
    print("Token expired or not found, fetching a new one...")
    return(get_new_jwt(platform))



def clean_tonic_data(data, platform = "tiktok"):
    ## Clean and pre-process the data
    ### Remove rows with empty subid1
    data = data[data["subid1"].notna() & (data['subid1']!= "")]

    # Split subid3 into multiple columns 
    if platform == "tiktok": 
        subid3_split = data['subid3'].str.split("|", expand = True)
        data['ad_id'], data['adgroup_id'], data['campaign_id'] = subid3_split[0], subid3_split[1], subid3_split[2]
        data['network'] = "tiktok"
    elif platform == "facebook":
        subid3_split = data['subid3'].str.split("|", expand = True)
        data['campaign_id'], data['adgroup_id'], data['ad_id'] = subid3_split[0], subid3_split[1], subid3_split[2]
        data['network'] = "facebook"
    else: raise ValueError("Invalid platform selected.")

    # Drop unnecessary columns
    columns_to_drop = ['subid4', 'keyword', 'site', 'adtitle', 'network', 'device']
    data.drop(columns = [col for col in columns_to_drop if col in data.columns], inplace = True)

    # Rename columns
    data.rename(columns = {
        'subid2': 'adgroup_name',
        'subid1': 'tiktok_campaign_name',
        'revenueUsd': 'revenue'
    }, inplace = True)
    
    # Convert column types and other
    data['clicks'] = pd.to_numeric(data['clicks'], errors = 'coerce')
    data['revenue'] = pd.to_numeric(data['revenue'], errors = 'coerce')
    return data



def aggregate_tonic_data(data, platform = 'tiktok'):
    
    cleaned_data = clean_tonic_data(data, platform = platform)
    aggregated_data = cleaned_data.groupby(['adgroup_name', 'adgroup_id'], as_index = False).agg({
        'clicks': 'sum',
        'revenue': 'sum'
    })

    aggregated_data = (
    aggregated_data
    .rename(columns={'clicks': 'conversions_tonic'})
    .loc[:, ['adgroup_id', 'conversions_tonic', 'revenue']]
    .assign(epc = lambda df: df['revenue']/df['conversions_tonic'])
    .drop(columns = ['revenue', 'conversions_tonic'])
    .dropna()
    )
    return aggregated_data

def get_advertiser_data_tonic(period = "today", platform = "tiktok"):
    base_url = "https://api.publisher.tonic.com/privileged/v3/reports/tracking"

    if period == "today":
        params = {"from": msk_date()-timedelta(days=1),
                  "to": msk_date()}
    else: 
        raise ValueError("Invalid period specified. Use 'today' or an integer number of days.")
    
    # Ensure JWT token is valid
    jwt_token = get_valid_jwt(platform = platform)

    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }

    try: 
        response = requests.get(base_url, headers=headers, params=params)
        
        if response.status_code == 401:
            print("JWT token expired. Fetching a new token...")
            jwt_token = get_new_jwt(platform=platform)
            headers["Authorization"] = f"Bearer {jwt_token}"
            response = requests.get(base_url, headers=headers, params = params)
        response.raise_for_status()
        tonic_data = response.json()

        if bool(tonic_data):
            # Conver json to pd.dataframe
            tonic_df = pd.json_normalize(tonic_data)
            aggregated_data = aggregate_tonic_data(tonic_df, platform = platform)
            print("Script executed successfully. Real-time report returned.")
            return aggregated_data
        else:
            return print("No data found")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch report: {e}")


def get_report_tonic_today(): 
    tonic_allday_tt = get_advertiser_data_tonic(period="today", platform="tiktok")
    tonic_allday_fb = get_advertiser_data_tonic(period="today", platform="facebook")
    
    tonic_data = {
        'tiktok':tonic_allday_tt,
        'facebook':tonic_allday_fb
    }
    return tonic_data
