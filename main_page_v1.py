## Load packages and data
from report_tiktok import *
from report_tonic import *
from data_prep import *
from cf_conversions_load import *
from collect_active_adgroups import *
import pickle
import re
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta, timezone
import os
from dotenv import load_dotenv
import pytz
from urllib.parse import quote
import streamlit as st # type: ignore
import streamlit_authenticator as stauth
import yaml
import streamlit_analytics
from yaml.loader import SafeLoader
with open('realtime_logit_nexthr.pkl', 'rb') as f:
    logit = pickle.load(f)
with open('./config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

## Load packages and data

#------------------------------------------------------------------------------------------------------------------------------------

## Page setup (must be first streamlit command)
st.set_page_config(page_title="Buyer Terminal",
                   page_icon="🧊", 
                   layout="wide")
## Page setup (must be first streamlit command)

#------------------------------------------------------------------------------------------------------------------------------------

### Writing down functions
## Func to save last run time
@st.cache_data(ttl='10m')
def save_last_run_time():
    return datetime.now()

## Func to get data & cache
@st.cache_data(ttl='10m', show_spinner="Загружаю данные...")
def get_main_data():
    tiktok_clean = get_report_tiktok_today()
    tonic_clean = get_report_tonic_today()
    tonic_clean_tiktok = tonic_clean['tiktok']
    cf_conversions = get_all_conversions()
    active_adgroups = get_all_active_adgroups()
    if tonic_clean_tiktok.bool:
        
        logit_input = prep_logit_input(tiktok_clean, tonic_clean_tiktok, cf_conversions)
        table_input = prep_table_input(tiktok_clean, tonic_clean_tiktok, cf_conversions)

        preds = np.where(logit.predict_proba(logit_input.drop(columns = ['adgroup_id']))[:, 0] <= 0.4952382, "✅", "❌")
        preds = pd.concat([pd.DataFrame(preds), logit_input['adgroup_id']], axis = 1, ignore_index= True)
        preds.columns = ['prediction', 'adgroup_id']
        table_input_with_preds = (
            table_input
            .merge(preds, how = 'left', on = 'adgroup_id')
            .fillna("➖")
        )
        table_input_with_preds['CTR'] = table_input_with_preds['CTR']*100
        table_input_with_preds['rt_conversion_rate_custom'] = table_input_with_preds['rt_conversion_rate_custom']*100
        table_input_with_preds['ROI'] = pd.to_numeric(table_input_with_preds['ROI'], errors='coerce')*100
        table_input_with_preds = table_input_with_preds.merge(tiktok_accounts, how = 'left', on = 'advertiser_id')
        table_input_with_preds = table_input_with_preds.rename(columns = {
            "bid": "Bid",
            "rt_conversion_rate_custom":"Conversion Rate",
            "clicks": "Clicks",
            "estimated_revenue": "estRevenue",
            "spend": "Spend",
            "impressions": "Impressions",
            "cf_conversions": "Conversions",
            "campaign_name": "Campaign Name",
            "prediction": "Совет"
        })
        return {'table_data': table_input_with_preds,
                'active_adgroups': active_adgroups}
    else: 
        st.header(body="Данных за сегодня еще нет!")
        st.image("https://i.redd.it/4wel1xmq3wq21.jpg",
                 caption="Грустно...")

## Function to highlight rows
def highlight_row(s):
    # Initialize the row_style list to store the styles for each column
    row_style = []
    
    # Loop through each column (excluding column 13 which is 'Совет')
    for i, col in enumerate(s.index):
        if i == 16:
            if s['Совет'] == "❌":
                row_style.append('background-color: #F7C4B3;')
            elif s['Совет'] == "✅":
                row_style.append('background-color: #9DC183;')
            else:
                row_style.append('background-color: #fff5ba;')
        else:
            # Color the other columns based on ROI
            if s.ROI == -1:
                row_style.append('background-color: #fff5ba;')
            else:
                if s.ROI > 0:
                    row_style.append('background-color: #9DC183;')
                else:
                    row_style.append('background-color: #F7C4B3;')

    return row_style

## Format rows with INACTIVE adgroups
def highlight_inactive_adgroups(row):
    if row['is_active'] == False:
        return ['color: #8C92AC;'] * len(row)
    else:
        return [''] * len(row)
    
### Writing down functions

#------------------------------------------------------------------------------------------------------------------------------------

## Authenticate users
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

try:
    authenticator.login()
except Exception as e:
    st.error(e)
streamlit_analytics.start_tracking(load_from_json='./user_analytics.json')
if st.session_state['authentication_status']:
    ### Initializing the content
    ## Fetching buyer table
    #authenticator.logout()
    st.title('Терминал')
    main_data = get_main_data()
    fetched_buyer_table = main_data.get('table_data')
    active_adgroups = main_data.get('active_adgroups')
    active_adgroups = active_adgroups['adgroup_id']
    fetched_buyer_table['is_active'] = fetched_buyer_table['adgroup_id'].isin(active_adgroups)

    ## Time of rerun init
    last_run_time = save_last_run_time()

    col1, col2 = st.columns([1, 3], vertical_alignment="bottom")
    with col1:
        ## Button to force rerun
        if st.button("Обновить таблицу", use_container_width=True):
            get_main_data.clear()
            save_last_run_time.clear()
            last_run_time = save_last_run_time()
            main_data = get_main_data()
            fetched_buyer_table = main_data.get('table_data')
            fetched_buyer_table['is_active'] = fetched_buyer_table['adgroup_id'].isin(active_adgroups)
            active_adgroups = main_data.get('active_adgroups')

    with col2:
        ## Selection of the advertiser
        show_advertiser = st.selectbox(
            label="Выбранный аккаунт:",
            index=0,
            options=fetched_buyer_table['username'].unique(),
            key='advertiser_selection',
            placeholder="Выбери аккаунт",
            label_visibility="collapsed"
        )

    ## Filtering of data to be shown
    buyer_data_show = fetched_buyer_table[fetched_buyer_table["username"] == show_advertiser].sort_values(by='Spend', ascending = False)
    buyer_data_show['Bid'] = np.where(buyer_data_show['Bid'] == 0, '', buyer_data_show['Bid'])
    ##Style
    buyer_data_styled = (buyer_data_show.style
                        .apply(highlight_row, axis=1)
                        .apply(highlight_inactive_adgroups, axis=1))
    
                         
    ## Show last run time
    st.write(f"*<span style='color: #gray;'>Последнее обновление: {last_run_time.strftime('%Y-%m-%d %H:%M:%S')}</span>*", unsafe_allow_html=True)
    ## Format the data table
    st.dataframe(buyer_data_styled,
                column_order = ("Campaign Name", "adgroup_id", "Spend",
                                 "estRevenue", "ROI","RPC","CPA",
                                 "Bid", "Impressions","Clicks",
                                 "Conversions", "CTR", "Conversion Rate", 'is_active', "Совет"),
                column_config={
                    "Campaign Name": st.column_config.TextColumn(
                        'Campaign',
                        width=120
                    ),
                    "adgroup_id": st.column_config.TextColumn(
                        "Adgroup",
                        width=100
                    ),
                    "ROI": st.column_config.NumberColumn(
                        format = '%.1f%%',
                        help='Profit/Spend',
                        width=30
                    ),
                    "CPA": st.column_config.NumberColumn(
                        format = '$%.2f',
                        help='Spend/Conversions, т.е. цена за конверсию',
                        width=30
                    ),
                    "RPC": st.column_config.NumberColumn(
                        format = '$%.2f',
                        help='Revenue/Conversions, т.е. выручка за конверсию',
                        width=25
                    ),
                    "CTR": st.column_config.NumberColumn(
                        format = '%.1f%%',
                        help='Clicks/Impressions, т.е. процент просмотров с кликом',
                        width=25
                    ),
                    "Conversion Rate": st.column_config.NumberColumn(
                        "CR",
                        format = '%.1f%%',
                        help="Conversion Rate = Conversions/Impressions, т.е. процент просмотров, которые стали конверсией",
                        width=25
                    ), 
                    "Conversions": st.column_config.NumberColumn(
                        format = '%.0f',
                        help='Финальная конверсия',
                        width=70
                    ),
                    "Clicks": st.column_config.NumberColumn(
                        format = '%.0f',
                        help='Клики по нашей ссылке под',
                        width=30
                    ),
                    "estRevenue": st.column_config.NumberColumn(
                        format = '$%.2f',
                        width=50
                    ),
                    "Spend": st.column_config.NumberColumn(
                        format = '$%.2f',
                    ),
                    "Impressions": st.column_config.NumberColumn(
                        format = '%.0f',
                        width=60
                    ),
                    "Bid": st.column_config.NumberColumn(
                        format = '$%.2f',
                        width=30,
                        default = "Auto"
                    ),
                    "Spend": st.column_config.NumberColumn(
                        format = '%.2f',
                        width=35
                    ),
                    "Conversions": st.column_config.NumberColumn(
                        format = '%.0f',
                        width=50
                    ),
                    "is_active": st.column_config.CheckboxColumn(
                        "Статус"
                    ),
                    "Совет": st.column_config.TextColumn(
                        "⚡",
                        width=10
                    )
                },
                hide_index=True,
                use_container_width = True,
                height = 20*36
                )
    streamlit_analytics.stop_tracking(save_to_json="./user_analytics.json")
elif st.session_state['authentication_status'] is False:
    st.error('Username/password is incorrect')
elif st.session_state['authentication_status'] is None:
    st.warning('Please enter your username and password')

## Authenticate users

#------------------------------------------------------------------------------------------------------------------------------------

