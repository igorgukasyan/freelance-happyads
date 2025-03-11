import pandas as pd
import numpy as np
import re
import pickle
from cf_conversions_load import *
from datetime import timedelta
with open('train_data.pkl', 'rb') as d:
    train_data = pickle.load(d)
def prelim_prep(tiktok_clean, tonic_clean_tiktok, cf_conversions):
    # Merging data
    merged = (
    tiktok_clean
    .assign(
    stat_hour = pd.to_datetime(tiktok_clean['stat_time_hour']).dt.hour
    )
    .merge(tonic_clean_tiktok, how='inner', on='adgroup_id') #Inner to select those adgroups for which we know EPC
    .merge(cf_conversions, how = 'left', left_on=['adgroup_id', 'stat_hour'], right_on = ['adgroup_id', 'hourOfDay'])
    .fillna(0)
    .reset_index(drop=True)
    .assign(estimated_revenue = lambda df: df['epc']*df['cf_conversions'])
    .query('~(spend == 0 & cf_conversions == 0)')
    )
    # Adding hour of day, day of week, and is_weekend
    merged_timevars = (
    merged
    .assign(
        hour_of_day=lambda df: np.round(np.sin(df['stat_time_hour'].dt.hour * 2 * np.pi / 24), 2),
        day_of_week=lambda df: np.round(np.sin(df['stat_time_hour'].dt.weekday * 2 * np.pi / 7), 2),
        is_weekend=lambda df: (df['stat_time_hour'].dt.weekday >= 5).astype(int)
    )
    )
    # Adding adgroup phase id to track stopping/starting
    merged_phaseid = (
    merged_timevars
    .sort_values(by=['adgroup_id', 'stat_time_hour'])
    .assign(
        time_difference=lambda df: df.groupby('adgroup_id')['stat_time_hour'].diff().dt.total_seconds(),
        phase_id=lambda df: df.groupby('adgroup_id')['time_difference']
        .apply(lambda x: (x.isna() | (x > 3600)).cumsum())
        .reset_index(level = 0, drop = True)
    )
    .drop(columns=['time_difference'])
    )
    # Adding cumulative revenue AND creating a one-day window var
    merged_cummetrics = (
    merged_phaseid
    .assign(one_day_window=lambda df: df['stat_time_hour'].dt.date)
    .groupby(['advertiser_id', 'adgroup_id', 'phase_id', 'one_day_window'], as_index=False)[merged_phaseid.columns.tolist() + ['one_day_window']]
    .apply(lambda group: group.assign(cum_profit_1day=np.round(group['estimated_revenue'].cumsum() - group['spend'].cumsum(), 2)))
    .reset_index(drop=True)
    )
    # Adding country and one-hot encoding
    merged_withcountry = (
    merged_cummetrics
    .assign(
        country_code_prep=lambda df: df['campaign_name'].str[:2].replace({'CO': 'СО', 'SP': 'ES'}),
        country_code=lambda df: pd.Categorical(df['country_code_prep'])
    )
    .drop(columns=['country_code_prep'])
    )
    merged_withcountry_onehot = pd.get_dummies(merged_withcountry, columns=['country_code'], prefix='', prefix_sep='')
    merged_withoffer = (
    merged_withcountry_onehot
    .assign(
        offer = lambda df: df['campaign_name']
            .str.replace(r'.*(dmc|market|dig).*', 'dmc', regex=True, flags=re.IGNORECASE)
            .str.replace(r'.*(apart|aprt|apartment).*', 'apart', regex=True, flags=re.IGNORECASE)
            .str.replace(r'.*(wedding|ring).*', 'wedding_rings', regex=True, flags=re.IGNORECASE)
    )
    .query('offer != "WORLD"')
    )

    merged_withoffer_onehot = pd.get_dummies(merged_withoffer, columns=['offer'], prefix='', prefix_sep='').reset_index(drop=True)
    # Final dataframe
    clean_df = (
    merged_withoffer_onehot
    .assign(bid=lambda df: pd.to_numeric(df['bid'], errors='coerce'))
    .dropna()
    )
    return clean_df

def prep_logit_input(tiktok_clean, tonic_clean_tiktok, cf_conversions):
    
    clean_df = prelim_prep(tiktok_clean, tonic_clean_tiktok, cf_conversions)
    # Extracting data for last hour only
    last_available_hour = clean_df['stat_time_hour'].max()-timedelta(hours = 1)
    clean_df_lasthr = clean_df[clean_df['stat_time_hour']==last_available_hour]
    # Taking only the relevant cols
    missing_cols = [col for col in train_data.columns  if col not in clean_df_lasthr.columns]

    if missing_cols:   
        for col in missing_cols:
         clean_df_lasthr[col] = 0

    pre_final = clean_df_lasthr[train_data.columns.insert(0, 'adgroup_id')]
    pre_final = pre_final.reset_index(drop = True)
    return(pre_final)

def prep_table_input(tiktok_clean, tonic_clean_tiktok, cf_conversions):
    clean_df = prelim_prep(tiktok_clean, tonic_clean_tiktok, cf_conversions)
    table_data = clean_df.copy()
    keep_columns = ['advertiser_id', 'adgroup_id', 'cf_conversions', 'spend', 'clicks', 'impressions', 'rt_cpa',
                 'rt_conversion_rate', 'estimated_revenue', 'bid', 'campaign_name']
    table_data = table_data[keep_columns]
    table_data_grouped = (
    table_data
    .groupby(['advertiser_id', 'bid', 'adgroup_id', 'campaign_name'], as_index=False)
    .agg(
        cf_conversions = ('cf_conversions', 'sum'),
        spend = ('spend', 'sum'),
        impressions = ('impressions', 'sum'),
        rt_cpa = ('rt_cpa', lambda x: np.average(x, weights=table_data.loc[x.index, "cf_conversions"]) if table_data.loc[x.index, "cf_conversions"].sum() != 0 else 0),
        rt_conversion_rate = ('rt_conversion_rate', lambda x: np.average(x, weights=table_data.loc[x.index, "impressions"]) if table_data.loc[x.index, "impressions"].sum() != 0 else 0),
        estimated_revenue = ('estimated_revenue', 'sum'),
        clicks = ('clicks', 'sum')
    )
    .assign(
        ROI = lambda df: (df['estimated_revenue']-df['spend'])/df['spend'],
        CPA = lambda df: np.where(df['cf_conversions'] != 0, df['spend'] / df['cf_conversions'], 0),
        RPC = lambda df: np.where(df['cf_conversions'] != 0, df['estimated_revenue']/df['cf_conversions'], 0),
        CTR = lambda df: np.where(df['impressions'] != 0, df['clicks']/df['impressions'], 0),
        rt_conversion_rate_custom = lambda df: np.where(df['impressions'] != 0, df['cf_conversions']/df['impressions'], 0)
    )
    .sort_values(['advertiser_id', 'adgroup_id'])
    )
    return table_data_grouped
