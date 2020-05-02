import pandas as pd
import requests

from app import (NMI, SCORESBY_WEATHER_URL, VIEWBANK_WEATHER_URL,
                 init_gcp_logger)
from app.common import merge_df_to_db


def daily_temperatures_to_db(root_collection_name):
    gcp_logger = init_gcp_logger()
    viewbank_data = requests.get(VIEWBANK_WEATHER_URL).json().get(
        'observations').get('data')
    scoresby_data = requests.get(SCORESBY_WEATHER_URL).json().get(
        'observations').get('data')

    dfm_viewbank = pd.DataFrame(viewbank_data)
    dfm_viewbank['interval_date'] = pd.to_datetime(
        dfm_viewbank['local_date_time_full']).dt.date
    df_day_viewbank = dfm_viewbank.groupby(['interval_date']).agg(
        min_temperature_c=pd.NamedAgg(column='air_temp', aggfunc='min'),
        max_temperature_c=pd.NamedAgg(column='air_temp', aggfunc='max'),
    )
    df_day_viewbank.index = pd.to_datetime(df_day_viewbank.index)
    dfm_scoresby = pd.DataFrame(scoresby_data)
    dfm_scoresby['interval_date'] = pd.to_datetime(
        dfm_scoresby['local_date_time_full']).dt.date
    df_day_scoresby = dfm_scoresby.groupby(['interval_date']).agg(
        min_temperature_c=pd.NamedAgg(column='air_temp', aggfunc='min'),
        max_temperature_c=pd.NamedAgg(column='air_temp', aggfunc='max'),
    )
    df_day_scoresby.index = pd.to_datetime(df_day_scoresby.index)

    df_day_viewbank['min_temperature_c'].fillna(
        df_day_scoresby['min_temperature_c'])
    df_day_viewbank['max_temperature_c'].fillna(
        df_day_scoresby['max_temperature_c'])

    merge_df_to_db(NMI, df_day_viewbank, root_collection_name, gcp_logger)
# %%
