# # To add a new cell, type '# %%'
# # To add a new markdown cell, type '# %% [markdown]'

# %%
import pandas as pd
import requests

from app import SCORESBY_WEATHER_URL, VIEWBANK_WEATHER_URL

viewbank_data = requests.get(VIEWBANK_WEATHER_URL).json().get(
    'observations').get('data')
scoresby_data = requests.get(SCORESBY_WEATHER_URL).json().get(
    'observations').get('data')
viewbank_data
viewbank_data[0:3]

# %%
dfm_viewbank = pd.DataFrame(viewbank_data)
dfm_viewbank['interval_date'] = pd.to_datetime(
    dfm_viewbank['local_date_time_full']).dt.date
df_day_viewbank = dfm_viewbank.groupby(['interval_date']).agg(
    air_temp_min_c=pd.NamedAgg(column='air_temp', aggfunc='min'),
    air_temp_max_c=pd.NamedAgg(column='air_temp', aggfunc='max'),
)
df_day_viewbank.index = pd.to_datetime(df_day_viewbank.index)
dfm_scoresby = pd.DataFrame(scoresby_data)
dfm_scoresby['interval_date'] = pd.to_datetime(
    dfm_scoresby['local_date_time_full']).dt.date
df_day_scoresby = dfm_scoresby.groupby(['interval_date']).agg(
    air_temp_min_c=pd.NamedAgg(column='air_temp', aggfunc='min'),
    air_temp_max_c=pd.NamedAgg(column='air_temp', aggfunc='max'),
)
df_day_scoresby.index = pd.to_datetime(df_day_scoresby.index)

df_day_viewbank['air_temp_min_c'].fillna(df_day_scoresby['air_temp_min_c'])
df_day_viewbank['air_temp_max_c'].fillna(df_day_scoresby['air_temp_max_c'])
df_day_viewbank


# %%
