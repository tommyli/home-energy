# # To add a new cell, type '# %%'
# # To add a new markdown cell, type '# %% [markdown]'

# %%

from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from google.cloud import storage

from app import (GCP_STORAGE_BUCKET_ID, NEM12_DATA_MIN_DATE, NMI,
                 SCORESBY_WEATHER_URL, VIEWBANK_WEATHER_URL,
                 init_firestore_client, init_gcp_logger, init_storage_client)
from app.common import merge_df_to_db

viewbank_data = requests.get(VIEWBANK_WEATHER_URL).json().get(
    'observations').get('data')
scoresby_data = requests.get(SCORESBY_WEATHER_URL).json().get(
    'observations').get('data')
viewbank_data
viewbank_data[0:3]

# %% [markdown]

# ## Fetch daily temperatures of the past few days from BOM

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

# %% [markdown]

# ## Backload historical daily temperatures
# The data was manually downloaded from [BOM Weather Station Directory](http://www.bom.gov.au/climate/data/stations/)

# %%


def _storage_csv_to_df(filename, bucket, csv_column_name, df_column_name, min_date):
    blob = storage.Blob(
        f"weather/{filename}", bucket)
    df_result = None
    with open(f"/tmp/{filename}", 'wb') as file_obj:
        csv = blob.download_as_string().decode('utf-8')
        df_result = pd.read_csv(StringIO(csv))

    df_result['interval_date'] = pd.to_datetime(
        df_result[['Year', 'Month', 'Day']])
    df_result.index = pd.DatetimeIndex(df_result['interval_date'])
    df_result = df_result.loc[df_result.index >= min_date]
    df_result.rename(
        columns={csv_column_name: df_column_name}, inplace=True)
    df_result = df_result.loc[:, [df_column_name]]

    return df_result


def _min_max_storage_csv_to_df(min_max_filename_pair, bucket, min_date):
    min_file_name, max_file_name = min_max_filename_pair
    df_min = _storage_csv_to_df(
        min_file_name, bucket, 'Minimum temperature (Degree C)', 'min_temperature_c', min_date)
    df_max = _storage_csv_to_df(
        max_file_name, bucket, 'Maximum temperature (Degree C)', 'max_temperature_c', min_date)
    df_result = df_min.join(df_max)

    return df_result


# %%
scoresby_min_filename = 'SCORESBY_MIN_TEMP_IDCJAC0011_086104_1800_Data.csv'
scoresby_max_filename = 'SCORESBY_MAX_TEMP_IDCJAC0010_086104_1800_Data.csv'
viewbank_min_filename = 'VIEWBANK_MIN_TEMP_IDCJAC0011_086068_1800_Data.csv'
viewbank_max_filename = 'VIEWBANK_MAX_TEMP_IDCJAC0010_086068_1800_Data.csv'

storage_client = init_storage_client()
bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)
nem12_min_date = datetime.strptime(NEM12_DATA_MIN_DATE, '%Y-%m-%d')

df_viewbank = _min_max_storage_csv_to_df(
    (viewbank_min_filename, viewbank_max_filename), bucket, nem12_min_date)
df_scoresby = _min_max_storage_csv_to_df(
    (scoresby_min_filename, scoresby_max_filename), bucket, nem12_min_date)

df_viewbank['min_temperature_c'].fillna(
    df_scoresby['min_temperature_c'], inplace=True)
df_viewbank['max_temperature_c'].fillna(
    df_scoresby['max_temperature_c'], inplace=True)
df_viewbank

# %%
merge_df_to_db(NMI, df_viewbank, 'sites', init_gcp_logger())


# %%
