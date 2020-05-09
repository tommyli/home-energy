# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Home Energy Data Quality Analysis
#
# Discover any anomalies in the data.
#
# TODO - de-duplicate prepartion code that fetches data from Firestore DB
# and normalise to half hourly and daily dataframes

# %%
import logging
import os
import sys
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from google.cloud import storage
from more_itertools import flatten
from workalendar.oceania.australia import Victoria as VicHolidayCalendar

logger = logging.getLogger()
GCP_STORAGE_BUCKET_ID = os.environ.get(
    'GCP_STORAGE_BUCKET_ID', 'GCP_STORAGE_BUCKET_ID not set.')
NMI = os.environ.get(
    'NMI', 'NMI not set.')

STORAGE_CLIENT = storage.Client()
bucket = STORAGE_CLIENT.get_bucket(GCP_STORAGE_BUCKET_ID)
pkl_file_name = f"dailies_{NMI}.pkl"
blob = storage.Blob(pkl_file_name, bucket)
pkl_file_path = f"/tmp/{pkl_file_name}"

with open(pkl_file_path, 'wb') as pkl_file:
    STORAGE_CLIENT.download_blob_to_file(blob, pkl_file)

df = pd.read_pickle(pkl_file_path)
os.remove(pkl_file_path)
# %%
meter_cons = np.array(df['meter_consumptions_kwh'].values.tolist())
meter_gens = np.array(df['meter_generations_kwh'].values.tolist())
solar_gens = np.array(df['solar_generations_kwh'].values.tolist())
solar_powrs = np.array(df['solar_mean_powrs_kw'].values.tolist())
solar_devices = np.array(df['solar_devices_reportings'].values.tolist())
capacities = np.array(df['capacities_kw'].values.tolist())
charge_quantities = np.array(df['charge_quantities_kwh'].values.tolist())
deterioration_states = np.array(df['deterioration_states_pct'].values.tolist())
discharge_quantities = np.array(df['discharge_quantities_kwh'].values.tolist())
power_at_charges = np.array(df['power_at_charges_kw'].values.tolist())
residual_capacities = np.array(df['residual_capacities_pct'].values.tolist())
total_charge_quantities = np.array(
    df['total_charge_quantities_kwh'].values.tolist())
total_discharge_quantities = np.array(
    df['total_discharge_quantities_kwh'].values.tolist())
interval_dates = np.repeat(df.index.tolist(), meter_cons.shape[1])

numeric_columns = ('meter_consumption_kwh', 'meter_generation_kwh',
                   'solar_generation_kwh', 'solar_mean_powr_kw', 'solar_devices_reporting',
                   'capacity_kw', 'charge_quantity_kwh', 'deterioration_state_pct',
                   'discharge_quantity_kwh', 'power_at_charge_kw', 'residual_capacity_pct',
                   'total_charge_quantity_kwh', 'total_discharge_quantity_kwh'
                   )

columns = ('interval_date',) + numeric_columns

df_energy_hh = pd.DataFrame(np.column_stack((
    interval_dates,
    meter_cons.ravel(),
    meter_gens.ravel(),
    solar_gens.ravel(),
    solar_powrs.ravel(),
    solar_devices.ravel(),
    capacities.ravel(),
    charge_quantities.ravel(),
    deterioration_states.ravel(),
    discharge_quantities.ravel(),
    power_at_charges.ravel(),
    residual_capacities.ravel(),
    total_charge_quantities.ravel(),
    total_discharge_quantities.ravel(),
)),
    columns=columns,
)
df_energy_hh.reset_index(inplace=True)
df_energy_hh.rename(columns={'index': 'interval'}, inplace=True)
df_energy_hh['interval'] = (df_energy_hh['interval'] % 48) + 1
df_energy_hh.set_index('interval_date', inplace=True)

for nc in numeric_columns:
    df_energy_hh[nc] = pd.to_numeric(df_energy_hh[nc])

df_energy_hh['gross_usage_kwh'] = df_energy_hh['meter_consumption_kwh'] + \
    (df_energy_hh['solar_generation_kwh'] - df_energy_hh['meter_generation_kwh'] -
     df_energy_hh['charge_quantity_kwh']) + df_energy_hh['discharge_quantity_kwh']

df_energy_daily = df_energy_hh.groupby(['interval_date']).agg(
    meter_consumption_kwh=pd.NamedAgg(
        column='meter_consumption_kwh', aggfunc='mean'),
    meter_generation_kwh=pd.NamedAgg(
        column='meter_generation_kwh', aggfunc='mean'),
    solar_generation_kwh=pd.NamedAgg(
        column='solar_generation_kwh', aggfunc='mean'),
    solar_mean_powr_kw=pd.NamedAgg(
        column='solar_mean_powr_kw', aggfunc='mean'),
    solar_devices_reporting=pd.NamedAgg(
        column='solar_devices_reporting', aggfunc='median'),
    capacity_kw=pd.NamedAgg(
        column='capacity_kw', aggfunc='mean'),
    charge_quantity_kwh=pd.NamedAgg(
        column='charge_quantity_kwh', aggfunc='mean'),
    discharge_quantity_kwh=pd.NamedAgg(
        column='discharge_quantity_kwh', aggfunc='mean'),
    deterioration_state_pct=pd.NamedAgg(
        column='deterioration_state_pct', aggfunc='mean'),
    power_at_charge_kw=pd.NamedAgg(
        column='power_at_charge_kw', aggfunc='mean'),
    residual_capacity_pct=pd.NamedAgg(
        column='residual_capacity_pct', aggfunc='mean'),
    total_charge_quantity_kwh=pd.NamedAgg(
        column='total_charge_quantity_kwh', aggfunc='mean'),
    total_discharge_quantity_kwh=pd.NamedAgg(
        column='total_discharge_quantity_kwh', aggfunc='mean'),
    gross_usage_kwh=pd.NamedAgg(
        column='gross_usage_kwh', aggfunc='mean'),
)

df_min_max = df[['min_temperature_c', 'max_temperature_c']].astype('float')
df_energy_daily = df_energy_daily.join(df_min_max)
df_energy_daily

# %% [markdown]
# All gross_usage_kwh must be positive, is this true?
# %%
df_negative_gross_usage = df_energy_daily.loc[df_energy_daily['gross_usage_kwh']
                                              < 0.0]
df_negative_gross_usage

# %% [markdown]
# All gross_usage_kwh must be greater than meter_consumption_kwh, is this true?
# %%
df_error = df_energy_daily.loc[df_energy_daily['gross_usage_kwh']
                               < df_energy_daily['meter_consumption_kwh']]
df_error

# %%
df_solar_error = df_error.loc[df_error['solar_generation_kwh']
                              < df_error['meter_generation_kwh']]
df_solar_error

# %%
df_20150820 = df_energy_hh.loc['2015-08-20']
df_error_20150820 = df_20150820.loc[df_20150820['gross_usage_kwh']
                                    <= df_20150820['meter_consumption_kwh']]
df_error_20150820
df_error_20150820.drop(['capacity_kw', 'charge_quantity_kwh',
                        'discharge_quantity_kwh', 'deterioration_state_pct', 'power_at_charge_kw', 'residual_capacity_pct', 'total_charge_quantity_kwh', 'total_discharge_quantity_kwh'], axis='columns', inplace=True)
df_error_20150820
# %% [markdown]
# Not many but 24 days has issues.  Looking at the errors, the solar_generation_kwh seems to be the issue.
# Of the 24 days, 23 of them had solar_generation_kwh less than meter_generation_kwh which should not be possible.
# Digging deeper into each half hour for 2015-08-20 reveals the solar_generation_kwh is too uniform.  It's most likely
# the errors are caused when the solar panels were not reporting correctly and an average value was provided as an estimate.

# What about 2016-12-27 and 2016-12-28?

# %%
df_20161227 = df_energy_hh.loc['2016-12-27']
df_20161228 = df_energy_hh.loc['2016-12-28']
df_20161227

# %%
for dfm in [df_20161227, df_20161228]:
    x = np.linspace(1, 48, num=48)
    fig, axes = plt.subplots(figsize=(20, 10))
    axes.plot(x, dfm['gross_usage_kwh'], label='gross_usage_kwh')
    axes.plot(x, dfm['meter_consumption_kwh'],
              label='meter_consumption_kwh')
    axes.plot(x, dfm['solar_generation_kwh'], label='solar_generation_kwh')
    axes.plot(x, dfm['meter_generation_kwh'], label='meter_generation_kwh')
    axes.plot(x, dfm['charge_quantity_kwh'], label='charge_quantity_kwh')
    axes.plot(x, dfm['discharge_quantity_kwh'],
              label='discharge_quantity_kwh')
    axes.legend()


# %% [markdown]

# Something weird is going on these two days.  Turns out for these two days
# Red Energy (my energy retailer) was testing battery discharge remotely.
# It explains why these two days of data seems a bit inconsistent but
# not sure how.  E.g. shouldn't the discharge quantiy still balance?
# TODO - further investigation required.
