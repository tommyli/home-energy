# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Analysis of energy data
#
# ## Getting Started
#
# First download pickle file from GCP storage bucket.  The structure is indexed
# on interval_date and each column is an array of 48 values, each value representing
# each half hour starting at midnight.

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
MEDIUM_FIGSIZE = (12, 9)
LARGE_FIGSIZE = (16, 12)
STORAGE_CLIENT = storage.Client()
bucket = STORAGE_CLIENT.get_bucket(GCP_STORAGE_BUCKET_ID)
pkl_file_name = f"dailies_{NMI}.pkl"
blob = storage.Blob(pkl_file_name, bucket)
pkl_file_path = f"/tmp/{pkl_file_name}"

with open(pkl_file_path, 'wb') as pkl_file:
    STORAGE_CLIENT.download_blob_to_file(blob, pkl_file)

df = pd.read_pickle(pkl_file_path)
os.remove(pkl_file_path)
df

# %% [markdown]
# ## Preparing the data
#
# Nested arrays in a dataframe are a bit difficult to use, let's
# unnest them into 48 rows of values.

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

# df_energy = df_energy[:'2019-11-30']
df_energy_hh

# %% [markdown]
# We can now derive new data based on existing raw data in dataframe.
#
# ### Gross Usage
#
# Gross usage is what the usage would be as measured by the meter if no solar nor battery exists.
# This provides a good baseline to compare how the household uses energy.
# The usage measured at the meter is no longer accurate for households with either solar and/or
# batteries installed so this would have to be calculated.
#
# `gross_usage_kwh` = `meter_consumption_kwh + (solar_generation_kwh - meter_generation_kwh - charge_quantity_kwh) + discharge_quantity_kwh`
#
# ### Self Consumption
#
# Self consumption is the amount solar energy used within the household, whether
# that is used during sunlight hours or stored in the battery for later use.
#
# `self_consumption_kwh` = `solar_generation_kwh - meter_generation_kwh - charge_quantity_kwh + discharge_quantity_kwh`


# %%
df_energy_hh['gross_usage_kwh'] = df_energy_hh['meter_consumption_kwh'] + \
    (df_energy_hh['solar_generation_kwh'] - df_energy_hh['meter_generation_kwh'] -
     df_energy_hh['charge_quantity_kwh']) + df_energy_hh['discharge_quantity_kwh']
df_energy_hh['self_consumption_kwh'] = df_energy_hh['solar_generation_kwh'] - \
    df_energy_hh['meter_generation_kwh'] - df_energy_hh['charge_quantity_kwh'] + \
    df_energy_hh['discharge_quantity_kwh']
df_energy_hh

# %% [markdown]

# Also group data at day level and join to daily min and max temperatures.

# %%
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
    self_consumption_kwh=pd.NamedAgg(
        column='self_consumption_kwh', aggfunc='mean'),
)

df_min_max = df[['min_temperature_c', 'max_temperature_c']].astype('float')
df_energy_daily = df_energy_daily.join(df_min_max)
df_energy_daily

# %% [markdown]
# ## Comparing Workdays and Non-workdays
#
# Thank goodness for the Python library [Workalendar](https://peopledoc.github.io/workalendar/)
# for maintaining a list of public holidays for G20 countries.  For Australia, it even
# provides state specific holidays and handles 'observed' days if holidays falls on a weekend.

# %%
vic_cal = VicHolidayCalendar()
holidays = pd.to_datetime(list(flatten([[date for date, name in vic_cal.holidays(
    year)] for year in range(2000, datetime.now().year)])))
holidays_mask = df_energy_hh.index.isin(holidays)
weekend_mask = df_energy_hh.index.dayofweek.isin([5, 6])
non_workday_mask = holidays_mask | weekend_mask
df_non_workdays = df_energy_hh.loc[non_workday_mask]
df_workdays = df_energy_hh.loc[~non_workday_mask]

df_workdays_hh_mean = df_workdays.groupby(['interval']).agg(
    gross_usage_kwh=pd.NamedAgg(column='gross_usage_kwh', aggfunc='mean'),
)
df_non_workdays_hh_mean = df_non_workdays.groupby(['interval']).agg(
    gross_usage_kwh=pd.NamedAgg(column='gross_usage_kwh', aggfunc='mean'),
)

title = 'Gross Usage HH Mean - workdays vs. holidays'
x = np.linspace(1, 48, num=48)
fig, axes = plt.subplots(figsize=MEDIUM_FIGSIZE)
axes.plot(x, df_workdays_hh_mean['gross_usage_kwh'], label='workdays')
axes.plot(x, df_non_workdays_hh_mean['gross_usage_kwh'], label='non-workdays')
axes.set_xlabel('intervals')
axes.set_ylabel('kWh')
axes.set_title(title)
axes.legend()

# %% [markdown]
# ## Comparing seasons
#
# ### Gross Usage Across Seasons

# %%
seasons = [
    {'name': 'Summer', 'months': [12, 1, 2]},
    {'name': 'Autumn', 'months': [3, 4, 5]},
    {'name': 'Winter', 'months': [6, 7, 8]},
    {'name': 'Spring', 'months': [9, 10, 11]},
]

seasons_data = [{'dfm': df_energy_hh.loc[df_energy_hh.index.month.isin(
    season.get('months'))], **season} for season in seasons]

title = 'Gross Usage HH Mean - Seasons'
x = np.linspace(1, 48, num=48)
fig, axes = plt.subplots(figsize=MEDIUM_FIGSIZE)

for season_data in seasons_data:
    dfm = season_data.get('dfm')
    df_hh_mean = dfm.groupby(['interval']).agg(
        gross_usage_kwh=pd.NamedAgg(column='gross_usage_kwh', aggfunc='mean'),
        meter_consumption_kwh=pd.NamedAgg(
            column='meter_consumption_kwh', aggfunc='mean'),
    )
    axes.plot(x, df_hh_mean['gross_usage_kwh'],
              label=f"{season_data.get('name')} - Gross")
    axes.plot(x, df_hh_mean['meter_consumption_kwh'],
              label=f"{season_data.get('name')} - Net")

axes.set_xlabel('intervals')
axes.set_ylabel('kWh')
axes.set_title(title)
axes.legend()

# %% [markdown]
# ### Solar Generations Across Seasons

# %%
title = 'Solar HH Mean - Seasons'
x = np.linspace(1, 48, num=48)
fig, axes = plt.subplots(figsize=MEDIUM_FIGSIZE)

for season_data in seasons_data:
    dfm = season_data.get('dfm')
    df_hh_mean = dfm.groupby(['interval']).agg(
        solar_generation_kwh=pd.NamedAgg(
            column='solar_generation_kwh', aggfunc='mean'),
        meter_generation_kwh=pd.NamedAgg(
            column='meter_generation_kwh', aggfunc='mean'),
    )
    axes.plot(x, df_hh_mean['solar_generation_kwh'],
              label=f"{season_data.get('name')} - Gross")
    axes.plot(x, df_hh_mean['meter_generation_kwh'],
              label=f"{season_data.get('name')} - Net")

axes.set_xlabel('intervals')
axes.set_ylabel('kWh')
axes.set_title(title)
axes.legend()

# %% [markdown]
# ## Comparing different event periods
#
# Here, we define the periods we want to compare and create list of event periods (date ranges) as plot data.
#
#

# %%
periods = [
    {'name': 'Epoch', 'actual_start': '2014-11-26', 'actual_end': '2015-04-06'},
    {'name': 'Installed Solar 3 kW',
        'actual_start': '2015-04-07', 'actual_end': '2016-08-14'},
    {'name': 'Installed Battery 8 kW',
        'actual_start': '2016-08-14', 'actual_end': '2016-10-17'},
    {'name': 'Upgraded to Solar 6 kW',
        'actual_start': '2016-10-18', 'actual_end': '2019-11-30'},
    {'name': 'Purchased EV', 'actual_start': '2019-12-01', 'actual_end': '2020-03-22'},
    {'name': 'Red EV Plan', 'actual_start': '2020-03-23', 'actual_end': '2020-12-31'},
]

periods_data = []

for p in periods:
    period_data = {**p}
    date_mask = (df_energy_hh.index >= p.get('actual_start')) & (
        df_energy_hh.index <= p.get('actual_end'))
    df_period = df_energy_hh.loc[date_mask]
    df_hh_mean = df_period.groupby(['interval']).agg(
        meter_consumptions_kwh=pd.NamedAgg(
            column='meter_consumption_kwh', aggfunc='mean'),
        meter_generations_kwh=pd.NamedAgg(
            column='meter_generation_kwh', aggfunc='mean'),
        solar_generations_kwh=pd.NamedAgg(
            column='solar_generation_kwh', aggfunc='mean'),
        charge_quantity_kwh=pd.NamedAgg(
            column='charge_quantity_kwh', aggfunc='mean'),
        discharge_quantity_kwh=pd.NamedAgg(
            column='discharge_quantity_kwh', aggfunc='mean'),
        gross_usage_kwh=pd.NamedAgg(column='gross_usage_kwh', aggfunc='mean'),
    )
    period_data['df_hh_mean'] = df_hh_mean
    periods_data.append(period_data)


# %%
for period_data in periods_data:
    df_hh_mean = period_data['df_hh_mean']
    if len(df_hh_mean.index) == 0:
        logger.warning(
            "Skipping period '%s' because there's no data.", period_data.get('name'))
        continue

    title = f"HH Mean ({period_data.get('name')}) - {period_data.get('actual_start')} to {period_data.get('actual_end')}"
    x = np.linspace(1, 48, num=48)
    fig, axes = plt.subplots(figsize=MEDIUM_FIGSIZE)
    axes.plot(x, df_hh_mean['meter_consumptions_kwh'], label='meter_con')
    axes.plot(x, df_hh_mean['meter_generations_kwh'], label='meter_gen')
    axes.plot(x, df_hh_mean['solar_generations_kwh'], label='solar_gen')
    axes.plot(x, df_hh_mean['charge_quantity_kwh'], label='battery_charge')
    axes.plot(x, df_hh_mean['discharge_quantity_kwh'],
              label='battery_discharge')
    axes.plot(x, df_hh_mean['gross_usage_kwh'], label='gross_usage')
    axes.set_xlabel('intervals')
    axes.set_ylabel('kWh')
    axes.set_title(title)
    axes.legend()


# %% [markdown]
# ## Gross vs. Net - Weekly and Monthly

# %%
df_energy_weekly = df_energy_daily.groupby(df_energy_daily.index.to_period('W')).agg(
    gross_usage_kwh=pd.NamedAgg(column='gross_usage_kwh', aggfunc='sum'),
    meter_consumption_kwh=pd.NamedAgg(
        column='meter_consumption_kwh', aggfunc='sum'),
    solar_generation_kwh=pd.NamedAgg(
        column='solar_generation_kwh', aggfunc='sum'),
    meter_generation_kwh=pd.NamedAgg(
        column='meter_generation_kwh', aggfunc='sum'),
)
x = df_energy_weekly.index.strftime('%Y-W%W')
y_gross_usage = df_energy_weekly['gross_usage_kwh']
y_net_usage = df_energy_weekly['meter_consumption_kwh']
y_solar_gen = df_energy_weekly['solar_generation_kwh'] * -1
y_meter_gen = df_energy_weekly['meter_generation_kwh'] * -1

title = f"Weekly Gross vs. Net"
fig, axes = plt.subplots(figsize=LARGE_FIGSIZE)
plt.xticks(rotation='vertical')
axes.bar(x=x,
         height=y_gross_usage, label='Gross Usage', align='edge')
axes.bar(x=x,
         height=y_net_usage, label='Net Usage', align='edge')
axes.bar(x=x,
         height=y_solar_gen, label='Gross Generation', align='edge')
axes.bar(x=x,
         height=y_meter_gen, label='Net Generation', align='edge')
axes.set_xlabel('Time')
for idx, label in enumerate(axes.xaxis.get_ticklabels()):
    if idx % 4 != 0:
        label.set_visible(False)

axes.set_ylabel('Usage kWh')
axes.set_title(title)
axes.legend()

# %%
df_energy_monthly = df_energy_daily.groupby(df_energy_daily.index.to_period('M')).agg(
    gross_usage_kwh=pd.NamedAgg(column='gross_usage_kwh', aggfunc='sum'),
    meter_consumption_kwh=pd.NamedAgg(
        column='meter_consumption_kwh', aggfunc='sum'),
    solar_generation_kwh=pd.NamedAgg(
        column='solar_generation_kwh', aggfunc='sum'),
    meter_generation_kwh=pd.NamedAgg(
        column='meter_generation_kwh', aggfunc='sum'),
)

x = df_energy_monthly.index.strftime('%Y-%m')
y_gross_usage = df_energy_monthly['gross_usage_kwh']
y_net_usage = df_energy_monthly['meter_consumption_kwh']
y_solar_gen = df_energy_monthly['solar_generation_kwh'] * -1
y_meter_gen = df_energy_monthly['meter_generation_kwh'] * -1

title = f"Monthly Gross vs. Net"
fig, axes = plt.subplots(figsize=LARGE_FIGSIZE)
plt.xticks(rotation='vertical')
axes.bar(x=x,
         height=y_gross_usage, label='Gross Usage', align='edge')
axes.bar(x=x,
         height=y_net_usage, label='Net Usage', align='edge')
axes.bar(x=x,
         height=y_solar_gen, label='Gross Generation', align='edge')
axes.bar(x=x,
         height=y_meter_gen, label='Net Generation', align='edge')
axes.set_xlabel('Time')
axes.set_ylabel('Usage kWh')
axes.set_title(title)
axes.legend()

# %% [markdown]

# ## Temperature Impacts

# %%

title = f"Daily Gross Usage vs. Daily min and max Temperatures"
fig, axes = plt.subplots(figsize=MEDIUM_FIGSIZE)
y = df_energy_daily['gross_usage_kwh']
axes.scatter(x=df_energy_daily['min_temperature_c'],
             y=y, label='Day min')
axes.scatter(x=df_energy_daily['max_temperature_c'],
             y=y, label='Day max')
axes.set_xlabel('Temperature C')
axes.set_ylabel('Day Gross Usage kWh')
axes.set_title(title)
axes.grid(True)
axes.legend()

# %% [markdown]

# There seems to be some non-linear trend.  As temperature increases, gross usage increases.
# However, this trend reverses around 22 degrees (just based on visuals) where gross usage
# increases as temperature decreases.

# TODO - R&D on calculating and plotting non-linear trendlines.

# %%
