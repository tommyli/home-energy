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
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from google.cloud import storage
from more_itertools import flatten
from mpl_toolkits.axes_grid1 import make_axes_locatable
from workalendar.oceania.australia import Victoria as VicHolidayCalendar

# %%
logger = logging.getLogger()
GCP_STORAGE_BUCKET_ID = os.environ.get(
    'GCP_STORAGE_BUCKET_ID', 'GCP_STORAGE_BUCKET_ID not set.')
NMI = os.environ.get(
    'NMI', 'NMI not set.')
MEDIUM_FIGSIZE = (12, 9)
LARGE_FIGSIZE = (16, 12)

# Use colors from palette and excluding Red, Green
# https://github.com/d3/d3-3.x-api-reference/blob/master/Ordinal-Scales.md#category20
BAR_COLORS = {
    'Battery Discharge': '#aec7e8',
    'Solar Self Use': '#1f77b4',
    'Grid Consumption': '#c5b0d5',
    'Gross Solar Generation': '#c49c94',
    'Grid Generation': '#dbdb8d',
    'Battery Charge': '#8c564b',
}

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
# ### Solar Self Use
#
# Solar Self Use is any solar energy that gets used immediately within the household during
# sunlight hours, i.e. excludes any battery storage.
#
# `solar_self_kwh` = `solar_generation_kwh - meter_generation_kwh - charge_quantity_kwh`
#
# ### Gross Usage
#
# Gross usage is what the usage would be as measured by the meter if no solar nor battery exists.
# This provides a good baseline to compare how the household uses energy.
# The usage measured at the meter is no longer accurate for households with either solar and/or
# batteries installed so this would have to be derived using formula below.
#
# `gross_usage_kwh` = `meter_consumption_kwh + solar_self_kwh + discharge_quantity_kwh`
#
# ### Self Consumption
#
# Self consumption is the amount solar energy used within the household, whether
# that is used during sunlight hours or stored in the battery for later use.
#
# `self_consumption_kwh` = `solar_self_kwh + discharge_quantity_kwh`


# %%
df_energy_hh['solar_self_kwh'] = df_energy_hh['solar_generation_kwh'] - \
    df_energy_hh['meter_generation_kwh'] - df_energy_hh['charge_quantity_kwh']

df_energy_hh['gross_usage_kwh'] = df_energy_hh['meter_consumption_kwh'] + \
    df_energy_hh['solar_self_kwh'] + df_energy_hh['discharge_quantity_kwh']

df_energy_hh['self_consumption_kwh'] = df_energy_hh['solar_self_kwh'] + \
    df_energy_hh['discharge_quantity_kwh']

df_energy_hh

# %% [markdown]

# Also group data at day level and join to daily min and max temperatures.

# %%
df_energy_daily = df_energy_hh.groupby(['interval_date']).agg(
    meter_consumption_kwh_mean=pd.NamedAgg(
        column='meter_consumption_kwh', aggfunc='mean'),
    meter_consumption_kwh_sum=pd.NamedAgg(
        column='meter_consumption_kwh', aggfunc='sum'),
    meter_generation_kwh_mean=pd.NamedAgg(
        column='meter_generation_kwh', aggfunc='mean'),
    meter_generation_kwh_sum=pd.NamedAgg(
        column='meter_generation_kwh', aggfunc='sum'),
    solar_generation_kwh_mean=pd.NamedAgg(
        column='solar_generation_kwh', aggfunc='mean'),
    solar_generation_kwh_sum=pd.NamedAgg(
        column='solar_generation_kwh', aggfunc='sum'),
    solar_mean_powr_kw=pd.NamedAgg(
        column='solar_mean_powr_kw', aggfunc='mean'),
    solar_devices_reporting=pd.NamedAgg(
        column='solar_devices_reporting', aggfunc='max'),
    capacity_kw=pd.NamedAgg(
        column='capacity_kw', aggfunc='mean'),
    charge_quantity_kwh_mean=pd.NamedAgg(
        column='charge_quantity_kwh', aggfunc='mean'),
    charge_quantity_kwh_sum=pd.NamedAgg(
        column='charge_quantity_kwh', aggfunc='sum'),
    discharge_quantity_kwh_mean=pd.NamedAgg(
        column='discharge_quantity_kwh', aggfunc='mean'),
    discharge_quantity_kwh_sum=pd.NamedAgg(
        column='discharge_quantity_kwh', aggfunc='sum'),
    deterioration_state_pct=pd.NamedAgg(
        column='deterioration_state_pct', aggfunc='max'),
    power_at_charge_kw=pd.NamedAgg(
        column='power_at_charge_kw', aggfunc='mean'),
    residual_capacity_pct=pd.NamedAgg(
        column='residual_capacity_pct', aggfunc='mean'),
    total_charge_quantity_kwh=pd.NamedAgg(
        column='total_charge_quantity_kwh', aggfunc='last'),
    total_discharge_quantity_kwh=pd.NamedAgg(
        column='total_discharge_quantity_kwh', aggfunc='last'),
    solar_self_kwh_mean=pd.NamedAgg(
        column='solar_self_kwh', aggfunc='mean'),
    solar_self_kwh_sum=pd.NamedAgg(
        column='solar_self_kwh', aggfunc='sum'),
    gross_usage_kwh_mean=pd.NamedAgg(
        column='gross_usage_kwh', aggfunc='mean'),
    gross_usage_kwh_sum=pd.NamedAgg(
        column='gross_usage_kwh', aggfunc='sum'),
    self_consumption_kwh_mean=pd.NamedAgg(
        column='self_consumption_kwh', aggfunc='mean'),
    self_consumption_kwh_sum=pd.NamedAgg(
        column='self_consumption_kwh', aggfunc='sum'),
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

title = 'Gross Consumption HH Mean - workdays vs. holidays'
x = np.arange(1, 49)
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
# ### Gross Consumption Across Seasons

# %%
seasons = [
    {'name': 'Summer', 'months': [12, 1, 2]},
    {'name': 'Autumn', 'months': [3, 4, 5]},
    {'name': 'Winter', 'months': [6, 7, 8]},
    {'name': 'Spring', 'months': [9, 10, 11]},
]

seasons_data = [{'dfm': df_energy_hh.loc[df_energy_hh.index.month.isin(
    season.get('months'))], **season} for season in seasons]

title = 'Gross Consumption HH Mean - Seasons'
x = np.arange(1, 49)
fig, axes = plt.subplots(figsize=MEDIUM_FIGSIZE)

for season_data in seasons_data:
    dfm = season_data.get('dfm')
    df_hh_mean = dfm.groupby(['interval']).agg(
        gross_usage_kwh=pd.NamedAgg(column='gross_usage_kwh', aggfunc='mean'),
        meter_consumption_kwh=pd.NamedAgg(
            column='meter_consumption_kwh', aggfunc='mean'),
    )
    axes.plot(x, df_hh_mean['gross_usage_kwh'],
              label=season_data.get('name'))

axes.set_xlabel('intervals')
axes.set_ylabel('kWh')
axes.set_title(title)
axes.legend()

# %% [markdown]
# ### Gross Generation Across Seasons

# %%
title = 'Gross Solar HH Mean - Seasons'
x = np.arange(1, 49)
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
              label=season_data.get('name'))

axes.set_xlabel('intervals')
axes.set_ylabel('kWh')
axes.set_title(title)
axes.legend()

# %% [markdown]

# ## Temperature Impacts

# %%

title = f"Daily Gross Usage vs. Daily min and max Temperatures"
fig, axes = plt.subplots(figsize=LARGE_FIGSIZE)
y = df_energy_daily['gross_usage_kwh_sum']
axes.scatter(x=df_energy_daily['min_temperature_c'],
             y=y, label='Day min')
axes.scatter(x=df_energy_daily['max_temperature_c'],
             y=y, label='Day max')
axes.set_xlabel('Temperature C')
axes.set_ylabel('Day Gross Usage kWh')
axes.set_title(title)
axes.grid(True)
axes.legend()

# %%
title = f"Daily Gross Generation vs. Daily min and max Temperatures"
fig, axes = plt.subplots(figsize=LARGE_FIGSIZE)
has_solar = df_energy_daily['solar_devices_reporting'] > 0
y = df_energy_daily['solar_generation_kwh_sum'].loc[has_solar]
axes.scatter(x=df_energy_daily['min_temperature_c'].loc[has_solar],
             y=y, label='Day min')
axes.scatter(x=df_energy_daily['max_temperature_c'].loc[has_solar],
             y=y, label='Day max')
axes.set_xlabel('Temperature C')
axes.set_ylabel('Day Gross Solar Generation kWh')
axes.set_title(title)
axes.grid(True)
axes.legend()

# %% [markdown]

# There seems to be some non-linear trend with Gross Usage vs. Temperature.  As temperature increases,
# gross usage increases.  However, this trend reverses around 22 degrees (just based on visuals)
# where gross usage increases as temperature decreases.

# TODO - R&D on calculating and plotting non-linear trendlines.

# %% [markdown]
# ## Comparing different event periods
#
# Here, we define the periods we want to compare and create list of event periods (date ranges) as plot data.
#

# %%
two_days_ago = datetime.now() - timedelta(days=2)

periods = [
    {'name': 'Epoch', 'actual_start': '2014-11-26', 'actual_end': '2015-04-06'},
    {'name': 'Installed Solar 3 kW',
        'actual_start': '2015-04-07', 'actual_end': '2016-08-14'},
    {'name': 'Installed Battery 8 kW',
        'actual_start': '2016-08-14', 'actual_end': '2016-10-17'},
    {'name': 'Upgraded to Solar 6 kW',
        'actual_start': '2016-10-18', 'actual_end': '2019-11-30'},
    {'name': 'Purchased EV', 'actual_start': '2019-12-01', 'actual_end': '2020-03-22'},
    {'name': 'Red EV Plan', 'actual_start': '2020-03-23',
        'actual_end': datetime.strftime(two_days_ago, '%Y-%m-%d')},
]

periods_data = []

for p in periods:
    period_data = {**p}
    date_mask = (df_energy_hh.index >= p.get('actual_start')) & (
        df_energy_hh.index <= p.get('actual_end'))
    df_period = df_energy_hh.loc[date_mask]
    df_hh_mean = df_period.groupby(['interval']).agg(
        meter_consumption_kwh=pd.NamedAgg(
            column='meter_consumption_kwh', aggfunc='mean'),
        meter_generation_kwh=pd.NamedAgg(
            column='meter_generation_kwh', aggfunc='mean'),
        solar_generation_kwh=pd.NamedAgg(
            column='solar_generation_kwh', aggfunc='mean'),
        charge_quantity_kwh=pd.NamedAgg(
            column='charge_quantity_kwh', aggfunc='mean'),
        discharge_quantity_kwh=pd.NamedAgg(
            column='discharge_quantity_kwh', aggfunc='mean'),
        solar_self_kwh=pd.NamedAgg(
            column='solar_self_kwh', aggfunc='mean'),
        gross_usage_kwh=pd.NamedAgg(column='gross_usage_kwh', aggfunc='mean'),
        self_consumption_kwh=pd.NamedAgg(
            column='self_consumption_kwh', aggfunc='mean'),
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

    title = f"Half Hourly Mean {period_data.get('name')} - {period_data.get('actual_start')} to {period_data.get('actual_end')}"
    x = np.arange(1, 49)
    fig, axes = plt.subplots(figsize=LARGE_FIGSIZE)

    y_discharge_quantity_kwh = df_hh_mean['discharge_quantity_kwh']
    y_meter_consumption_kwh = df_hh_mean['meter_consumption_kwh']
    y_solar_self = df_hh_mean['solar_self_kwh']

    y_solar_generation_kwh = df_hh_mean['solar_generation_kwh'] * -1
    y_meter_generation_kwh = df_hh_mean['meter_generation_kwh'] * -1
    y_charge_quantity_kwh = df_hh_mean['charge_quantity_kwh'] * -1

    axes.bar(x=x,
             height=y_meter_consumption_kwh + y_solar_self + y_discharge_quantity_kwh, label='Battery Discharge', align='edge', color=BAR_COLORS['Battery Discharge'])
    axes.bar(x=x,
             height=y_meter_consumption_kwh + y_solar_self, label='Solar Self Use', align='edge', color=BAR_COLORS['Solar Self Use'])
    axes.bar(x=x,
             height=y_meter_consumption_kwh, label='Grid Consumption', align='edge', color=BAR_COLORS['Grid Consumption'])

    axes.bar(x=x,
             height=y_solar_generation_kwh, label='Gross Solar Generation', align='edge', color=BAR_COLORS['Gross Solar Generation'])
    axes.bar(x=x,
             height=y_meter_generation_kwh, label='Grid Generation', align='edge', color=BAR_COLORS['Grid Generation'])
    axes.bar(x=x,
             height=y_charge_quantity_kwh, label='Battery Charge', align='edge', color=BAR_COLORS['Battery Charge'])

    axes.set_xlabel('Time - Interval')
    axes.set_ylabel('kWh')
    axes.set_ylim(ymin=-1.8, ymax=1.8)
    axes.set_title(title)
    axes.legend()

# %% [markdown]
# ## Gross vs. Net - Weekly and Monthly

# %%
df_energy_weekly = df_energy_daily.groupby(df_energy_daily.index.to_period('W')).agg(
    meter_consumption_kwh=pd.NamedAgg(
        column='meter_consumption_kwh_sum', aggfunc='sum'),
    solar_generation_kwh=pd.NamedAgg(
        column='solar_generation_kwh_sum', aggfunc='sum'),
    meter_generation_kwh=pd.NamedAgg(
        column='meter_generation_kwh_sum', aggfunc='sum'),
    charge_quantity_kwh=pd.NamedAgg(
        column='charge_quantity_kwh_sum', aggfunc='sum'),
    discharge_quantity_kwh=pd.NamedAgg(
        column='discharge_quantity_kwh_sum', aggfunc='sum'),
    solar_self_kwh=pd.NamedAgg(
        column='solar_self_kwh_sum', aggfunc='sum'),
    gross_usage_kwh=pd.NamedAgg(
        column='gross_usage_kwh_sum', aggfunc='sum'),
    self_consumption_kwh=pd.NamedAgg(
        column='self_consumption_kwh_sum', aggfunc='sum'),
    deterioration_state_pct=pd.NamedAgg(
        column='deterioration_state_pct', aggfunc='max'),
    min_temperature_c_mean=pd.NamedAgg(
        column='min_temperature_c', aggfunc='mean'),
    max_temperature_c_mean=pd.NamedAgg(
        column='max_temperature_c', aggfunc='mean'),
)
df_energy_weekly
# %%
x = df_energy_weekly.index.strftime('%Y-W%W')

y_discharge_quantity_kwh = df_energy_weekly['discharge_quantity_kwh']
y_meter_consumption_kwh = df_energy_weekly['meter_consumption_kwh']
y_solar_self = df_energy_weekly['solar_self_kwh']

y_solar_generation_kwh = df_energy_weekly['solar_generation_kwh'] * -1
y_meter_generation_kwh = df_energy_weekly['meter_generation_kwh'] * -1
y_charge_quantity_kwh = df_energy_weekly['charge_quantity_kwh'] * -1

fig, axes = plt.subplots(figsize=LARGE_FIGSIZE)
divider = make_axes_locatable(axes)

title = f"Actual by Week"
plt.xticks(rotation='45')
axes.bar(x=x,
         height=y_meter_consumption_kwh + y_solar_self + y_discharge_quantity_kwh, label='Battery Discharge', align='edge', color=BAR_COLORS['Battery Discharge'])
axes.bar(x=x,
         height=y_meter_consumption_kwh + y_solar_self, label='Solar Self Use', align='edge', color=BAR_COLORS['Solar Self Use'])
axes.bar(x=x,
         height=y_meter_consumption_kwh, label='Grid Consumption', align='edge', color=BAR_COLORS['Grid Consumption'])

axes.bar(x=x,
         height=y_solar_generation_kwh, label='Gross Solar Generation', align='edge', color=BAR_COLORS['Gross Solar Generation'])
axes.bar(x=x,
         height=y_meter_generation_kwh, label='Grid Generation', align='edge', color=BAR_COLORS['Grid Generation'])
axes.bar(x=x,
         height=y_charge_quantity_kwh, label='Battery Charge', align='edge', color=BAR_COLORS['Battery Charge'])
axes.set_xlabel('Time - Week')
for idx, label in enumerate(axes.xaxis.get_ticklabels()):
    if idx % 8 != 0:
        label.set_visible(False)
axes.set_ylabel('kWh')
axes.set_title(title)
axes.legend()

y_temp_min = df_energy_weekly['min_temperature_c_mean']
y_temp_max = df_energy_weekly['max_temperature_c_mean']
axes_max_temp = divider.append_axes("top", 1.5, pad=0.5, sharex=axes)
axes_max_temp.set_title('Mean Min and Max Temperatures')
axes_max_temp.set_ylabel('Temperature C')
axes_max_temp.get_yaxis().set_label_position('right')
axes_max_temp.get_yaxis().tick_right()
axes_max_temp.get_xaxis().set_visible(False)
axes_max_temp.plot(x, y_temp_min)
axes_max_temp.plot(x, y_temp_max)

y_batt_deter = df_energy_weekly['deterioration_state_pct'].replace(
    0, np.nan) * 100
axes_batt_health = divider.append_axes("bottom", 1.5, pad=1.5, sharex=axes)
axes_batt_health.set_title('Battery Deterioration')
axes_batt_health.set_ylim(ymin=0, ymax=110)
axes_batt_health.set_ylabel('Percentage')
axes_batt_health.get_yaxis().set_label_position('right')
axes_batt_health.get_yaxis().tick_right()
axes_batt_health.get_xaxis().set_visible(False)
axes_batt_health.plot(x, y_batt_deter)

# %%
df_energy_monthly = df_energy_daily.groupby(df_energy_daily.index.to_period('M')).agg(
    meter_consumption_kwh=pd.NamedAgg(
        column='meter_consumption_kwh_sum', aggfunc='sum'),
    solar_generation_kwh=pd.NamedAgg(
        column='solar_generation_kwh_sum', aggfunc='sum'),
    meter_generation_kwh=pd.NamedAgg(
        column='meter_generation_kwh_sum', aggfunc='sum'),
    charge_quantity_kwh=pd.NamedAgg(
        column='charge_quantity_kwh_sum', aggfunc='sum'),
    discharge_quantity_kwh=pd.NamedAgg(
        column='discharge_quantity_kwh_sum', aggfunc='sum'),
    solar_self_kwh=pd.NamedAgg(
        column='solar_self_kwh_sum', aggfunc='sum'),
    gross_usage_kwh=pd.NamedAgg(
        column='gross_usage_kwh_sum', aggfunc='sum'),
    self_consumption_kwh=pd.NamedAgg(
        column='self_consumption_kwh_sum', aggfunc='sum'),
)
df_energy_monthly
# %% [markdown]

# ## TODO

# * Apply rates and plot similar charts to assess dollar impacts overtime
# * Analyse solar generation deterioration
