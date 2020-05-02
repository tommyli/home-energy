import json
import re
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from google.cloud.storage import Blob

from app import ENLIGHTEN_URL, NMI
from app.common import AEST_OFFSET, LOCAL_TZ, merge_df_to_db


def get_enlighten_stats_resp(api_key, user_id, system_id, as_of_date):
    local_as_of_date = datetime.combine(
        as_of_date.date(), datetime.min.time(), tzinfo=LOCAL_TZ)
    path = f"/api/v2/systems/{system_id}/stats"
    query = {'key': api_key, 'user_id': user_id,
             'datetime_format': 'iso8601', 'start_at': f"{int(local_as_of_date.timestamp())}"}
    resp = requests.get(f"{ENLIGHTEN_URL}{path}", params=query)

    return resp


def handle_enlighten_blob(data, context, storage_client, bucket, blob_name, root_collection_name, logger):
    """
    Handle blob events in path ENLIGHTEN_STORAGE_PATH_PREFIX, parses the enlighten stats blob (from the blob event),
    groups into half hour intervals and loads into Firestore.  Each blob represents data for one day.
    """

    logger.info('handle_enlighten_blob(blob_name=%s)', blob_name)

    match = re.search(r'enlighten_stats_(\d\d\d\d\d\d\d\d).json',
                      blob_name)
    if match is None:
        logger.warn('Unexpected blob_name=%s', blob_name)
        return None

    blob = Blob(blob_name, bucket)
    raw_data = json.loads(blob.download_as_string())

    interval_date = datetime.strptime(match[1], '%Y%m%d')

    df_day = create_normalised_enlighten_stats_df(
        interval_date, raw_data.get('intervals'))

    merge_df_to_db(NMI, df_day, root_collection_name, logger)


def rounded_mean(num):
    return round(np.mean(num / 1000), 3)


def sum_to_kwh(num):
    return np.sum(num / 1000)


def create_normalised_enlighten_stats_df(interval_date, enlighten_intervals):
    df_stats = pd.DataFrame(enlighten_intervals)

    df_stats.rename(columns={'end_at': 'period_end'}, inplace=True)
    df_stats['period_end'] = pd.to_datetime(
        df_stats['period_end']).dt.tz_convert(LOCAL_TZ)
    df_stats['period_start'] = df_stats['period_end'] - \
        pd.Timedelta('5 minutes')
    df_stats['interval_date'] = pd.to_datetime(
        df_stats['period_start'].dt.date)
    df_stats.set_index(['interval_date', 'period_start',
                        'period_end'], inplace=True)

    df_periods = _create_5_min_periods_df(interval_date)

    df_solar = df_periods.join(df_stats, how='left', on=[
        'interval_date', 'period_start', 'period_end'])
    df_interval = df_solar.groupby(['interval_date', 'interval']).agg(
        devices_reporting=pd.NamedAgg(
            column='devices_reporting', aggfunc='first'),
        mean_powr_kw=pd.NamedAgg(column='powr', aggfunc=rounded_mean),
        generation_kwh=pd.NamedAgg(column='enwh', aggfunc=sum_to_kwh),
    ).fillna(0)
    df_day = df_interval.groupby(['interval_date']).agg(
        solar_devices_reportings=pd.NamedAgg(
            column='devices_reporting', aggfunc=list),
        solar_mean_powrs_kw=pd.NamedAgg(column='mean_powr_kw', aggfunc=list),
        solar_generations_kwh=pd.NamedAgg(
            column='generation_kwh', aggfunc=list),
    )

    return df_day


def _create_5_min_periods_df(interval_date):
    iso_date_str = interval_date.strftime('%Y-%m-%d')
    interval_date = pd.Timestamp(iso_date_str)

    interval_count = int(60 * 24 / 5)
    interval5s = pd.Index(np.arange(1, interval_count + 1))
    period_starts = pd.date_range(
        start=interval_date, periods=interval_count, freq='5min', tz=AEST_OFFSET)
    period_ends = period_starts + pd.Timedelta('5 minutes')
    df_periods = pd.DataFrame({
        'interval_date': interval_date,
        'interval_5': interval5s,
        'period_start': period_starts,
        'period_end': period_ends,
    })
    df_periods['interval'] = np.ceil(
        df_periods['interval_5'] / (30 / 5)).astype(int)
    df_periods.set_index(['interval_date', 'period_start',
                          'period_end', 'interval_5', 'interval'])

    return df_periods
