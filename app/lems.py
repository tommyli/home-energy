import base64
import re
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from google.cloud.storage import Blob

from app import LEMS_STORAGE_PATH_PREFIX, LEMS_URL, NMI
from app.common import AEST_OFFSET, merge_df_to_db


def get_lems_data_resp(user_id, password, batter_id, as_of_date):
    auth = base64.b64encode(
        f"{user_id}:{password}".encode()).decode("utf-8")
    headers = {"Authorization": f"Basic {auth}"}

    path = f"/api/Battery/{batter_id}/soc/data"
    query = {'MinDate': as_of_date, 'Hours': '24'}

    resp = requests.get(f"{LEMS_URL}{path}", headers=headers, params=query)

    return resp


def handle_lems_blob(data, context, storage_client, bucket, blob_name, root_collection_name, logger):
    """
    Handle blob events in path LEMS_STORAGE_PATH_PREFIX, parses the LEMS data blob (from the blob event),
    groups into half hour intervals and loads into Firestore.  Each blob represents data for one day.
    """

    logger.info('handle_lems_blob(blob_name=%s)', blob_name)

    match = re.search(r'lems_data_(\d\d\d\d\d\d\d\d).csv',
                      blob_name)
    if match is None:
        logger.warning('Unexpected blob_name=%s', blob_name)
        return None

    interval_date = datetime.strptime(match[1], '%Y%m%d')

    blob_today = Blob(blob_name, bucket)
    csv_today = blob_today.download_as_string().decode('utf-8')
    dfm = create_df_with_yesterday(bucket, interval_date, csv_today)

    df_days = create_normalised_lems_df(dfm)

    merge_df_to_db(NMI, df_days, root_collection_name, logger)


def create_df_with_yesterday(bucket, interval_date, raw_csv):
    dfm = pd.read_csv(StringIO(raw_csv))
    df_today = fix_dst_issue(dfm)

    yesterday = interval_date - timedelta(days=1)
    blob_name_yesterday = f"{LEMS_STORAGE_PATH_PREFIX}/{yesterday.year}/lems_data_{yesterday.strftime('%Y%m%d')}.csv"
    blob_yesterday = Blob(blob_name_yesterday, bucket)
    csv_yesterday = None
    if blob_yesterday.exists():
        csv_yesterday = blob_yesterday.download_as_string().decode('utf-8')
        df_yesterday = fix_dst_issue(pd.read_csv(StringIO(csv_yesterday)))
        return df_today.append(df_yesterday)

    return df_today


def create_normalised_lems_df(dfm):
    df_normalised = dfm.drop(['Unnamed: 0', 'BatteryId', 'UserGroupId', 'UserGroupName', 'RegistrationId',
                              'IFUnitSerial', 'CustomerNumber', 'CustomerName', 'CurrentMode', 'TimeZoneId'], axis='columns')
    df_normalised['period_start'] = pd.to_datetime(
        dfm['DateMeasuredUtc']).dt.tz_convert(AEST_OFFSET)
    df_normalised['interval_date'] = pd.to_datetime(
        df_normalised['period_start'].dt.date)
    df_normalised.index = dfm.index + 1
    df_normalised['DeteriorationState_pct'] = df_normalised['DeteriorationState'] / 100
    df_normalised['Capacity_kwh'] = df_normalised['Capacity'] / 1000
    df_normalised['ResidualCapacity_pct'] = df_normalised['ResidualCapacity'] / 100
    df_normalised['PowerAtCharge_kw'] = df_normalised['PowerAtCharge'] / 1000
    df_normalised['ChargeQty_kwh'] = df_normalised['ChargeQty'] / 1000
    df_normalised['DischargeQty_kwh'] = df_normalised['DischargeQty'] / 1000
    df_normalised['TotalChargeQty_kwh'] = df_normalised['TotalChargeQty'] / 1000
    df_normalised['TotalDischargeQty_kwh'] = df_normalised['TotalDischargeQty'] / 1000

    df_grouped_by_day = df_normalised.groupby(['interval_date']).agg(
        deterioration_states_pct=pd.NamedAgg(
            column='DeteriorationState_pct', aggfunc=list),
        capacities_kw=pd.NamedAgg(
            column='Capacity_kwh', aggfunc=list),
        residual_capcacities_pct=pd.NamedAgg(
            column='ResidualCapacity_pct', aggfunc=list),
        power_at_charges_kw=pd.NamedAgg(
            column='PowerAtCharge_kw', aggfunc=list),
        charge_quantities_kwh=pd.NamedAgg(
            column='ChargeQty_kwh', aggfunc=list),
        discharge_quantities_kwh=pd.NamedAgg(
            column='DischargeQty_kwh', aggfunc=list),
        total_charge_quantities_kwh=pd.NamedAgg(
            column='TotalChargeQty_kwh', aggfunc=list),
        total_discharge_quantities_kwh=pd.NamedAgg(
            column='TotalDischargeQty_kwh', aggfunc=list),
        count=pd.NamedAgg(column='interval_date', aggfunc='count'),
    )
    df_result = df_grouped_by_day.loc[df_grouped_by_day['count'] == 48]
    df_result = df_result.drop(['count'], axis='columns')

    return df_result


def fix_dst_issue(dfm):
    """
    This is needed because LEMS do not handle data correctly during DST transition, i.e. from AEDT to AEST
    The other way seems fine from AEST to AEDT.
    Two issues exists when going from AEDT to AEST:
    1. the hour that rolls back one hour has all values empty.
    2. Only 47 rows are returned instead of 48.
    There's nothing much we can do here, simply duplicate the row of data with empty values.
    Another hack is to add two additional half hourly data when transitioning from AEST to AEDT
    so that local time data can be converted to standard time data when these files are processed
    everyday.
    """
    row_count = len(dfm.index)
    if row_count == 47:
        df_nan = dfm.loc[dfm['UserGroupName'].isnull()]

        df_result = dfm.append([df_nan, df_nan, df_nan]).sort_values(
            by=['DateMeasuredBattery']).reset_index(drop=True)
        return df_result

    return dfm
