import re
from datetime import datetime

import pandas as pd
from google.cloud.storage import Blob

from app import (GCP_STORAGE_BUCKET_ID, LEMS_STORAGE_PATH_PREFIX, NMI,
                 init_gcp_logger, init_storage_client)
from app.common import get_already_fetched, merge_df_to_db
from app.lems import create_df_with_yesterday, create_normalised_lems_df


def on_http_reload_lems(request):
    gcp_logger = init_gcp_logger()
    request_args = request.args
    gcp_logger.info('on_http_reload_lems(), args=%s', request_args)

    year = request_args['year'] if request_args and 'year' in request_args else datetime.now(
    ).year
    storage_client = init_storage_client()
    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)

    already_fetched = [x.get('name') for x in get_already_fetched(
        storage_client, bucket, LEMS_STORAGE_PATH_PREFIX, 1024)]
    file_name_mask = f"lems_data_{str(year)}"
    to_reload = [af for af in already_fetched if file_name_mask in af]

    dfs = [_blob_to_df(bucket, gcp_logger, blob_name)
           for blob_name in to_reload]

    df_all_dates = pd.concat(dfs)
    df_unique = df_all_dates.loc[~df_all_dates.index.duplicated(keep='first')]
    merge_df_to_db(NMI, df_unique, 'sites', gcp_logger)

    return ('', 200)


def _blob_to_df(bucket, logger, blob_name):
    start = datetime.now()

    match = re.search(r'lems_data_(\d\d\d\d\d\d\d\d).csv',
                      blob_name)
    if match is None:
        return None

    interval_date = datetime.strptime(match[1], '%Y%m%d')
    blob_today = Blob(blob_name, bucket)
    csv_today = blob_today.download_as_string().decode('utf-8')
    dfm = create_df_with_yesterday(bucket, interval_date, csv_today)

    df_days = create_normalised_lems_df(dfm)

    end = datetime.now()

    logger.info(f"_blob_to_df(blob_name={blob_name}), elapsed={end-start}")

    return df_days
