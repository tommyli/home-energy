import json
import re
from datetime import datetime

import pandas as pd
from google.cloud.storage import Blob

from .. import GCP_STORAGE_BUCKET_ID, NMI, init_gcp_logger, init_storage_client
from ..common import get_already_fetched, merge_df_to_db
from ..enlighten import (ENLIGHTEN_STORAGE_PATH_PREFIX,
                         create_normalised_enlighten_stats_df)


def on_http_reload_enlighten(request):
    request_args = request.args
    year = request_args['year'] if request_args and 'year' in request_args else datetime.now(
    ).year
    storage_client = init_storage_client()
    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)
    gcp_logger = init_gcp_logger()

    already_fetched = [x.get('name') for x in get_already_fetched(
        storage_client, bucket, ENLIGHTEN_STORAGE_PATH_PREFIX, 1024) if f"/{str(year)}/" in x.get('name')]

    dfs = [_blob_to_df(bucket, gcp_logger, blob_name)
           for blob_name in already_fetched]

    df_all_dates = pd.concat(dfs)
    merge_df_to_db(NMI, df_all_dates, 'sites', gcp_logger)

    return ('', 200)


def _blob_to_df(bucket, logger, blob_name):
    start = datetime.now()

    blob = Blob(blob_name, bucket)
    raw_data = json.loads(blob.download_as_string())
    match = re.search(r'enlighten_stats_(\d\d\d\d\d\d\d\d).json',
                      blob_name)
    if match is None:
        return None

    interval_date = datetime.strptime(match[1], '%Y%m%d')
    df_result = create_normalised_enlighten_stats_df(
        interval_date, raw_data.get('intervals'))

    end = datetime.now()

    logger.info(f"_blob_to_df(blob_name={blob_name}), elapsed={end-start}")

    return df_result
