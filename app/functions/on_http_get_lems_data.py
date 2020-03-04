from datetime import datetime, timedelta, date
import csv
import os
import sys

import pandas as pd

from .. import init_gcp_logger
from .. import init_storage_client
from .. import LEMS_USER, LEMS_PASSWORD, LEMS_BATTERY_ID, LEMS_STORAGE_PATH_PREFIX, LEMS_DATA_MIN_DATE, GCP_STORAGE_BUCKET_ID
from ..common import idate_range
from ..common import get_already_fetched
from ..lems import get_lems_data_resp


def on_http_get_lems_data(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    gcp_logger = init_gcp_logger()
    gcp_logger.info(f"on_http_get_lems_data()")
    storage_client = init_storage_client()

    # If blob name already exists but file size is small then it's probably an API error messgage only.
    ALREADY_FETCHED_SIZE_THRESHOLD_BYTES = 1024

    # Enlighten API free plan only allows maximum of 10 API calls per minute
    MAX_FETCHED_BATCH_SIZE = 10

    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)

    min_date = datetime.combine(date.fromisoformat(
        LEMS_DATA_MIN_DATE), datetime.min.time())
    yesterday = datetime.combine(
        date.today(), datetime.min.time()) - timedelta(days=1)
    already_fetched = get_already_fetched(
        storage_client, bucket, LEMS_STORAGE_PATH_PREFIX, ALREADY_FETCHED_SIZE_THRESHOLD_BYTES)

    for as_of_date in idate_range(min_date, yesterday):
        blob_name = f"{LEMS_STORAGE_PATH_PREFIX}/{str(as_of_date.year)}/lems_data_{as_of_date.strftime('%Y%m%d')}.csv"
        blob_exists = next((i for i in already_fetched if (
            i.get('name') == blob_name)), None) != None
        if (not blob_exists):
            gcp_logger.info(f"blob {blob_name} not exists, downloading.")
            resp = get_lems_data_resp(
                LEMS_USER, LEMS_PASSWORD, LEMS_BATTERY_ID, as_of_date)
            df = pd.DataFrame(resp.json())

            new_blob = bucket.blob(blob_name)
            new_blob.upload_from_string(df.to_csv())
        else:
            gcp_logger.debug(f"blob {blob_name} already exists, skipping.")

    return ('', 200)
