from datetime import datetime, timedelta, date
from google.cloud import storage
from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging
from operator import itemgetter
import google.cloud.logging as gcp_logging
import itertools
import json
import logging
import os
import requests
import sys
import time


GCP_PROJECT = os.environ.get(
    'GCP_PROJECT', 'GCP_PROJECT not set.')
ENLIGHTEN_SYSTEM_ID = os.environ.get(
    'ENLIGHTEN_SYSTEM_ID', 'ENLIGHTEN_SYSTEM_ID not set.')
ENLIGHTEN_API_KEY = os.environ.get(
    'ENLIGHTEN_API_KEY', 'ENLIGHTEN_API_KEY not set.')
ENLIGHTEN_USER_ID = os.environ.get(
    'ENLIGHTEN_USER_ID', 'ENLIGHTEN_USER_ID not set.')
GCP_STORAGE_BUCKET_ID = os.environ.get(
    'GCP_STORAGE_BUCKET_ID', 'GCP_STORAGE_BUCKET_ID not set.')
STORAGE_PATH_PREFIX = os.environ.get(
    'GCP_STORAGE_PATH_PREFIX', 'GCP_STORAGE_PATH_PREFIX not set.')
ENLIGHTEN_DATA_MIN_DATE = os.environ.get(
    'ENLIGHTEN_DATA_MIN_DATE', 'ENLIGHTEN_DATA_MIN_DATE not set.')

# If blob name already exists but file size is small then it's probably an API error messgage only.
ALREADY_FETCHED_SIZE_THRESHOLD_BYTES = 1024

# Enlighten API free plan only allows maximum of 10 API calls per minute
MAX_FETCHED_BATCH_SIZE = 10

GCP_LOG_CLIENT = gcp_logging.Client()
GCP_LOG_HANDLER = CloudLoggingHandler(GCP_LOG_CLIENT)
gcp_logger = logging.getLogger()
gcp_logger.setLevel(logging.INFO)
gcp_logger.addHandler(GCP_LOG_HANDLER)


def fetch_enlighten_data_get(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)

    min_date = datetime.combine(date.fromisoformat(
        ENLIGHTEN_DATA_MIN_DATE), datetime.min.time())
    yesterday = datetime.combine(
        date.today(), datetime.min.time()) - timedelta(days=1)
    already_fetched = _get_already_fetched(storage_client, bucket)
    fetched_counter = 0

    for as_of_date in _idate_range(min_date, yesterday):
        if (fetched_counter >= MAX_FETCHED_BATCH_SIZE):
            break

        blob_name = f"{STORAGE_PATH_PREFIX}/{str(as_of_date.year)}/enlighten_stats_{as_of_date.strftime('%Y%m%d')}.json"
        blob_exists = next((i for i in already_fetched if (
            i.get('name') == blob_name)), None) != None
        if (not blob_exists):
            gcp_logger.info(f"blob {blob_name} not exists, downloading.")
            resp = _get_enlighten_stats_resp(as_of_date)
            new_blob = bucket.blob(blob_name)
            new_blob.upload_from_string(resp.text)
            fetched_counter += 1
        else:
            gcp_logger.debug(f"blob {blob_name} already exists, skipping.")

    return ('', 200)


def _get_already_fetched(storage_client, bucket):
    all_blobs = ({'name': b.name, 'size': b.size}
                 for b in storage_client.list_blobs(bucket))
    sorted_by_size = sorted(all_blobs, key=itemgetter('size'))
    grouped_by_size_more_than_threshold = itertools.groupby(
        sorted_by_size, lambda b: b.get('size') > ALREADY_FETCHED_SIZE_THRESHOLD_BYTES)
    already_fetched_group = next(
        (i for i in grouped_by_size_more_than_threshold if (i[0])), (True, iter([])))
    already_fetched = already_fetched_group[1]

    return list(already_fetched)


def _get_enlighten_stats_resp(as_of_date):
    query = {'key': ENLIGHTEN_API_KEY, 'user_id': ENLIGHTEN_USER_ID,
             'datetime_format': 'iso8601', 'start_at': f"{int(as_of_date.timestamp())}"}
    resp = requests.get(
        f"https://api.enphaseenergy.com/api/v2/systems/{ENLIGHTEN_SYSTEM_ID}/stats", params=query)

    return resp


def _idate_range(start_date, end_date):
    all_dates_from_min = (start_date + timedelta(days=n)
                          for n in range(sys.maxsize))
    min_to_max = itertools.takewhile(
        lambda d: d <= end_date, all_dates_from_min)
    for d in min_to_max:
        yield d
