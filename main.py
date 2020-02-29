from pathlib import Path
from datetime import datetime, timedelta, date
from google.cloud import storage
from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging
import google.cloud.logging as gcp_logging
import logging
import sys
from enlighten import ENLIGHTEN_API_KEY, ENLIGHTEN_USER_ID, ENLIGHTEN_SYSTEM_ID, ENLIGHTEN_DATA_MIN_DATE, ENLIGHTEN_STORAGE_PATH_PREFIX
from enlighten.enlighten import get_enlighten_stats_resp, get_already_fetched
from common import idate_range
import os
from nem12 import NEM12_STORAGE_PATH_IN, NEM12_STORAGE_PATH_MERGED
from nem12.nem12 import Nem12Merger, handle_nem12_blob_in, handle_nem12_blob_merged
import csv

GCP_PROJECT = os.environ.get(
    'GCP_PROJECT', 'GCP_PROJECT not set.')
GCP_STORAGE_BUCKET_ID = os.environ.get(
    'GCP_STORAGE_BUCKET_ID', 'GCP_STORAGE_BUCKET_ID not set.')

GCP_LOG_CLIENT = gcp_logging.Client()
GCP_LOG_HANDLER = CloudLoggingHandler(GCP_LOG_CLIENT)
gcp_logger = logging.getLogger()
gcp_logger.setLevel(logging.INFO)
gcp_logger.addHandler(GCP_LOG_HANDLER)

storage_client = storage.Client()


def on_schedule_post(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    # If blob name already exists but file size is small then it's probably an API error messgage only.
    ALREADY_FETCHED_SIZE_THRESHOLD_BYTES = 1024

    # Enlighten API free plan only allows maximum of 10 API calls per minute
    MAX_FETCHED_BATCH_SIZE = 10

    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)

    min_date = datetime.combine(date.fromisoformat(
        ENLIGHTEN_DATA_MIN_DATE), datetime.min.time())
    yesterday = datetime.combine(
        date.today(), datetime.min.time()) - timedelta(days=1)
    already_fetched = get_already_fetched(
        storage_client, bucket, ALREADY_FETCHED_SIZE_THRESHOLD_BYTES)
    fetched_counter = 0

    for as_of_date in idate_range(min_date, yesterday):
        if (fetched_counter >= MAX_FETCHED_BATCH_SIZE):
            break

        blob_name = f"{ENLIGHTEN_STORAGE_PATH_PREFIX}/{str(as_of_date.year)}/enlighten_stats_{as_of_date.strftime('%Y%m%d')}.json"
        blob_exists = next((i for i in already_fetched if (
            i.get('name') == blob_name)), None) != None
        if (not blob_exists):
            gcp_logger.info(f"blob {blob_name} not exists, downloading.")
            resp = get_enlighten_stats_resp(
                ENLIGHTEN_API_KEY, ENLIGHTEN_USER_ID, ENLIGHTEN_SYSTEM_ID, as_of_date)
            new_blob = bucket.blob(blob_name)
            new_blob.upload_from_string(resp.text)
            fetched_counter += 1
        else:
            gcp_logger.debug(f"blob {blob_name} already exists, skipping.")

    return ('', 200)


def on_storage_blob(data, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """
    event_id = context.event_id
    event_type = context.event_type
    blob_created = data.get('timeCreated')
    blob_updated = data.get('updated')
    blob_name = data.get('name')
    bucket_name = data.get('bucket')
    gcp_logger.info(
        f"event_id={event_id}, event_type={event_type}, bucket={bucket_name}, name={blob_name}, metageneration={data.get('metageneration')}, created={blob_created}, updated={blob_updated}")

    bucket = storage_client.get_bucket(bucket_name)

    if (blob_name.startswith(NEM12_STORAGE_PATH_IN)):
        handle_nem12_blob_in(data, context, storage_client,
                          bucket, blob_name, gcp_logger)
    elif (blob_name.startswith(NEM12_STORAGE_PATH_MERGED)):
        handle_nem12_blob_merged(data, context, storage_client,
                          bucket, blob_name, gcp_logger)
    else:
        gcp_logger.debug(
            f"Skipping storage event event_id={context.event_id}, event_type={context.event_type}")

    return ('', 200)
