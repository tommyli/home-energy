from datetime import datetime, timedelta, date
from pathlib import Path
import csv
import os
import sys

from .. import NEM12_STORAGE_PATH_IN, NEM12_STORAGE_PATH_MERGED
from .. import ENLIGHTEN_STORAGE_PATH_PREFIX
from .. import init_gcp_logger
from .. import init_storage_client

from ..common import idate_range
from ..nem12 import handle_nem12_blob_in
from ..nem12 import handle_nem12_blob_merged
from ..nem12 import Nem12Merger
from ..enlighten import handle_enlighten_blob


def on_storage_blob(data, context):
    """Background Cloud Function to be triggered by Cloud Storage.
       This generic function logs relevant data when a file is changed.

    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    gcp_logger = init_gcp_logger()
    gcp_logger.info(f"on_http_get_lems_data()")
    storage_client = init_storage_client()

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
                                 bucket, blob_name, 'sites', gcp_logger)
    elif(blob_name.startswith(ENLIGHTEN_STORAGE_PATH_PREFIX)):
        handle_enlighten_blob(data, context, storage_client,
                              bucket, blob_name, 'sites', gcp_logger)
    else:
        gcp_logger.debug(
            f"Skipping storage event event_id={context.event_id}, event_type={context.event_type}")

    return ('', 200)
