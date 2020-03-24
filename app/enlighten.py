from datetime import date
from datetime import datetime
from datetime import timedelta
from pytz import timezone
from pathlib import Path
import pytz
import csv
import getopt
import os
import requests
import sys

from google.cloud import firestore
from google.cloud.storage import Blob
import pandas as pd

from . import ENLIGHTEN_STORAGE_PATH_PREFIX
from . import ENLIGHTEN_URL
from . import init_firestore_client


def get_enlighten_stats_resp(api_key, user_id, system_id, as_of_date):
    local_tz = timezone('Australia/Melbourne')
    local_as_of_date = datetime.combine(
        as_of_date.date(), datetime.min.time(), tzinfo=local_tz)
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

    logger.info(f"handle_enlighten_blob()")

    Path(
        f"/tmp/{ENLIGHTEN_STORAGE_PATH_PREFIX}").mkdir(parents=True, exist_ok=True)

    blob = Blob(blob_name, bucket)

    with open(f"/tmp/{blob_name}", 'wb') as file_obj:
        blob.download_to_file(file_obj)

    os.remove(f"/tmp/{blob_name}")
