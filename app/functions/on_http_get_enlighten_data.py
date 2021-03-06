from datetime import date, datetime, timedelta

from app import (ENLIGHTEN_API_KEY, ENLIGHTEN_DATA_MIN_DATE,
                 ENLIGHTEN_STORAGE_PATH_PREFIX, ENLIGHTEN_SYSTEM_ID,
                 ENLIGHTEN_USER_ID, GCP_STORAGE_BUCKET_ID, init_gcp_logger,
                 init_storage_client)
from app.common import get_already_fetched, idate_range
from app.enlighten import get_enlighten_stats_resp

# If blob name already exists but file size is small then it's probably an API error messgage only.
ALREADY_FETCHED_SIZE_THRESHOLD_BYTES = 1024

# Enlighten API free plan only allows maximum of 10 API calls per minute
MAX_FETCHED_BATCH_SIZE = 10


def on_http_get_enlighten_data(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    gcp_logger = init_gcp_logger()
    gcp_logger.info('on_http_get_enlighten_data(), args=%s', request.args)
    storage_client = init_storage_client()

    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)

    min_date = datetime.combine(date.fromisoformat(
        ENLIGHTEN_DATA_MIN_DATE), datetime.min.time())
    yesterday = datetime.combine(
        date.today(), datetime.min.time()) - timedelta(days=1)
    already_fetched = get_already_fetched(
        storage_client, bucket, ENLIGHTEN_STORAGE_PATH_PREFIX, ALREADY_FETCHED_SIZE_THRESHOLD_BYTES)
    fetched_counter = 0

    for as_of_date in idate_range(min_date, yesterday):
        if fetched_counter >= MAX_FETCHED_BATCH_SIZE:
            break

        blob_name = f"{ENLIGHTEN_STORAGE_PATH_PREFIX}/{str(as_of_date.year)}/enlighten_stats_{as_of_date.strftime('%Y%m%d')}.json"
        blob_exists = next((i for i in already_fetched if (
            i.get('name') == blob_name)), None) is not None
        if not blob_exists:
            gcp_logger.info('blob %s not exists, downloading.', blob_name)
            resp = get_enlighten_stats_resp(
                ENLIGHTEN_API_KEY, ENLIGHTEN_USER_ID, ENLIGHTEN_SYSTEM_ID, as_of_date)
            new_blob = bucket.blob(blob_name)
            new_blob.upload_from_string(resp.text)
            fetched_counter += 1
        else:
            gcp_logger.debug('blob %s already exists, skipping.', blob_name)

    return ('', 200)
