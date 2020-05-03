from app import (ENLIGHTEN_STORAGE_PATH_PREFIX, LEMS_STORAGE_PATH_PREFIX,
                 NEM12_STORAGE_PATH_IN, NEM12_STORAGE_PATH_MERGED,
                 init_gcp_logger, init_storage_client)
from app.enlighten import handle_enlighten_blob
from app.lems import handle_lems_blob
from app.nem12 import handle_nem12_blob_in, handle_nem12_blob_merged


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
    gcp_logger.info('on_http_get_lems_data()')
    storage_client = init_storage_client()

    event_id = context.event_id
    event_type = context.event_type
    blob_created = data.get('timeCreated')
    blob_updated = data.get('updated')
    blob_name = data.get('name')
    bucket_name = data.get('bucket')
    gcp_logger.info(
        'event_id=%s, event_type=%s, bucket=%s, name=%s, metageneration=%s, created=%s, updated=%s', event_id, event_type, bucket_name, blob_name, data.get('metageneration'), blob_created, blob_updated)

    bucket = storage_client.get_bucket(bucket_name)

    if blob_name.startswith(NEM12_STORAGE_PATH_IN):
        handle_nem12_blob_in(data, context, storage_client,
                             bucket, blob_name, gcp_logger)
    elif blob_name.startswith(NEM12_STORAGE_PATH_MERGED):
        handle_nem12_blob_merged(data, context, storage_client,
                                 bucket, blob_name, 'sites', gcp_logger)
    elif blob_name.startswith(ENLIGHTEN_STORAGE_PATH_PREFIX):
        handle_enlighten_blob(data, context, storage_client,
                              bucket, blob_name, 'sites', gcp_logger)
    elif blob_name.startswith(LEMS_STORAGE_PATH_PREFIX):
        handle_lems_blob(data, context, storage_client,
                         bucket, blob_name, 'sites', gcp_logger)
    else:
        gcp_logger.debug(
            'Skipping storage event event_id=%s, event_type=%s', context.event_id, context.event_type)

    return ('', 200)
