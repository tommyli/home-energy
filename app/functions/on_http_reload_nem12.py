from .. import GCP_STORAGE_BUCKET_ID, NMI, init_gcp_logger, init_storage_client
from ..nem12 import handle_nem12_blob_merged


def on_http_reload_nem12(request):
    gcp_logger = init_gcp_logger()
    gcp_logger.info('on_http_reload_nem12(), args=%s', request.args)

    storage_client = init_storage_client()
    blob_name = f"nem12/merged/nem12_{NMI}.csv"
    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)
    gcp_logger = init_gcp_logger()

    handle_nem12_blob_merged(None, None, storage_client,
                             bucket, blob_name, 'sites', gcp_logger)

    return ('', 200)
