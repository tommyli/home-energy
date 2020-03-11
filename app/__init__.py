import os

ENLIGHTEN_URL = os.environ.get(
    'ENLIGHTEN_URL', 'ENLIGHTEN_URL not set.')
ENLIGHTEN_SYSTEM_ID = os.environ.get(
    'ENLIGHTEN_SYSTEM_ID', 'ENLIGHTEN_SYSTEM_ID not set.')
ENLIGHTEN_API_KEY = os.environ.get(
    'ENLIGHTEN_API_KEY', 'ENLIGHTEN_API_KEY not set.')
ENLIGHTEN_USER_ID = os.environ.get(
    'ENLIGHTEN_USER_ID', 'ENLIGHTEN_USER_ID not set.')
ENLIGHTEN_DATA_MIN_DATE = os.environ.get(
    'ENLIGHTEN_DATA_MIN_DATE', 'ENLIGHTEN_DATA_MIN_DATE not set.')
ENLIGHTEN_STORAGE_PATH_PREFIX = os.environ.get(
    'ENLIGHTEN_STORAGE_PATH_PREFIX', 'ENLIGHTEN_STORAGE_PATH_PREFIX not set.')

LEMS_URL = os.environ.get(
    'LEMS_URL', 'LEMS_URL not set.')
LEMS_USER = os.environ.get(
    'LEMS_USER', 'LEMS_USER not set.')
LEMS_PASSWORD = os.environ.get(
    'LEMS_PASSWORD', 'LEMS_PASSWORD not set.')
LEMS_BATTERY_ID = os.environ.get(
    'LEMS_BATTERY_ID', 'LEMS_BATTERY_ID not set.')
LEMS_STORAGE_PATH_PREFIX = os.environ.get(
    'LEMS_STORAGE_PATH_PREFIX', 'LEMS_STORAGE_PATH_PREFIX not set.')
LEMS_DATA_MIN_DATE = os.environ.get(
    'LEMS_DATA_MIN_DATE', 'LEMS_DATA_MIN_DATE not set.')

NEM12_STORAGE_PATH_IN = os.environ.get(
    'NEM12_STORAGE_PATH_IN', 'NEM12_STORAGE_PATH_IN not set.')
NEM12_STORAGE_PATH_MERGED = os.environ.get(
    'NEM12_STORAGE_PATH_MERGED', 'NEM12_STORAGE_PATH_MERGED not set.')

GCP_STORAGE_BUCKET_ID = os.environ.get(
    'GCP_STORAGE_BUCKET_ID', 'GCP_STORAGE_BUCKET_ID not set.')

storage_client = None
firestore_client = None
gcp_logger = None


def init_storage_client():
    from google.cloud import storage

    global storage_client

    if storage_client:
        return storage_client

    storage_client = storage.Client()

    return storage_client


def init_firestore_client():
    from google.cloud import firestore

    global firestore_client

    if firestore_client:
        return firestore_client

    firestore_client = firestore.Client()

    return firestore_client


def init_gcp_logger():
    import logging
    from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging
    import google.cloud.logging as gcp_logging

    global gcp_logger

    if gcp_logger:
        return gcp_logger

    GCP_LOG_CLIENT = gcp_logging.Client()
    GCP_LOG_HANDLER = CloudLoggingHandler(GCP_LOG_CLIENT)
    gcp_logger = logging.getLogger()
    gcp_logger.setLevel(logging.INFO)
    gcp_logger.addHandler(GCP_LOG_HANDLER)

    return gcp_logger
