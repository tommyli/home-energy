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

NEM12_DATA_MIN_DATE = os.environ.get(
    'NEM12_DATA_MIN_DATE', 'NEM12_DATA_MIN_DATE not set.')
NEM12_STORAGE_PATH_IN = os.environ.get(
    'NEM12_STORAGE_PATH_IN', 'NEM12_STORAGE_PATH_IN not set.')
NEM12_STORAGE_PATH_MERGED = os.environ.get(
    'NEM12_STORAGE_PATH_MERGED', 'NEM12_STORAGE_PATH_MERGED not set.')

GCP_STORAGE_BUCKET_ID = os.environ.get(
    'GCP_STORAGE_BUCKET_ID', 'GCP_STORAGE_BUCKET_ID not set.')

NMI = os.environ.get('NMI', 'NMI not set.')

VIEWBANK_WEATHER_URL = 'https://reg.bom.gov.au/fwo/IDV60901/IDV60901.95874.json'
SCORESBY_WEATHER_URL = 'https://reg.bom.gov.au/fwo/IDV60901/IDV60901.95867.json'

STORAGE_CLIENT = None
FIRESTORE_CLIENT = None
GCP_LOGGER = None


def init_storage_client():
    from google.cloud import storage

    global STORAGE_CLIENT

    if STORAGE_CLIENT:
        return STORAGE_CLIENT

    STORAGE_CLIENT = storage.Client()

    return STORAGE_CLIENT


def init_firestore_client():
    from google.cloud import firestore

    global FIRESTORE_CLIENT

    if FIRESTORE_CLIENT:
        return FIRESTORE_CLIENT

    FIRESTORE_CLIENT = firestore.Client()

    return FIRESTORE_CLIENT


def init_gcp_logger():
    import logging
    from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging
    import google.cloud.logging as gcp_logging

    global GCP_LOGGER

    if GCP_LOGGER:
        return GCP_LOGGER

    GCP_LOG_CLIENT = gcp_logging.Client()
    GCP_LOG_HANDLER = CloudLoggingHandler(GCP_LOG_CLIENT)
    GCP_LOGGER = logging.getLogger()
    GCP_LOGGER.setLevel(logging.INFO)
    GCP_LOGGER.addHandler(GCP_LOG_HANDLER)

    return GCP_LOGGER
