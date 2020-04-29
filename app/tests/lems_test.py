import logging
from datetime import datetime

import pandas as pd

from app.lems import get_lems_data_resp, handle_lems_blob

from .. import (GCP_STORAGE_BUCKET_ID, LEMS_BATTERY_ID, LEMS_PASSWORD,
                LEMS_USER, init_firestore_client, init_storage_client)


def test_get_lems_data_resp():
    # when
    actual_resp = get_lems_data_resp(
        LEMS_USER, LEMS_PASSWORD, LEMS_BATTERY_ID, datetime(2019, 4, 7))
    dfm = pd.DataFrame(actual_resp.json())
    dfm.to_pickle(
        'fixtures/lems/test_get_lems_data_resp.pkl')

    # then
    assert dfm['BatteryId'].iloc[0] == LEMS_BATTERY_ID


def test_handle_lems_blob():
    # setup
    fdb = init_firestore_client()
    storage_client = init_storage_client()
    logger = logging.getLogger()

    # given
    blob_name = 'lems/2019/lems_data_20190406.csv'
    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)

    # when
    handle_lems_blob(None, None, storage_client,
                     bucket, blob_name, 'test_sites', logger)

    # then
    yesterday_doc = fdb.collection(
        'test_sites/6408091979/dailies').document('20190405').get()
    assert yesterday_doc.exists

    # tear down
    yesterday_doc = fdb.collection(
        'test_sites/6408091979/dailies').document('20190405').delete()
