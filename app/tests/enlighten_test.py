import json
import logging
from datetime import datetime

import pandas as pd

from app.enlighten import get_enlighten_stats_resp, handle_enlighten_blob

from .. import (ENLIGHTEN_API_KEY, ENLIGHTEN_SYSTEM_ID, ENLIGHTEN_USER_ID,
                GCP_STORAGE_BUCKET_ID, init_firestore_client,
                init_storage_client)


def test_get_enlighten_stats_resp():
    # when
    actual_resp = get_enlighten_stats_resp(
        ENLIGHTEN_API_KEY, ENLIGHTEN_USER_ID, ENLIGHTEN_SYSTEM_ID, datetime(2019, 4, 7))
    actual_data = json.loads(actual_resp.text)
    df_intervals = pd.DataFrame(actual_data.get('intervals'))
    df_intervals.to_pickle(
        'fixtures/enlighten/test_get_enlighten_stats_resp.pkl')

    # then
    assert actual_data.get('system_id') == 597188


def test_handle_enlighten_blob():
    # setup
    fdb = init_firestore_client()
    storage_client = init_storage_client()
    logger = logging.getLogger()

    # given
    blob_name = 'enlighten/2019/enlighten_stats_20190407.json'
    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)

    # when
    handle_enlighten_blob(None, None, storage_client,
                          bucket, blob_name, 'test_sites', logger)

    # then
    doc = fdb.collection(
        'test_sites/6408091979/dailies').document('20190407').get()
    assert doc.exists

    # tear down
    doc = fdb.collection(
        'test_sites/6408091979/dailies').document('20190407').delete()
