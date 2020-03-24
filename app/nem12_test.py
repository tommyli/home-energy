from os import listdir
from os.path import isfile, join
from app.nem12 import Nem12Merger
import pandas as pd
import pickle
import time
import logging

from . import GCP_STORAGE_BUCKET_ID
from . import init_firestore_client
from . import init_storage_client
from app.nem12 import handle_nem12_blob_merged

nem12_in_path = 'fixtures/nem12/in'
nem12_merged_path = 'fixtures/nem12/merged'
nem12_in_files = [join(nem12_in_path, f) for f in listdir(
    nem12_in_path) if isfile(join(nem12_in_path, f))]
nem12_merged_files = [join(nem12_merged_path, f) for f in listdir(
    nem12_merged_path) if isfile(join(nem12_merged_path, f))]


def test_nem12_parsing():
    # when
    merger = Nem12Merger(nem12_in_files)
    nmi_meter_registers = merger.nmi_meter_registers

    # then
    nmis = set([nmr.nmi for nmr in nmi_meter_registers])
    assert sorted(list(nmis)) == ['6123456789', '6408091979']


def test_handle_nem12_blob_merged():
    # given
    storage_client = init_storage_client()
    db = init_firestore_client()
    blob_name = 'nem12/merged/nem12_6408091979_small.csv'
    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)
    logger = logging.getLogger()

    # when
    handle_nem12_blob_merged(None, None, storage_client,
                             bucket, blob_name, 'test_sites', logger)

    # then
    dailies = [d for d in db.collection('test_sites').document(
        '6408091979').collection('dailies').stream()]
    assert len(dailies) == 83


def test_flatten_data():
    # when
    nem12_files = ['fixtures/nem12/merged/nem12_6408091979_small.csv']
    merger = Nem12Merger(nem12_files)
    flattened = merger.flatten_data()
    df = pd.DataFrame(flattened)
    df.to_pickle('fixtures/nem12/test_flatten_data.pkl')

    df_result = pd.read_pickle('fixtures/nem12/test_flatten_data.pkl')
    assert len(df_result.index) == len(df.index)
