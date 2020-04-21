import logging
from os import listdir
from os.path import isfile, join

import pandas as pd

from app.nem12 import Nem12Merger, handle_nem12_blob_merged

from . import GCP_STORAGE_BUCKET_ID, init_firestore_client, init_storage_client

NEM12_IN_PATH = 'fixtures/nem12/in'
NEM12_MERGED_PATH = 'fixtures/nem12/merged'
NEM12_IN_FILES = [join(NEM12_IN_PATH, f) for f in listdir(
    NEM12_IN_PATH) if isfile(join(NEM12_IN_PATH, f))]
NEM12_MERGED_FILES = [join(NEM12_MERGED_PATH, f) for f in listdir(
    NEM12_MERGED_PATH) if isfile(join(NEM12_MERGED_PATH, f))]


def test_nem12_parsing():
    # when
    merger = Nem12Merger(NEM12_IN_FILES)
    nmi_meter_registers = merger.nmi_meter_registers

    # then
    nmis = set([nmr.nmi for nmr in nmi_meter_registers])
    assert sorted(list(nmis)) == ['6123456789', '6408091979']


def test_handle_nem12_blob_merged():
    # given
    storage_client = init_storage_client()
    fdb = init_firestore_client()
    blob_name = 'nem12/merged/nem12_6408091979_small.csv'
    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)
    logger = logging.getLogger()

    # when
    handle_nem12_blob_merged(None, None, storage_client,
                             bucket, blob_name, 'test_sites', logger)

    # then
    dailies = [d for d in fdb.collection('test_sites').document(
        '6408091979').collection('dailies').stream()]
    assert len(dailies) == 108


def test_flatten_data():
    # when
    nem12_files = ['fixtures/nem12/merged/nem12_6408091979_small.csv']
    merger = Nem12Merger(nem12_files)
    flattened = merger.flatten_data()
    dfm = pd.DataFrame(flattened)
    dfm.to_pickle('fixtures/nem12/test_flatten_data.pkl')

    df_result = pd.read_pickle('fixtures/nem12/test_flatten_data.pkl')
    assert len(df_result.index) == len(dfm.index)
