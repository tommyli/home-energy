import os

import numpy as np
import pandas as pd
from google.cloud.storage import Blob

from app import (GCP_STORAGE_BUCKET_ID, NMI, init_firestore_client,
                 init_gcp_logger, init_storage_client)


def on_http_fetch_dailies(request):
    gcp_logger = init_gcp_logger()
    gcp_logger.info('on_http_fetch_dailies(), args=%s', request.args)
    storage_client = init_storage_client()
    fdb = init_firestore_client()

    columns = ['meter_consumptions_kwh', 'meter_generations_kwh',
               'solar_generations_kwh', 'solar_mean_powrs_kw', 'solar_devices_reportings',
               'capacities_kw', 'charge_quantities_kwh', 'deterioration_states_pct',
               'discharge_quantities_kwh', 'power_at_charges_kw', 'residual_capacities_pct',
               'total_charge_quantities_kwh', 'total_discharge_quantities_kwh'
               ]

    df_all_dailies = pd.DataFrame(
        index=pd.DatetimeIndex([]), columns=columns)
    missing_values = np.zeros(48)

    for doc in fdb.collection(f"sites/{NMI}/dailies").order_by('interval_date', direction='ASCENDING').stream():
        doc_fields = list(doc.to_dict().keys())
        if 'interval_date' not in doc_fields:
            gcp_logger.info('Missing field interval_date')
            continue

        gcp_logger.info('Processing interval_date=%s',
                        doc.get('interval_date'))

        doc_dict = {}
        for column in columns:
            doc_dict[column] = np.array(
                doc.get(column)) if column in doc_fields else missing_values

        df_all_dailies.loc[doc.get('interval_date')] = doc_dict

    pkl_file_name = f"dailies_{NMI}.pkl"
    pkl_file_path = f"/tmp/{pkl_file_name}"
    df_all_dailies.to_pickle(f"/tmp/{pkl_file_name}")

    bucket = storage_client.get_bucket(GCP_STORAGE_BUCKET_ID)
    blob = Blob(pkl_file_name, bucket)
    with open(pkl_file_path, "rb") as pkl_file:
        blob.upload_from_file(pkl_file)

    os.remove(pkl_file_path)

    return ('', 200)
