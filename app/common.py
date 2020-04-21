import itertools
import sys
from datetime import timedelta, timezone
from operator import itemgetter

import pandas as pd
from pytz import timezone as pytz_timezone

from . import init_firestore_client

LOCAL_TZ = pytz_timezone('Australia/Melbourne')
AEST_OFFSET = timezone(pd.Timedelta('10 hours'))


def idate_range(start_date, end_date):
    all_dates_from_min = (start_date + timedelta(days=n)
                          for n in range(sys.maxsize))
    min_to_max = itertools.takewhile(
        lambda d: d <= end_date, all_dates_from_min)
    for date in min_to_max:
        yield date


def get_already_fetched(storage_client, bucket, prefix, already_fetched_size_threshold_bytes):
    all_blobs = ({'name': b.name, 'size': b.size}
                 for b in storage_client.list_blobs(bucket_or_name=bucket, prefix=prefix))
    sorted_by_size = sorted(all_blobs, key=itemgetter('size'))

    grouped_by_size_more_than_threshold = itertools.groupby(
        sorted_by_size, lambda b: b.get('size') > already_fetched_size_threshold_bytes)
    already_fetched_group = next(
        (i for i in grouped_by_size_more_than_threshold if i[0]), (True, iter([])))
    already_fetched = already_fetched_group[1]

    return list(already_fetched)


def merge_df_to_db(nmi, dfm, root_collection_name, logger):
    """
    df must have index ['interval_date']
    interval_length must be 30 mins so there should be 48 array values for each day
    all values must be normalised to kWh
    """

    assert 'interval_date' in dfm.index.names, f"Provided data frame must contain index name 'interval_date' but got {dfm.index.names}"

    logger.info('merge_df_to_db(%s)', nmi)

    fdb = init_firestore_client()

    interval_length = 30
    uom = 'KWH'

    site_doc = fdb.collection(root_collection_name).document(nmi)
    site_doc.set({
        'nmi': nmi,
        'name': 'Home',
        'interval_length': interval_length,
        'uom': uom,
    }, merge=True)

    # There is a limit of 500 on the number of batch writes
    # Write one year of data at a time
    first_year = dfm.index.get_level_values(
        'interval_date')[0].year
    last_year = dfm.index.get_level_values(
        'interval_date')[-1].year

    for year in range(first_year, last_year + 1):
        df_year = dfm[str(year)]
        date_key_dict = df_year.to_dict('index')

        batch = fdb.batch()

        for interval_date in date_key_dict:
            doc_data = {'interval_date': interval_date,
                        **(date_key_dict.get(interval_date))}
            daily_doc = site_doc.collection(
                'dailies').document(interval_date.strftime('%Y%m%d'))
            batch.set(daily_doc, doc_data, merge=True)

        batch.commit()
