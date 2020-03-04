from datetime import datetime, timedelta, date
from operator import itemgetter
import itertools
import sys


def idate_range(start_date, end_date):
    all_dates_from_min = (start_date + timedelta(days=n)
                          for n in range(sys.maxsize))
    min_to_max = itertools.takewhile(
        lambda d: d <= end_date, all_dates_from_min)
    for d in min_to_max:
        yield d


def get_already_fetched(storage_client, bucket, prefix, already_fetched_size_threshold_bytes):
    all_blobs = ({'name': b.name, 'size': b.size}
                 for b in storage_client.list_blobs(bucket_or_name=bucket, prefix=prefix))
    sorted_by_size = sorted(all_blobs, key=itemgetter('size'))

    grouped_by_size_more_than_threshold = itertools.groupby(
        sorted_by_size, lambda b: b.get('size') > already_fetched_size_threshold_bytes)
    already_fetched_group = next(
        (i for i in grouped_by_size_more_than_threshold if (i[0])), (True, iter([])))
    already_fetched = already_fetched_group[1]

    return list(already_fetched)
