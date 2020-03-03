from datetime import datetime
from operator import itemgetter
import itertools
import requests
import sys


def get_already_fetched(storage_client, bucket, already_fetched_size_threshold_bytes):
    all_blobs = ({'name': b.name, 'size': b.size}
                 for b in storage_client.list_blobs(bucket))
    sorted_by_size = sorted(all_blobs, key=itemgetter('size'))
    grouped_by_size_more_than_threshold = itertools.groupby(
        sorted_by_size, lambda b: b.get('size') > already_fetched_size_threshold_bytes)
    already_fetched_group = next(
        (i for i in grouped_by_size_more_than_threshold if (i[0])), (True, iter([])))
    already_fetched = already_fetched_group[1]

    return list(already_fetched)


def get_enlighten_stats_resp(api_key, user_id, system_id, as_of_date):
    query = {'key': api_key, 'user_id': user_id,
             'datetime_format': 'iso8601', 'start_at': f"{int(as_of_date.timestamp())}"}
    resp = requests.get(
        f"https://api.enphaseenergy.com/api/v2/systems/{system_id}/stats", params=query)

    return resp
