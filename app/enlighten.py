import requests
import sys
from . import ENLIGHTEN_URL


def get_enlighten_stats_resp(api_key, user_id, system_id, as_of_date):
    path = f"/api/v2/systems/{system_id}/stats"
    query = {'key': api_key, 'user_id': user_id,
             'datetime_format': 'iso8601', 'start_at': f"{int(as_of_date.timestamp())}"}
    resp = requests.get(f"{ENLIGHTEN_URL}{path}", params=query)

    return resp
