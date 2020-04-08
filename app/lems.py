import base64
import os
import sys

import requests

from . import LEMS_URL


def get_lems_data_resp(user_id, password, batter_id, as_of_date):
    auth = base64.b64encode(
        f"{user_id}:{password}".encode()).decode("utf-8")
    headers = {"Authorization": f"Basic {auth}"}

    path = f"/api/Battery/{batter_id}/soc/data"
    query = {'MinDate': as_of_date, 'Hours': '24'}

    resp = requests.get(f"{LEMS_URL}{path}", headers=headers, params=query)

    return resp
