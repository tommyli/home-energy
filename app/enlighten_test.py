from datetime import datetime
import time
import logging
import json

from . import ENLIGHTEN_SYSTEM_ID
from . import ENLIGHTEN_API_KEY
from . import ENLIGHTEN_USER_ID
from app.enlighten import get_enlighten_stats_resp


def test_get_enlighten_stats_resp():
    # when
    actual_resp = get_enlighten_stats_resp(
        ENLIGHTEN_API_KEY, ENLIGHTEN_USER_ID, ENLIGHTEN_SYSTEM_ID, datetime(2020, 1, 1))
    actual_data = json.loads(actual_resp.text)

    # then
    assert actual_data.get('system_id') == 597188
