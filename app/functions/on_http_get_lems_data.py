from datetime import datetime, timedelta, date
from pathlib import Path
import csv
import os
import sys

from ..common import idate_range
from .. import init_gcp_logger
from .. import init_storage_client
from .. import LEMS_USER, LEMS_PASSWORD, LEMS_BATTERY_ID


def on_http_get_lems_data(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    gcp_logger = init_gcp_logger()
    gcp_logger.info(f"on_http_get_lems_data()")
    storage_client = init_storage_client()

    return ('', 200)
