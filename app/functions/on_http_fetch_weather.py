from app import init_gcp_logger
from app.weather import daily_temperatures_to_db


def on_http_fetch_daily_temperatures(request):
    gcp_logger = init_gcp_logger()
    gcp_logger.info('on_http_get_enlighten_data(), args=%s', request.args)

    daily_temperatures_to_db('sites')

    return ('', 200)
# %%
