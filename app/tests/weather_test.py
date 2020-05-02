from datetime import datetime, timedelta

from app import NMI, init_firestore_client
from app import weather as under_test


def test_daily_temperatures_to_db():
    # setup
    fdb = init_firestore_client()

    # when
    under_test.daily_temperatures_to_db('test_sites')

    # then
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    yesterday_doc = fdb.collection(
        f"test_sites/{NMI}/dailies").document(yesterday_str).get()

    assert yesterday_doc.exists
    min_temp = yesterday_doc.get('min_temperature_c')
    max_temp = yesterday_doc.get('max_temperature_c')
    assert max_temp > min_temp
    assert min_temp > -50 and min_temp < 50
    assert max_temp > -50 and max_temp < 50

    # tear down
    yesterday_doc = fdb.collection(
        "test_sites/{NMI}/dailies").document(yesterday_str).delete()
