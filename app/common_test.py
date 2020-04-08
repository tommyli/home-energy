from datetime import date, datetime, timedelta
from unittest.mock import Mock

from app.common import idate_range


def test_idate_range():
    from_date = datetime.fromisoformat('2020-01-01T00:00:00')
    to_date = datetime.fromisoformat('2020-01-03T00:00:00')
    actual_result = list(idate_range(from_date, to_date))

    assert actual_result == [
        datetime.fromisoformat('2020-01-01T00:00:00'),
        datetime.fromisoformat('2020-01-02T00:00:00'),
        datetime.fromisoformat('2020-01-03T00:00:00'),
    ]
