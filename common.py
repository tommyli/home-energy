from datetime import datetime, timedelta, date
import itertools
import sys


def idate_range(start_date, end_date):
    all_dates_from_min = (start_date + timedelta(days=n)
                          for n in range(sys.maxsize))
    min_to_max = itertools.takewhile(
        lambda d: d <= end_date, all_dates_from_min)
    for d in min_to_max:
        yield d
