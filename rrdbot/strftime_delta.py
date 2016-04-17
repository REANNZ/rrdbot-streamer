# Python 2 / 3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime


class InterleaveError(Exception):
    """Filename template will produce a non-interleaving sequence of
    filenames."""
    pass


class DatetimeIncrementer(object):
    # pylint: disable=too-few-public-methods

    def __radd__(self, base_datetime):
        raise NotImplementedError()

    def __rsub__(self, base_datetime):
        raise NotImplementedError()


class DatetimeDeltaIncrementer(object):
    # pylint: disable=too-few-public-methods

    def __init__(self, **kwargs):
        self.timedelta = datetime.timedelta(**kwargs)

    def __radd__(self, base_datetime):
        return base_datetime + self.timedelta

    def __rsub__(self, base_datetime):
        return base_datetime - self.timedelta

    def __str__(self):
        return str(self.timedelta)


class DatetimeMonthIncrementer(object):
    # pylint: disable=too-few-public-methods

    def __radd__(self, base_datetime):
        if base_datetime.month == 12:
            return base_datetime.replace(year=base_datetime.year+1, month=1)
        else:
            return base_datetime.replace(month=base_datetime.month+1)

    def __rsub__(self, base_datetime):
        if base_datetime.month == 1:
            return base_datetime.replace(year=base_datetime.year-1, month=12)
        else:
            return base_datetime.replace(month=base_datetime.month-1)

    def __str__(self):
        return "1 month"


class DatetimeYearIncrementer(object):
    # pylint: disable=too-few-public-methods

    def __radd__(self, base_datetime):
        return base_datetime.replace(year=base_datetime.year+1)

    def __rsub__(self, base_datetime):
        return base_datetime.replace(year=base_datetime.year-1)

    def __str__(self):
        return "1 year"


# pylint: disable=invalid-name
def get_datetime_incrementer(strftime_template):

    # List of time increment functions to check for changes to output of
    # strftime, which  covers the full set of format directives, excluding %Z
    # which is not applicable. Noted are the format directives each incrementer
    # is *guaranteed* to change. Ordered from smallest to largest.
    incrementers = []
    incrementers.append(DatetimeDeltaIncrementer(seconds=1)) # Second [S]
    incrementers.append(DatetimeDeltaIncrementer(minutes=1)) # Minute [c,M,X]
    incrementers.append(DatetimeDeltaIncrementer(hours=1))   # Hour   [c,H,I,X]
    incrementers.append(DatetimeDeltaIncrementer(hours=12))  # AM/PM  [c,H,p]
    incrementers.append(DatetimeDeltaIncrementer(days=1))    # Day    [a,A,c,d,j,w,x]
    incrementers.append(DatetimeMonthIncrementer())          # Month  [b,B,c,j,m,U,W,x]
    incrementers.append(DatetimeYearIncrementer())           # Year   [a,A,c,w,x,y,Y]

    # Specially chosen date where applying deltas will not cause larger
    # components to rollover e.g. adding a minute will not rollover the hour,
    # AM/PM, day, month or year
    test_datetime = datetime.datetime(
        year=2001, month=1, day=1, hour=6, minute=1, second=1
    )

    base = test_datetime.strftime(strftime_template)
    smallest_incrementer = None
    for incrementer in incrementers:
        check_time = test_datetime + incrementer
        check = check_time.strftime(strftime_template)
        if check != base:
            if smallest_incrementer is None:
                smallest_incrementer = incrementer
        elif smallest_incrementer is not None:
            raise InterleaveError()

    return smallest_incrementer
