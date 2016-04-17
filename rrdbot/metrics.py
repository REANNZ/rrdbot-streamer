# Python 2 / 3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import logging

from . import config


Metric = collections.namedtuple(
    "Metric",
    ("reference", "timestamp", "interval_seconds", "value", "rate", "delta",
     "type")
)


# Absolute -> time, interval, raw, rate=raw/interval
# Gauge -> time, interval, raw, rate=raw
# Counter -> time, interval, raw, delta=raw-old[rollover], rate=delta/interval
# Derive -> time, interval, raw, delta=raw-old, rate=delta/interval


class MetricProcessor(object):

    __slots__ = ("config_item", "last_timestamp", "last_raw_value")

    TYPE = None

    def __init__(self, config_item):
        self.config_item = config_item
        self.last_timestamp = None
        self.last_raw_value = None

    def __call__(self, raw_file_line):
        assert self.reference == raw_file_line.reference
        if self.reference != raw_file_line.reference:
            return None

        new_value = None
        if raw_file_line.value != config.VAL_UNKNOWN:
            new_value = self.parse_value(raw_file_line.value)

        interval_seconds = None
        new_timestamp = raw_file_line.timestamp
        if self.last_timestamp is not None:
            interval_seconds = new_timestamp - self.last_timestamp

        rate, delta = \
            self.process(interval_seconds, self.last_raw_value, new_value)
        rate = self.apply_limits(rate)

        self.last_timestamp = new_timestamp
        self.last_raw_value = new_value

        return Metric(
            raw_file_line.reference, new_timestamp, interval_seconds, new_value,
            rate, delta, self.TYPE
        )

    @classmethod
    def parse_value(cls, value):
        if value == '':
            return None
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                logger = logging.getLogger()
                logger.warning("Invalid raw value : %s", value)

    def process(self, interval_seconds, last_raw_value, new_value):
        raise NotImplementedError()

    @property
    def reference(self):
        return self.config_item.reference

    @property
    def min(self):
        return self.config_item.min

    @property
    def max(self):
        return self.config_item.max

    # See rrdtool/src/rrd_update.c update_pdp_prep
    def apply_limits(self, rate):
        if rate is not None:
            if self.max != config.VAL_UNKNOWN and rate > self.max:
                rate = None
            elif self.min != config.VAL_UNKNOWN and rate < self.min:
                rate = None
        return rate


class FloatMetricProcessor(MetricProcessor):

    def process(self, interval_seconds, last_raw_value, new_value):
        raise NotImplementedError()


# See rrdtool/src/rrd_update.c update_pdp_prep case DST_ABSOLUTE
class AbsoluteMetricProcessor(FloatMetricProcessor):

    TYPE = config.VAL_ABSOLUTE

    #newval = rrd_strtodbl()
    #rate = newval / interval;

    def process(self, interval_seconds, last_raw_value, new_value):
        rate = None
        if interval_seconds is not None and new_value is not None:
            rate = new_value / interval_seconds
        return rate, None


# See rrdtool/src/rrd_update.c update_pdp_prep case DST_GAUGE
class GaugeMetricProcessor(FloatMetricProcessor):

    TYPE = config.VAL_GAUGE

    #newval = rrd_strtodbl()
    #rate = newval;

    def process(self, interval_seconds, last_raw_value, new_value):
        rate = new_value
        return rate, None


# See rrdtool/src/rrd_diff.c rrd_diff
class IntegerMetricProcessor(MetricProcessor):

    @classmethod
    def parse_value(cls, value):
        value = super(IntegerMetricProcessor, cls).parse_value(value)
        if value is not None and isinstance(value, float):
            # Truncate float value just like rrd_diff does
            value = int(value)
        return value

    def process(self, interval_seconds, last_raw_value, new_value):
        raise NotImplementedError()


# See rrdtool/src/rrd_update.c update_pdp_prep case DST_COUNTER
#                 rrd_diff.c rrd_diff
class CounterMetricProcessor(IntegerMetricProcessor):

    TYPE = config.VAL_COUNTER

    #pdp_new[ds_idx] = rrd_diff() [truncate floats]
    #if (pdp_new[ds_idx] < (double) 0.0)
    #pdp_new[ds_idx] += (double) 4294967295.0; /* 2^32-1 */
    #if (pdp_new[ds_idx] < (double) 0.0)
    #pdp_new[ds_idx] += (double) 18446744069414584320.0; /* 2^64-2^32 */
    #rate = pdp_new[ds_idx] / interval;

    def process(self, interval_seconds, last_raw_value, new_value):
        rate = None
        delta = None
        if last_raw_value is not None and new_value is not None:
            delta = new_value - last_raw_value
            if delta < 0:
                delta += 4294967295 # 2^32-1
            if delta < 0:
                delta += 18446744069414584320 # 2^64-2^32
            rate = delta / interval_seconds
        return rate, delta


# See rrdtool/src/rrd_update.c update_pdp_prep case DST_DERIVE
#                 rrd_diff.c rrd_diff
class DeriveMetricProcessor(IntegerMetricProcessor):

    TYPE = config.VAL_DERIVE

    #pdp_new[ds_idx] = rrd_diff() [truncate floats]
    #rate = pdp_new[ds_idx] / interval;

    def process(self, interval_seconds, last_raw_value, new_value):
        rate = None
        delta = None
        if last_raw_value is not None and new_value is not None:
            delta = new_value - last_raw_value
            if interval_seconds > 0:
                rate = delta / interval_seconds
        return rate, delta
