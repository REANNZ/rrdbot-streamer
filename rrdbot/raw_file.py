# Python 2 / 3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import datetime
import gzip
import io
import logging
import os.path
import time

# Python 2 / 3 compatibility
try:
    import queue
except ImportError:
    # pylint: disable=wrong-import-order
    import Queue as queue

import pyinotify

from . import watch


# Chunk size when seeking backwards searching for beginning of line.
REVERSE_SEEK_LENGTH = 128

# Mask used for add_watch on rrdbot raw files.
# pylint: disable=no-member
RAW_FILE_INOTIFY_MASK = pyinotify.IN_MODIFY


RawFileLine = collections.namedtuple(
    "RawFileLine",
    ("timestamp", "reference", "value")
)


class RawFileReader(object):
    # pylint: disable=too-many-instance-attributes

    __slots__ = (
        "watch_manager", "raw_file_template", "metric_processor_map",
        "metric_queue", "raw_file_datetime_incrementer", "raw_file_datetime",
        "raw_file_type", "raw_file_path", "raw_file_handle", "watch_descriptor",
        "watch_descriptor_next"
    )

    RawFileType = collections.namedtuple(
        "RawFileType",
        ("ext", "open", "random_access")
    )
    RAW_FILE_TYPES = [
        RawFileType('', io.open, True),
        RawFileType('.gz', gzip.open, False)
    ]

    # pylint: disable=too-many-arguments
    def __init__(self, watch_manager, metric_processor_map, metric_queue,
                 raw_file_template, raw_file_datetime_incrementer,
                 raw_file_datetime=None):
        self.watch_manager = watch_manager
        self.metric_processor_map = metric_processor_map
        self.metric_queue = metric_queue
        self.raw_file_template = raw_file_template
        self.raw_file_datetime_incrementer = raw_file_datetime_incrementer
        self.raw_file_datetime = raw_file_datetime
        self.raw_file_type = None
        self.raw_file_path = None
        self.raw_file_handle = None
        self.watch_descriptor = None
        self.watch_descriptor_next = None

    def open(self, raw_file_datetime=None, watch_file=None):
        if self.is_open:
            raise RuntimeError("Already open")

        logger = logging.getLogger()

        if raw_file_datetime is None:
            raw_file_datetime = datetime.datetime.now()
        self.raw_file_datetime = raw_file_datetime

        raw_file_path = self.raw_file_path_at(raw_file_datetime)
        self.raw_file_path = raw_file_path

        for raw_file_type in self.RAW_FILE_TYPES:
            if os.path.exists(raw_file_path + raw_file_type.ext):
                raw_file_path += raw_file_type.ext
                self.raw_file_path = raw_file_path
                self.raw_file_type = raw_file_type
                break
        else:
            logger.info("  Raw file %s not found", raw_file_path)
            return False

        if watch_file is None:
            watch_file = self.is_raw_file_path_now

        if watch_file:
            self.watch()

        try:
            # Binary I/O is significantly faster - i.e. 2 orders of magnitude.
            # https://docs.python.org/2/library/io.html#id1
            raw_file_handle = self.raw_file_type.open(self.raw_file_path, 'rb')
        except EnvironmentError as ex:
            logger.warning("  Failed to open %s : %s", self.raw_file_path, ex)
        else:
            logger.info("  Opened raw file %s", self.raw_file_path)
            self.raw_file_handle = raw_file_handle

        return True

    def watch(self):
        watch_descriptor = self._add_watch(self.raw_file_path)
        if self.watch_descriptor != watch_descriptor:
            if self.watch_descriptor is not None:
                watch.unwatch(self.watch_manager, self.watch_descriptor)
            self.watch_descriptor = watch_descriptor

        raw_file_path_next = self.raw_file_path_next()
        if raw_file_path_next is not None:
            watch_descriptor_next = self._add_watch(raw_file_path_next)
            if self.watch_descriptor_next != watch_descriptor_next:
                if self.watch_descriptor_next is not None:
                    watch.unwatch(
                        self.watch_manager, self.watch_descriptor_next
                    )
                self.watch_descriptor_next = watch_descriptor_next

    def close(self):
        if self.watch_descriptor is not None:
            watch.unwatch(
                self.watch_manager, self.watch_descriptor, self.raw_file_path
            )
        if self.watch_descriptor_next is not None:
            watch.unwatch(self.watch_manager, self.watch_descriptor_next)
        if self.raw_file_handle is not None:
            self.raw_file_handle.close()
            self.raw_file_handle = None
            logger = logging.getLogger()
            logger.info("  Closed raw file %s", self.raw_file_path)
        self.raw_file_type = None

    @property
    def is_open(self):
        return (
            self.raw_file_handle is not None and not self.raw_file_handle.closed
        )

    @property
    def is_watched(self):
        return self.watch_descriptor is not None

    def seek_to_end(self):
        self.raw_file_handle.seek(0, os.SEEK_END)

    def seek_to_datetime(self, target_datetime):
        timestamp = time.mktime(target_datetime.timetuple())
        if self.raw_file_type.random_access:
            seek_to_timestamp(self.raw_file_handle, timestamp)
        else:
            line = read_to_timestamp(self.raw_file_handle, timestamp)
            if line is not None:
                self._process_line(line)

    def rollover(self, watch_file=None):
        # Ensure watch_descriptor_next is not unwatched in close()
        watch_descriptor_next = self.watch_descriptor_next
        self.watch_descriptor_next = None

        if self.is_open:
            self.process_lines_until_eof(False)
            self.close()

        raw_file_datetime_next = self.raw_file_datetime_next()
        if self.open(raw_file_datetime_next, watch_file=watch_file):
            self.process_lines_until_eof(True)
        else:
            self.watch_descriptor = watch_descriptor_next

    def _add_watch(self, raw_file_path):
        if not os.path.exists(raw_file_path):
            return None

        # Check for existing watch first.
        watch_descriptor = self.watch_manager.get_wd(raw_file_path)
        if watch_descriptor is not None:
            return watch_descriptor

        try:
            wdd = self.watch_manager.add_watch(
                raw_file_path, RAW_FILE_INOTIFY_MASK, quiet=False
            )
        except pyinotify.WatchManagerError as ex:
            logger.warning("  %s", ex.message)
            watch_descriptor = None
        else:
            watch_descriptor = wdd[raw_file_path]
            logger = logging.getLogger()
            logger.debug(
                "  Watching %s for modification [%s]",
                raw_file_path, watch_descriptor
            )

        return watch_descriptor

    def raw_file_path_at(self, raw_file_datetime):
        raw_file_path = raw_file_datetime.strftime(self.raw_file_template)
        if self.raw_file_type is not None:
            raw_file_path += self.raw_file_type.ext
        return raw_file_path

    def raw_file_path_now(self):
        return self.raw_file_path_at(datetime.datetime.now())

    def raw_file_path_next(self):
        raw_file_datetime_next = self.raw_file_datetime_next()
        if raw_file_datetime_next is None:
            return None
        return self.raw_file_path_at(raw_file_datetime_next)

    @property
    def is_raw_file_path_now(self):
        return self.raw_file_path == self.raw_file_path_now()

    def raw_file_datetime_begin(self, raw_file_datetime=None):
        if self.raw_file_datetime_incrementer is None:
            return None

        if raw_file_datetime is None:
            raw_file_datetime = self.raw_file_datetime
            raw_file_path = self.raw_file_path
        else:
            raw_file_path = self.raw_file_path_at(raw_file_datetime)

        search_min_datetime = \
          raw_file_datetime - self.raw_file_datetime_incrementer
        search_max_datetime = raw_file_datetime

        # Loop invariant: raw_file_path at search_max_datetime always
        # matches target raw_file_path.
        while True:
            search_range = (search_max_datetime - search_min_datetime)
            search_mid_datetime = search_min_datetime + search_range // 2

            # Check whether we have finished, min == max within rounding
            if search_mid_datetime in (search_min_datetime, search_max_datetime):
                return search_max_datetime # See invariant above.

            search_mid_raw_file_path = self.raw_file_path_at(search_mid_datetime)
            if search_mid_raw_file_path == raw_file_path:
                search_max_datetime = search_mid_datetime
            else:
                search_min_datetime = search_mid_datetime

    def raw_file_datetime_begin_next(self, raw_file_datetime=None):
        raw_file_datetime_begin = \
            self.raw_file_datetime_begin(raw_file_datetime)
        return self.raw_file_datetime_next(raw_file_datetime_begin)

    def raw_file_datetime_next(self, raw_file_datetime=None):
        if self.raw_file_datetime_incrementer is None:
            return None
        if raw_file_datetime is None:
            raw_file_datetime = self.raw_file_datetime
        return raw_file_datetime + self.raw_file_datetime_incrementer

    def raw_file_datetimes_until(self, raw_file_datetime_max):
        raw_file_datetimes = []

        if (self.raw_file_datetime is None or
                self.raw_file_datetime_incrementer is None):
            return raw_file_datetimes

        raw_file_datetime = self.raw_file_datetime
        while raw_file_datetime <= raw_file_datetime_max:
            raw_file_datetime += self.raw_file_datetime_incrementer
            raw_file_datetimes.append(raw_file_datetime)
        return raw_file_datetimes

    def process_lines_until_eof(self, reset_partial_line):
        if not self.is_open:
            return
        while True:
            if reset_partial_line:
                position = self.raw_file_handle.tell()
            line = self.raw_file_handle.readline()
            if line and line.endswith(b"\n"):
                self._process_line(line)
            else:
                if (reset_partial_line and
                        position != self.raw_file_handle.tell()):
                    # Leave any partial line until next time.
                    self.raw_file_handle.seek(position)
                break

    def _process_line(self, line):
        raw_file_line = parse_raw_file_line(line)
        if raw_file_line is None:
            # Ignore invalid lines
            return

        try:
            processor = self.metric_processor_map[raw_file_line.reference]
        except KeyError:
            # Raw file contains lines not in configuration, or this reference is
            # to be found in mutliple raw files and a different raw file was
            # selected to provide the data.
            return

        metric = processor(raw_file_line)

        metric_rate = None
        if metric.rate is not None:
            metric_rate = "%.3f" % (metric.rate, )

        logger = logging.getLogger()
        logger.debug(
            "  %s: time: %s value: %s interval: %s rate: %s delta: %s",
            metric.reference,
            datetime.datetime.fromtimestamp(metric.timestamp),
            metric.value,
            metric.interval_seconds,
            metric_rate,
            metric.delta
        )

        try:
            self.metric_queue.put_nowait(metric)
        except queue.Full:
            pass


def read_to_timestamp(raw_file_handle, timestamp):
    for line in raw_file_handle:
        raw_file_line = parse_raw_file_line(line)
        if raw_file_line is not None and raw_file_line.timestamp >= timestamp:
            return line


def seek_to_timestamp(raw_file_handle, timestamp):
    # pylint: disable=too-many-branches,too-many-statements

    search_min_position = None
    search_min_timestamp = None

    # Find first valid line
    raw_file_handle.seek(0)
    while True:
        search_min_position = raw_file_handle.tell()
        line = raw_file_handle.readline()
        if not line:
            return
        raw_file_line = parse_raw_file_line(line)
        if raw_file_line is not None:
            search_min_timestamp = raw_file_line.timestamp
            break

    # Sanity check to prevent broken interpolation below
    if timestamp <= search_min_timestamp:
        raw_file_handle.seek(search_min_position)
        return

    raw_file_handle.seek(0, os.SEEK_END)
    search_max_position = raw_file_handle.tell()
    search_max_timestamp = None

    # Find last valid line
    while True:
        seek_to_line_start(raw_file_handle)
        search_max_position = raw_file_handle.tell()
        line = raw_file_handle.readline()
        if not line:
            return
        raw_file_line = parse_raw_file_line(line)
        if raw_file_line is not None:
            search_max_timestamp = raw_file_line.timestamp
            break
        raw_file_handle.seek(search_max_position-1)

    # Sanity check to prevent broken interpolation below
    if timestamp == search_max_timestamp:
        raw_file_handle.seek(search_max_position)
        return
    elif timestamp > search_max_timestamp:
        raw_file_handle.seek(search_max_position)
        raw_file_handle.readline()
        return

    # Find first line with a timestamp greater than or equal to that specified
    while search_min_position < search_max_position:
        # Linearly interpolate mid-point of search range. Timestamps for lines
        # of raw file should be uniformly distributed so this reduces seeks by
        # an order of magnitude in practice compared to taking actual mid-point:
        # - log(log(n)) vs log(n)
        search_timestamp_range = (search_max_timestamp - search_min_timestamp)
        interpolation_ratio = \
            (timestamp - search_min_timestamp) / search_timestamp_range
        search_position_range = (search_max_position - search_min_position)
        search_mid_position = \
            search_min_position + (search_position_range * interpolation_ratio)
        search_mid_position = int(search_mid_position)

        # Find the line containing the mid-point
        raw_file_handle.seek(search_mid_position)
        if search_mid_position > search_min_position:
            seek_to_line_start(raw_file_handle)
            search_mid_position = raw_file_handle.tell()

        # ... or the next line after it that is valid
        while True:
            position_line_start = raw_file_handle.tell()
            line = raw_file_handle.readline()
            if not line:
                return
            position_after_line = raw_file_handle.tell()
            raw_file_line = parse_raw_file_line(line)
            if raw_file_line is not None:
                break

        # Narrow the remaining search range
        search_mid_timestamp = raw_file_line.timestamp
        if timestamp > search_mid_timestamp:
            search_min_position = position_after_line
            search_min_timestamp = search_mid_timestamp
        else:
            # Defer detection of equality - we want the first matching instance
            # in the file not the first matching instance we come across.
            # Set maximum position to the last character before the newline.
            search_max_position = position_line_start - 1
            search_max_timestamp = search_mid_timestamp

    # Min position is after newline (before first character of next line).
    # Max position is before newline (after last character of line).
    assert search_min_position == search_max_position + 1
    raw_file_handle.seek(search_min_position)


def seek_to_line_start(raw_file_handle):
    block_end_position = raw_file_handle.tell()
    while True:
        block_begin_position = max(0, block_end_position - REVERSE_SEEK_LENGTH)
        read_size = block_end_position - block_begin_position

        raw_file_handle.seek(block_begin_position)
        read_value = raw_file_handle.read(read_size)

        # Don't include the terminating newline of the current line when
        # searching for the newline at the beginning of the line.
        newline_index = read_value.rfind(b"\n", 0, -1)
        if newline_index >= 0:
            line_start_position = block_begin_position + newline_index + 1
            raw_file_handle.seek(line_start_position)
            return

        # Line start is at beginning of file.
        if block_begin_position == 0:
            raw_file_handle.seek(block_begin_position)
            return

        # No newline found in block, seek back further and keep looking.
        block_end_position = block_begin_position


def parse_raw_file_line(line):
    if not line.endswith(b"\n"):
        return None

    line = line.rstrip(b"\r\n")

    try:
        timestamp, reference, value = line.split(b"\t")
    except ValueError:
        logger = logging.getLogger()
        logger.warning("  Invalid raw file line : %s", line)
        return None

    try:
        timestamp = int(timestamp)
    except ValueError:
        logger = logging.getLogger()
        logger.warning("  Invalid timestamp value : %s", timestamp)
        return None

    return RawFileLine(timestamp, reference, value)
