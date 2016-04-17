"""Main metric stream interface for RRDBot Streamer."""

# Python 2 / 3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import collections
import datetime
import logging
import signal
import threading

# Python 2 / 3 compatibility
try:
    import queue
except ImportError:
    # pylint: disable=wrong-import-order
    import Queue as queue

import pyinotify

from . import config
from . import metrics
from . import raw_file
from . import strftime_delta
from . import watch


# Mapping of rrdbot metric types to the class that can process them.
PROCESSOR_CLASS_MAP = {
    config.VAL_ABSOLUTE: metrics.AbsoluteMetricProcessor,
    config.VAL_GAUGE: metrics.GaugeMetricProcessor,
    config.VAL_COUNTER: metrics.CounterMetricProcessor,
    config.VAL_DERIVE: metrics.DeriveMetricProcessor
}

# Sentinel value used for clean shutdown.
STREAM_END = object()

# Global values used for signal handler and clean shutdown.
# pylint: disable=invalid-name
g_sigint_handler_installed = False
g_sigint_handler_original = False
g_sigint_received = threading.Event()
g_metric_queues = []
g_stream_threads = []


def metric_stream(rrdbot_configs, start=None, follow=True,
                  max_buffered_metrics=0):
    """Yield a stream of metrics from RRDBot raw files.

    :param rrdbot_configs: List of RRDBot configurations, generally obtained via
      a call to rrdbot.config.cfg_parse_dir.
    :type rrdbot_configs: list of rrdbot.config.RRDBotConfigFile.
    :param start: Date and time to seek to in metric stream before yielding
      metrics. If this value is None or in the future it is ignored
      (default: None).
    :type start: datetime.datetime.
    :param follow: Continue to output metrics as they are added to RRDBot raw
      files (default: True).
    :type follow: bool.
    :param max_buffered_metrics: Maximum number of metrics that will be buffered
      by generator before metrics are dropped (default: no limit).
    :type max_buffered_metrics: int.
    :returns: Stream of RRDBot metrics.
    :rtype: generator of rrdbot.metrics.Metric

    """
    metric_queue = queue.Queue(maxsize=max_buffered_metrics)
    g_metric_queues.append(metric_queue)
    stream_thread = StreamThread(rrdbot_configs, start, follow, metric_queue)
    stream_thread.start()
    if not stream_thread.wait_until_started(timeout=15):
        stream_thread.request_finish()
        stream_thread.join()
        return
    g_stream_threads.append(stream_thread)
    try:
        while not g_sigint_received.is_set():
            metric = metric_queue.get()
            if metric is STREAM_END:
                break
            yield metric
    finally:
        g_metric_queues.remove(metric_queue)
        stream_thread.request_finish()
        stream_thread.join()


def sigint_handler(signum, frame):
    """Signal handler for SIGINT to initiate clean shutdown."""
    # pylint: disable=unused-argument
    g_sigint_received.set()
    for metric_queue in g_metric_queues:
        metric_queue.put(STREAM_END)
    for stream_thread in g_stream_threads:
        stream_thread.request_finish()
    if g_sigint_handler_original is not None:
        signal.signal(signal.SIGINT, g_sigint_handler_original)
        g_sigint_handler_original(signal, frame)


if not g_sigint_handler_installed:
    g_sigint_handler_original = signal.signal(signal.SIGINT, sigint_handler)
    g_sigint_handler_installed = True


class StreamThread(threading.Thread):
    """Thread to process raw files asynchronously, adding metrics to provided
    queue to be removed by caller.
    """

    def __init__(self, rrdbot_configs, start, follow, metric_queue):
        """Constructor.

        :param rrdbot_configs: List of RRDBot configurations, generally obtained
          via a call to rrdbot.config.cfg_parse_dir.
        :type rrdbot_configs: list of rrdbot.config.RRDBotConfigFile.
        :param start: Date and time to seek to in metric stream before yielding
          metrics. If this value is None or in the future it is ignored.
        :type start: datetime.datetime.
        :param follow: Continue to output metrics as they are added to RRDBot
          raw files.
        :type follow: bool.
        :param metric_queue: Queue to receive metrics as they are read from raw
          files.
        :type metric_queue: queue.Queue.

        """
        self._rrdbot_configs = rrdbot_configs
        self._start = start
        self._follow = follow
        self._metric_queue = metric_queue
        self._started = threading.Event()
        self._finish_requested = threading.Event()
        self._event_handler = None
        super(StreamThread, self).__init__()

    def wait_until_started(self, timeout=None):
        """Wait until thread has started executing.

        :param timeout: Number of seconds to wait for thread to start. If
          timeout is None will wait indefinitely (default: None).
        :type timeout: int or float.
        :returns: Whether thread started successfully within timeout.
        :rtype: bool

        """
        return self._started.wait(timeout)

    def request_finish(self):
        """Request thread stops at next opportunity."""
        self._finish_requested.set()

    def run(self):
        """Main thread execution."""
        # pylint: disable=too-many-locals,too-many-branches

        self._started.set()

        logger = logging.getLogger()

        watch_manager = None
        if self._follow:
            watch_manager = pyinotify.WatchManager()

        raw_file_template_reader_map = create_readers(
            self._rrdbot_configs, watch_manager, self._start, self._metric_queue
        )

        directory_watch_map = {}

        files_watched = False
        while not self._is_finish_requested:
            raw_file_rollover_count = 0

            for raw_file_template in sorted(raw_file_template_reader_map):
                raw_file_reader = \
                  raw_file_template_reader_map[raw_file_template]

                logger.info("Processing %s", raw_file_template)

                if not raw_file_reader.is_open:
                    opened = raw_file_reader.open(
                        raw_file_reader.raw_file_datetime, watch_file=False
                    )
                    if not opened:
                        continue
                    if (self._start is None or
                            self._start > raw_file_reader.raw_file_datetime):
                        logger.info(
                            "  Starting %s from now",
                            raw_file_reader.raw_file_path
                        )
                        raw_file_reader.seek_to_end()
                    elif raw_file_reader.raw_file_datetime == self._start:
                        logger.info(
                            "  Starting %s from %s",
                            raw_file_reader.raw_file_path, self._start
                        )
                        raw_file_reader.seek_to_datetime(self._start)

                if raw_file_reader.raw_file_datetime_incrementer is not None:
                    raw_file_datetime_begin_now = \
                      raw_file_reader.raw_file_datetime_begin(
                          datetime.datetime.now()
                      )
                    while (not self._is_finish_requested and
                           raw_file_reader.raw_file_datetime < raw_file_datetime_begin_now):
                        raw_file_reader.rollover(watch_file=False)
                        raw_file_rollover_count += 1

                raw_file_reader.process_lines_until_eof(True)

            if not self._follow:
                break

            if files_watched:
                break

            if raw_file_rollover_count == 0:
                logger.info("Enabling watches")
                for raw_file_template in sorted(raw_file_template_reader_map):
                    raw_file_reader = \
                        raw_file_template_reader_map[raw_file_template]
                    watch.add_reader_directory_watches(
                        raw_file_reader, directory_watch_map
                    )
                    raw_file_reader.watch()
                files_watched = True
                # Final pass for events between last reads and watch creation.

        if self._follow:
            try:
                self._event_handler = watch.FileSystemEventHandler(
                    watch_manager=watch_manager,
                    raw_file_template_reader_map=raw_file_template_reader_map,
                    directory_watch_map=directory_watch_map
                )
                watch.log_watches(watch_manager)
                notifier = pyinotify.Notifier(
                    watch_manager, default_proc_fun=self._event_handler
                )
                notifier.loop(callback=self._cb_loop)
            finally:
                for raw_file_reader in raw_file_template_reader_map.values():
                    raw_file_reader.close()

        self._metric_queue.put(STREAM_END)

    @property
    def _is_finish_requested(self):
        """Whether this thread has been requested to stop.

        :rtype: bool

        """
        return self._finish_requested.is_set()

    def _cb_loop(self, notifier):
        """Loop callback for pyinotify.

        :param notifier: Notifier object callback is for.
        :type notifier: pyinotify.Notifier.
        :returns: Whether notifier should terminate processing loop.
        :rtype: bool

        """
        if self._is_finish_requested:
            return True
        return self._event_handler.cb_loop(notifier)


def create_readers(rrdbot_configs, watch_manager, start, metric_queue):
    """Create raw file readers necessary to read RRDBot raw files.

    :param rrdbot_configs: List of RRDBot configurations, generally obtained via
      a call to rrdbot.config.cfg_parse_dir.
    :type rrdbot_configs: list of rrdbot.config.RRDBotConfigFile.
    :param watch_manager: File system watch manager used to manage pyinotify
      watches on raw files. May be None if raw files will not be followed.
    :type watch_manager: pyinotify.WatchManager.
    :param start: Date and time for initial raw files to open based on strftime
      filename templates.
    :type start: datetime.datetime.
    :param metric_queue: Queue to be provided to raw file readers to receive
      metrics as they are read from raw file.
    :type metric_queue: queue.Queue.
    :returns: Minimal set of raw file readers required to read all metrics.
    :rtype: list of rrdbot.raw_fle.RawFileReader

    """
    # pylint: disable=too-many-locals,invalid-name

    logger = logging.getLogger()

    # Grab details of raw file templates and the items they contain.
    raw_file_templates = set()
    raw_file_template_items_dict = collections.defaultdict(set)
    item_raw_file_templates_dict = collections.defaultdict(set)
    for rrdbot_config in rrdbot_configs:
        raw_file_templates.update(rrdbot_config.raw_file_templates)
        for item in rrdbot_config.items.values():
            for raw_file_template in rrdbot_config.raw_file_templates:
                raw_file_template_items_dict[raw_file_template].add(item.key)
            item_raw_file_templates_dict[item.key].update(
                rrdbot_config.raw_file_templates
            )

    logger.debug("Raw files templates:")
    for raw_file_template in sorted(raw_file_templates):
        logger.debug("  %s", raw_file_template)

    # Determine how often each raw file template rolls over
    logger.debug("Raw file template durations:")
    raw_file_datetime_incrementer_map = {}
    for raw_file_template in sorted(raw_file_templates):
        raw_file_datetime_incrementer = \
            get_raw_file_datetime_incrementer(raw_file_template)
        raw_file_datetime_incrementer_map[raw_file_template] = \
            raw_file_datetime_incrementer
        logger.debug(
            "  %s: [%s]", raw_file_template, raw_file_datetime_incrementer
        )

    # Log which items are destined for which raw file template
    logger.debug("Item Files:")
    for item_key in sorted(item_raw_file_templates_dict):
        item_raw_file_templates = item_raw_file_templates_dict[item_key]
        logger.debug("  %s %s", item_key, sorted(item_raw_file_templates))

    item_set = set(item_raw_file_templates_dict)
    selected_raw_file_templates = select_minimal_raw_file_templates(
        raw_file_templates, raw_file_template_items_dict, item_set
    )

    # Select raw file template to act as source for each item
    processor_map = collections.defaultdict(dict)
    for rrdbot_config in rrdbot_configs:
        for item in rrdbot_config.items.values():
            item_raw_file_templates = item_raw_file_templates_dict[item.key]
            item_raw_file_templates = \
                item_raw_file_templates & selected_raw_file_templates
            item_raw_file_template = item_raw_file_templates.pop()
            processor_class = PROCESSOR_CLASS_MAP[item.type]
            processor = processor_class(item)
            processor_map[item_raw_file_template][item.reference] = processor

    # Created readers for each selected raw file template
    raw_file_template_reader_map = {}
    for raw_file_template in sorted(selected_raw_file_templates):
        raw_file_processor_map = processor_map[raw_file_template]
        raw_file_datetime_incrementer = \
            raw_file_datetime_incrementer_map[raw_file_template]
        raw_file_reader = raw_file.RawFileReader(
            watch_manager, raw_file_processor_map, metric_queue,
            raw_file_template, raw_file_datetime_incrementer, start
        )
        raw_file_template_reader_map[raw_file_template] = raw_file_reader
    return raw_file_template_reader_map


# pylint: disable=invalid-name
def get_raw_file_datetime_incrementer(raw_file_template):
    """Create a datetime incrementer instance suitable for the given raw file
    strftime filename template. This incrementer can be used to increment
    datetime instances enough to cause a rollover in raw file name.

    :param raw_file_template: Filename template for raw file optionally
      containing strftime format strings.
    :type raw_file_template: str
    :returns: Datetime incrementer instance.
    :rtype: rrdbot.strftime_delta.DatetimeIncrementer
    :raises: rrdbot.config.ConfigurationError (if filename template will not
      produce a non-interleaving sequence of filenames)

    """
    try:
        return strftime_delta.get_datetime_incrementer(
            raw_file_template
        )
    except strftime_delta.InterleaveError:
        raise config.ConfigurationError(
            "Invalid raw file template %s : sequential records may be "
            "interleaved between raw files" % (raw_file_template, )
        )


# pylint: disable=invalid-name
def select_minimal_raw_file_templates(raw_file_templates,
                                      raw_file_template_items_dict, item_set):
    """Select a minimal set of raw files necessary to read all metrics.

    :param raw_file_templates: List of filename templates for raw files
      optionally containing strftime format strings.
    :type raw_file_template: list of str
    :param raw_file_template_items_dict: Map of raw file templates to the
      metrics they are configured to contain.
    :type raw_file_template_items_dict: dict of str to tuple.
    :param item_set: Complete set of metrcis configured to be contained in full
       set of raw file templates.
    :type item_set: set of tuple.
    :returns: Minimal set of raw files templates necessary to read all metrics.
    :rtype: set of tuple

    """
    logger = logging.getLogger()

    selected_raw_file_templates = set()
    covered_items = set()

    # Apply greedy algorithm for set cover: select the raw file that provides
    # the largest number of uncovered items, until all items are covered / all
    # raw files are selected.
    unselected_raw_file_templates = \
        raw_file_templates - selected_raw_file_templates
    uncovered_items = item_set - covered_items
    while (covered_items < item_set and
           selected_raw_file_templates < raw_file_templates):
        logger.debug("Selecting raw file %d", len(selected_raw_file_templates))

        # Find raw file providing the largest number of uncovered items
        max_uncovered_item_count = -1
        max_raw_file_template = None
        for raw_file_template in unselected_raw_file_templates:
            raw_file_template_uncovered_items = \
                raw_file_template_items_dict[raw_file_template] - covered_items
            uncovered_item_count = len(raw_file_template_uncovered_items)
            logger.debug(
                "  Raw file template %s provides %d new items",
                raw_file_template, uncovered_item_count
            )
            if uncovered_item_count > max_uncovered_item_count:
                max_uncovered_item_count = uncovered_item_count
                max_raw_file_template = raw_file_template

        # Select raw file providing the largest number of uncovered items
        logger.debug("  Selected %s", max_raw_file_template)
        selected_raw_file_templates.add(max_raw_file_template)
        unselected_raw_file_templates.remove(max_raw_file_template)
        raw_file_template_items = \
            raw_file_template_items_dict[max_raw_file_template]
        covered_items |= raw_file_template_items
        uncovered_items -= raw_file_template_items

    logger.debug("Selected raw file templates:")
    for raw_file_template in sorted(selected_raw_file_templates):
        logger.debug("  %s", raw_file_template)

    return selected_raw_file_templates
