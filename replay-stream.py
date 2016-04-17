#!/usr/bin/env python

# Python 2 / 3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import collections
import gzip
import io
import logging
import logging.config
import os.path
import re
import signal
import sys
import time


LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)-7s] %(message)s"
        }
    },
    "handlers": {
        "default": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "stream": 'ext://sys.stdout',
            "formatter": "standard",
        }
    },
    "loggers": {
        "": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": True
        }
    }
}


# Split a raw file line into timestamp and remaining text
def get_timestamp(line):
    try:
        timestamp, remaining = line.split("\t", 1)
    except ValueError:
        return None, line

    try:
        timestamp = int(timestamp)
    except ValueError:
        return None, line

    return timestamp, remaining


# For argparse of --rrdbot-conf-dir
def check_in_file(value):
    if not os.path.exists(value):
        raise argparse.ArgumentTypeError("%s does not exist" % value)
    if not os.path.isfile(value):
        raise argparse.ArgumentTypeError("%s is not a file" % value)

    try:
        if os.path.splitext(value)[1] == '.gz':
            return gzip.open(value, 'rb')
        else:
            return io.open(value, 'rb')
    except IOError as e:
        raise ArgumentTypeError("can't open '%s': %s" % (value, e))


    return value


def main():
    parser = argparse.ArgumentParser(
        description="Replay rdbot metrics from file."
    )
    parser.add_argument(
        "--in-file", type=check_in_file, required=True,
        help="The input raw file to replay from."
    )
    parser.add_argument(
        "--out-file", type=argparse.FileType('wb'), required=True,
        help="The output raw file to replay to."
    )
    parser.add_argument(
        "--debug", action="store_true", help="Output extra debug information."
    )
    args = parser.parse_args()

    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    logger.info("RRDBot Replay starting")

    # Calculate offset between first timestamp in replay file and now
    timestamp_first = None
    timestamp_offset = None
    for line in args.in_file:
        timestamp, remaining = get_timestamp(line)
        if timestamp is not None:
            timestamp_first = timestamp
            timestamp_offset = int(time.time()) - timestamp
            break

    # Seek back to beginning of raw file to replay all lines
    args.in_file.seek(0)

    if timestamp_offset is None:
        for line in args.in_file:
            args.out_file.write(line)
    else:
        logger.debug("Timestamp offset = %d", timestamp_offset)

        # We'll group lines by timestamp and replay them with even spacing
        timestamp_last = timestamp_first
        timestamp_last_adjusted = timestamp_last + timestamp_offset
        timestamp_lines = []

        for line in args.in_file:
            timestamp, remaining = get_timestamp(line)

            if timestamp is None:
                timestamp_lines.append(line)
                continue

            if timestamp is None or timestamp == timestamp_last:
                timestamp_lines.append(
                  b"%s\t%s" % (timestamp_last_adjusted, remaining)
                )
                continue

            # New timestamp so replay all collected lines with even spacing
            line_count = len(timestamp_lines)
            for index, line in enumerate(timestamp_lines):
                spacing = index / line_count
                delay = timestamp_last_adjusted - time.time() + spacing
                if delay > 0:
                    time.sleep(delay)
                logger.debug(line.rstrip())
                args.out_file.write(line)

            # Start a new collection of lines initially containing single line
            timestamp_last = timestamp
            timestamp_last_adjusted = timestamp_last + timestamp_offset
            timestamp_lines = []
            timestamp_lines.append(
                b"%s\t%s" % (timestamp_last_adjusted, remaining)
            )

        # Replay final collection of lines
        line_count = len(timestamp_lines)
        for index, line in enumerate(timestamp_lines):
            spacing = index / line_count
            delay = timestamp_last_adjusted - time.time() + spacing
            if delay > 0:
                time.sleep(delay)
            logger.debug(line.rstrip())
            args.out_file.write(line)

    logger.info("RRDBot Streamer finished")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        # https://bugs.python.org/issue1054041
        # http://www.cons.org/cracauer/sigint.html
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        os.kill(os.getpid(), signal.SIGINT)
