#!/usr/bin/env python

# Python 2 / 3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import datetime
import logging
import logging.config
import os.path
import signal
import sys

import rrdbot.config
import rrdbot.streamer


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

MAX_BUFFERED_METRICS = 10000 # 30s at ~333/s


# For argparse of --rrdbot-conf-dir
def check_directory(value):
    if not os.path.exists(value):
        raise argparse.ArgumentTypeError("%s does not exist" % value)
    if not os.path.isdir(value):
        raise argparse.ArgumentTypeError("%s is not a directory" % value)
    return value


def main():
    # pylint: disable=broad-except

    parser = argparse.ArgumentParser(
        description="Output stream of RRDBot metrics."
    )
    parser.add_argument(
        "--rrdbot-conf-dir", type=check_directory,
        default=rrdbot.config.DEFAULT_CONFIG_DIR,
        help="The directory in which rrdbot configuration files are stored."
          " Defaults to " + rrdbot.config.DEFAULT_CONFIG_DIR
    )
    parser.add_argument(
        "--start", type=int,
        help="Unix epoch time to seek to in metric stream before output."
    )
    parser.add_argument(
        "--follow", action="store_true",
        help="Continue to output metrics as they are emitted."
    )
    parser.add_argument(
        "--debug", action="store_true", help="Output extra debug information."
    )
    args = parser.parse_args()

    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    logger.info("RRDBot Streamer starting")

    start = args.start
    if start is not None:
        start = datetime.datetime.fromtimestamp(start)

    try:
        rrdbot_configs = rrdbot.config.cfg_parse_dir(args.rrdbot_conf_dir)
        for metric in rrdbot.streamer.metric_stream(
                rrdbot_configs, start, args.follow,
                max_buffered_metrics=MAX_BUFFERED_METRICS
        ):
            logger.info("  %s", metric)
    except rrdbot.config.ConfigurationError as ex:
        logger.error("%s", ex)
        return 1
    except Exception as ex:
        logger.exception(ex)
        return 1

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
