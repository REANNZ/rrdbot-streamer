# Python 2 / 3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging
import os.path
import re


# Constants from rrdbotd and rrdbot-create.
DEFAULT_CONFIG_DIR = "/etc/rrdbot"

CONFIG_GENERAL = b"general"
CONFIG_RAW = b"raw"
CONFIG_POLL = b"poll"
CONFIG_INTERVAL = b"interval"
CONFIG_TIMEOUT = b"timeout"
CONFIG_SOURCE = b"source"
CONFIG_REFERENCE = b"reference"
CONFIG_CREATE = b"create"
CONFIG_CF = b"cf"
CONFIG_ARCHIVE = b"archive"
CONFIG_TYPE = b"type"
CONFIG_MIN = b"min"
CONFIG_MAX = b"max"

VAL_UNKNOWN = b"U"
VAL_ABSOLUTE = b"ABSOLUTE"
VAL_GAUGE = b"GAUGE"
VAL_COUNTER = b"COUNTER"
VAL_DERIVE = b"DERIVE"


class ConfigurationError(Exception):
    pass


class RRDBotConfigItem(object):
    # pylint: disable=too-few-public-methods

    __slots__ = ("name", "source", "reference", "type", "min", "max")

    def __init__(self, name, source):
        self.name = name
        self.source = source
        self.reference = None
        self.type = VAL_ABSOLUTE
        self.min = VAL_UNKNOWN
        self.max = VAL_UNKNOWN

    @property
    def key(self):
        return (self.source, self.reference)


class RRDBotConfigFile(object):
    # pylint: disable=too-few-public-methods

    __slots__ = ("filepath", "raw_file_templates", "items")

    def __init__(self, filepath):
        self.filepath = filepath
        self.raw_file_templates = set()
        self.items = {}


# See rrdbot/common/config-parser.c cfg_parse_dir, parse_dir_internal
def cfg_parse_dir(rrdbot_conf_dir):
    configs = []

    logger = logging.getLogger()
    logger.debug("Searching %s for configuration files", rrdbot_conf_dir)
    for directory, dummy_subdirectories, files in os.walk(rrdbot_conf_dir):
        logger.debug("Searching directory %s", directory)
        for filename in files:
            filepath = os.path.join(directory, filename)
            logger.debug("Reading %s", filepath)
            config = RRDBotConfigFile(filepath)
            try:
                with open(filepath) as config_file:
                    cfg_parse_file(config_file, config)
                configs.append(config)
            except EnvironmentError as ex:
                logger.warning(
                    "Failed to read config file %s : %s", filepath, ex
                )
            except ConfigurationError as ex:
                logger.warning("%s", ex)
            else:
                logger.debug("  Raw file templates:")
                for raw_file_template in sorted(config.raw_file_templates):
                    logger.debug("    %s", raw_file_template)
                logger.debug("  Items:")
                for field in sorted(config.items.keys()):
                    item = config.items[field]
                    if item.reference is None:
                        continue
                    logger.debug(
                        "    %s type=%s min=%s max=%s",
                        item.reference, item.type, item.min, item.max
                    )

    return configs


# See rrdbot/common/config-parser.c cfg_parse_file
def cfg_parse_file(config_file, config):
    header = name = value = None

    for index, line in enumerate(config_file):
        line = line.rstrip()

        # Continuation line (had spaces at start)
        if line and line[:1].isspace():
            if not value:
                raise ConfigurationError(
                    "Invalid continuation in config %s:%d : %s" % \
                    (index+1, config_file.name, line)
                )

            # Continuations are separated by spaces
            # NB: config-parser.c does not handle this properly
            value += " " + line.lstrip()
            continue

        # No continuation hand off value if necessary
        if name and value:
            config_value(header, name, value, config)

        name = value = None

        # Empty lines / comments at start / comments without continuation
        if not line or line.startswith("#"):
            continue

        # A header
        if line.startswith("["):
            index = line.find("]", 1)
            if index == -1 or index == 1:
                raise ConfigurationError(
                    "Invalid config header %s:%d : %s" % \
                    (index+1, config_file.name, line)
                )
            header = line[1:index].strip()
            continue

        # Look for the break between name = value on the same line
        match = re.search("[:=]", line)
        if match is None:
            raise ConfigurationError(
                "Invalid config line %s:%d : %s" % \
                (index+1, config_file.name, line)
            )
        name = line[:match.start()].strip()
        value = line[match.end():].strip()

    if name and value:
        config_value(header, name, value, config)


# See rrdbot/daemon/config.c cfg_value, config_value
#            tools/rrdbot-create.c cfg_value
def config_value(header, name, value, config):
    # pylint: disable=too-many-return-statements,too-many-branches

    logger = logging.getLogger()

    if header == CONFIG_GENERAL:
        if name == CONFIG_RAW:
            config.raw_file_templates.add(value)

    elif header == CONFIG_POLL:
        if name == CONFIG_INTERVAL:
            return

        if name == CONFIG_TIMEOUT:
            return

        # Parse out suffix
        if "." not in name:
            return

        name, suffix = name.split(".", 1)

        # If it starts with "field.reference"
        if suffix == CONFIG_SOURCE:
            # Parse out the field
            parse_item(name, value, config)

        # If it starts with "field.reference"
        if suffix == CONFIG_REFERENCE:
            # Parse out the field
            parse_item_reference(name, value, config)


    elif header == CONFIG_CREATE:
        if name == CONFIG_CF:
            return

        if name == CONFIG_ARCHIVE:
            return

        # Try and see if the field has a suffix
        if "." not in name:
            return # Ignore unknown options

        name, suffix = name.split(".", 1)

        if name not in config.items:
            logger.warning("%s: Field %s not found", config.filepath, name)
            return

        if suffix == CONFIG_TYPE:
            value = value.upper()
            if value in [VAL_ABSOLUTE, VAL_COUNTER, VAL_GAUGE, VAL_DERIVE]:
                config.items[name].type = value
            else:
                logger.warning(
                    "%s: Invalid field type: %s", config.filepath, value
                )
            return

        elif suffix == CONFIG_MIN:
            value = value.upper()
            if value != VAL_UNKNOWN:
                try:
                    value = int(value)
                except ValueError:
                    logger.warning(
                        "%s: Invalid field min: %s", config.filepath, value
                    )
                    return
            config.items[name].min = value

        elif suffix == CONFIG_MAX:
            value = value.upper()
            if value != VAL_UNKNOWN:
                try:
                    value = int(value)
                except ValueError:
                    logger.warning(
                        "%s: Invalid field max: %s", config.filepath, value
                    )
                    return
            config.items[name].max = value

        # Ignore unknown options


# See rrdbot/daemon/config.c parse_item
def parse_item(field, uri, config):
    # Don't parse URI, we can't do it completely from here anyway
    if field not in config.items:
        config.items[field] = RRDBotConfigItem(field, uri)


# See rrdbot/daemon/config.c parse_item_reference
def parse_item_reference(field, reference, config):
    if field not in config.items:
        logger = logging.getLogger()
        logger.warning("%s: Field %s not found", config.filepath, field)
    else:
        config.items[field].reference = reference
