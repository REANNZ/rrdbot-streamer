# Python 2 / 3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import os.path

import pyinotify


# Mask used for add_watch on directories containing rrdbot raw files.
# pylint: disable=no-member
DIRECTORY_INOTIFY_MASK = \
  pyinotify.IN_CREATE | pyinotify.IN_DELETE | pyinotify.IN_DELETE_SELF


class FileSystemEventHandler(pyinotify.ProcessEvent):

    # pylint: disable=arguments-differ
    def my_init(self, watch_manager, raw_file_template_reader_map,
                directory_watch_map):

        self.watch_manager = watch_manager
        self.raw_file_template_reader_map = raw_file_template_reader_map
        self.raw_file_path_reader_map = {}
        self.raw_file_path_next_reader_map = {}
        for raw_file_reader in self.raw_file_template_reader_map.values():
            self._add_to_path_reader_maps(raw_file_reader)
        self.directory_watch_map = directory_watch_map
        self.paths_pending_unwatch = set()

    # pylint: disable=invalid-name
    def process_IN_CREATE(self, event):
        logger = logging.getLogger()
        if event.dir:
            logger.debug("Directory %s created", event.pathname)
            self.process_IN_CREATE_dir(event)
        else:
            logger.debug("File %s created", event.pathname)
            self.process_IN_CREATE_file(event)

    # pylint: disable=invalid-name
    def process_IN_MODIFY(self, event):
        logger = logging.getLogger()
        logger.debug("Raw file %s modified", event.pathname)

        if event.pathname in self.raw_file_path_reader_map:
            raw_file_reader = self.raw_file_path_reader_map[event.pathname]
            raw_file_reader.process_lines_until_eof(True)
        elif event.pathname in self.raw_file_path_next_reader_map:
            raw_file_reader = self.raw_file_path_next_reader_map[event.pathname]
            raw_file_path_now = raw_file_reader.raw_file_path_now()
            if raw_file_path_now == event.pathname:
                self._remove_from_path_reader_maps(raw_file_reader)
                raw_file_reader.rollover(watch_file=True)
                self._add_to_path_reader_maps(raw_file_reader)
                log_watches(self.watch_manager)
        else:
            raise RuntimeError(
                "MODIFY event received for unknown file %s", event.pathname
            )

    # pylint: disable=invalid-name
    def process_IN_DELETE(self, event):
        if event.dir:
            self.process_IN_DELETE_dir(event)
        else:
            self.process_IN_DELETE_file(event)

    # pylint: disable=invalid-name
    def process_IN_DELETE_SELF(self, event):
        if event.dir:
            self.process_IN_DELETE_dir(event)

    # pylint: disable=invalid-name
    def process_IN_CREATE_dir(self, event):
        watches_changed = False

        if event.pathname not in self.directory_watch_map:
            watch_descriptor = self.watch_manager.get_wd(event.pathname)
            if watch_descriptor is not None:
                logger = logging.getLogger()
                logger.debug(
                    "  Found auto-added watch for %s [%s]",
                    event.pathname, watch_descriptor
                )
                self.directory_watch_map[event.pathname] = watch_descriptor
                watches_changed = True

        watches_changed |= self._update_directory_watches()
        if watches_changed:
            log_watches(self.watch_manager)

    # pylint: disable=invalid-name
    def process_IN_CREATE_file(self, event):
        now = datetime.datetime.now()

        watches_changed = False

        for raw_file_reader in self.raw_file_template_reader_map.values():
            if raw_file_reader.raw_file_path == event.pathname:
                if not raw_file_reader.is_open:
                    if raw_file_reader.open(raw_file_reader.raw_file_datetime):
                        self.raw_file_path_reader_map[event.pathname] = \
                            raw_file_reader
                        raw_file_reader.process_lines_until_eof(True)
                break

            next_datetime = None
            for next_datetime in raw_file_reader.raw_file_datetimes_until(now):
                next_raw_file_path = \
                    raw_file_reader.raw_file_path_at(next_datetime)
                if next_raw_file_path == event.pathname:
                    break
            else:
                continue

            while raw_file_reader.raw_file_datetime < next_datetime:
                self._remove_from_path_reader_maps(raw_file_reader)
                raw_file_reader.rollover()
                self._add_to_path_reader_maps(raw_file_reader)

            watches_changed |= raw_file_reader.is_watched
            break

        watches_changed |= self._update_directory_watches()
        if watches_changed:
            log_watches(self.watch_manager)

    # pylint: disable=invalid-name
    def process_IN_DELETE_dir(self, event):
        if event.pathname not in self.directory_watch_map:
            return
        watch_descriptor = self.directory_watch_map[event.pathname]

        logger = logging.getLogger()
        logger.debug("Directory %s deleted", event.pathname)

        # This unwatch usually does nothing as pyinotify will say the watch is
        # not a valid fd for rm_watch, even though the current watch list still
        # includes it, and will until after we return from this call.
        # Possibly related to fix of https://github.com/seb-m/pyinotify/issues/2
        unwatch(self.watch_manager, watch_descriptor, event.pathname)
        del self.directory_watch_map[event.pathname]

        self._update_directory_watches()

        # Don't log watch for the deleted directory, pyinotify will remove the
        # watch after a small delay, so logging it here is misleading.
        log_watches(self.watch_manager, wd_excludes=[watch_descriptor])

    # pylint: disable=invalid-name
    def process_IN_DELETE_file(self, event):
        if event.pathname not in self.raw_file_path_reader_map:
            return
        raw_file_reader = self.raw_file_path_reader_map[event.pathname]

        logger = logging.getLogger()
        logger.debug("Raw file %s deleted", event.pathname)

        raw_file_reader.process_lines_until_eof(False)
        raw_file_reader.close()

        log_watches(self.watch_manager)

        self._remove_from_path_reader_maps(raw_file_reader)

    def cb_loop(self, notifier):
        if not self.paths_pending_unwatch:
            return

        logger = logging.getLogger()
        logger.debug("Removing unrequired watches")
        for path in self.paths_pending_unwatch:
            watch_descriptor = self.directory_watch_map.pop(path)
            unwatch(self.watch_manager, watch_descriptor, path)
        self.paths_pending_unwatch.clear()
        log_watches(self.watch_manager)

    def _add_to_path_reader_maps(self, raw_file_reader):
        raw_file_path = raw_file_reader.raw_file_path
        self.raw_file_path_reader_map[raw_file_path] = raw_file_reader

        raw_file_path_next = raw_file_reader.raw_file_path_next()
        if raw_file_path_next is not None:
            self.raw_file_path_next_reader_map[raw_file_path_next] = \
              raw_file_reader

    def _remove_from_path_reader_maps(self, raw_file_reader):
        raw_file_path = raw_file_reader.raw_file_path
        del self.raw_file_path_reader_map[raw_file_path]

        raw_file_path_next = raw_file_reader.raw_file_path_next()
        self.raw_file_path_next_reader_map.pop(raw_file_path_next, None)

    def _update_directory_watches(self):
        watches_changed = False

        required_watch_set = set()
        for raw_file_reader in self.raw_file_template_reader_map.values():
            watches_changed |= add_reader_directory_watches(
                raw_file_reader, self.directory_watch_map,
                required_watch_set=required_watch_set
            )

        watch_set = set(directory for directory in self.directory_watch_map)
        unrequired_watches = watch_set - required_watch_set
        new_unrequired_watches = unrequired_watches - self.paths_pending_unwatch
        if new_unrequired_watches:
            logger = logging.getLogger()
            for directory in sorted(new_unrequired_watches):
                logger.debug('  Watch on %s not required', directory)

        # Don't unwatch paths immediately, there may be queued psuedo-events for
        # the directory (part of auto-add) and pyinotifiy will complain when it
        # tries to process them. https://github.com/seb-m/pyinotify/issues/2
        self.paths_pending_unwatch |= new_unrequired_watches

        return watches_changed


def log_watches(watch_manager, wd_excludes=None):
    if not watch_manager.watches:
        return
    logger = logging.getLogger()
    logger.debug("Watches")
    watch_path_map = {
        watch.path: watch.wd for watch in watch_manager.watches.values()
    }
    for watch_path in sorted(watch_path_map):
        watch_descriptor = watch_path_map[watch_path]
        if wd_excludes and watch_descriptor in wd_excludes:
            continue
        logger.debug("  %s [%s]", watch_path, watch_descriptor)


def unwatch(watch_manager, watch_descriptor, path=None):
    if path is None:
        path = watch_manager.get_path(watch_descriptor)
    try:
        # Don't let pyinotify log an error itself, let it raise an exception so
        # we can ignore it without logging
        watch_manager.rm_watch(watch_descriptor, quiet=False)
    except pyinotify.WatchManagerError:
        pass
    else:
        logger = logging.getLogger()
        logger.debug("  Unwatched %s [%s]", path, watch_descriptor)


def add_reader_directory_watches(raw_file_reader, watch_map,
                                 required_watch_set=None):
    watches_changed = update_directory_watches(
        raw_file_reader.raw_file_template,
        raw_file_reader.raw_file_path,
        raw_file_reader.watch_manager,
        watch_map,
        required_watch_set=required_watch_set
    )
    raw_file_path_next = raw_file_reader.raw_file_path_next()
    if raw_file_path_next is not None:
        watches_changed |= update_directory_watches(
            raw_file_reader.raw_file_template,
            raw_file_path_next,
            raw_file_reader.watch_manager,
            watch_map,
            required_watch_set=required_watch_set
        )
    return watches_changed


def update_directory_watches(raw_file_template, raw_file_path, watch_manager,
                             watch_map, required_watch_set=None):
    # pylint: disable=too-many-locals

    watches_changed = False

    template_path = raw_file_template
    path = raw_file_path
    path_exists = os.path.exists(path)
    while True:
        template_parent, template_basename = os.path.split(template_path)
        if template_parent == template_path:
            break
        parent, basename = os.path.split(path)

        parent_exists = os.path.exists(parent)

        requires_watch = parent_exists and (
            (
                # Raw file creation and deletion
                path == raw_file_path
            ) or (
                # Dynamically named directory creation
                template_basename != basename
            ) or (
                # First missing statically named directory creation
                not path_exists
            )
        )

        if requires_watch and required_watch_set is not None:
            required_watch_set.add(parent)

        add_watch = requires_watch and parent not in watch_map
        if add_watch:
            logger = logging.getLogger()
            try:
                wdd = watch_manager.add_watch(
                    parent, DIRECTORY_INOTIFY_MASK, auto_add=True, quiet=False
                )
            except pyinotify.WatchManagerError as ex:
                logger.warning("  %s", ex.message)
            else:
                if parent in wdd:
                    logger.debug(
                        "  Watching %s for child creation and deletion [%s]",
                        parent, wdd[parent]
                    )
                watch_map.update(wdd)
                watches_changed = len(wdd) > 0

        template_path = template_parent
        path = parent
        path_exists = parent_exists

    return watches_changed
