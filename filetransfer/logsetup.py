import logging
import logging.handlers
import sys
from datetime import datetime
from . import config

# Any callable registered here gets called with (timestamp_str, level, name, message)
# for every log record, in addition to normal console/file logging.
# The webapp layer registers a broadcaster here to push log lines to
# whichever UI (host or client) is currently connected via WebSocket.
_broadcast_listeners = []


def add_broadcast_listener(fn):
    _broadcast_listeners.append(fn)


def remove_broadcast_listener(fn):
    if fn in _broadcast_listeners:
        _broadcast_listeners.remove(fn)


class BroadcastHandler(logging.Handler):
    def emit(self, record):
        ts = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        msg = record.getMessage()
        for fn in list(_broadcast_listeners):
            try:
                fn(ts, record.levelname, record.name, msg)
            except Exception:
                pass  # never let a broken UI listener take down logging


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(config.LOG_LEVEL)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    try:
        file_handler = logging.handlers.RotatingFileHandler(
            config.LOG_FILE,
            maxBytes=config.LOG_MAX_BYTES,
            backupCount=config.LOG_BACKUP_COUNT,
        )
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    except OSError:
        pass  # e.g. read-only filesystem in some container setups

    logger.addHandler(BroadcastHandler())

    return logger
