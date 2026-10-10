"""
Helpers for logging resource status changes without repeating identical lines.
"""

import logging

logger = logging.getLogger(__name__)

# Tracks the last status logged per key for log_status_change()
_last_logged_status = {}


def log_status_change(key, status, logger=logger, message=None):
    """
    Log a resource status only when it differs from the last logged value.

    Useful inside retry/polling loops where the same status would otherwise be
    logged on every attempt. The status is logged the first time it is seen for
    a given key and again only when it transitions to a new value.

    Args:
        key (str): Unique identifier for the resource being tracked
            (e.g. "StorageCluster/ocs-storagecluster").
        status: Current status value, compared against the last logged value.
        logger (logging.Logger): Logger used to emit the message. Defaults to
            this module's logger.
        message (str, optional): Message to log. Defaults to "{key}: {status}".

    Returns:
        bool: True if the status changed and was logged, False otherwise.

    """
    if _last_logged_status.get(key) != status:
        logger.info(message if message is not None else f"{key}: {status}")
        _last_logged_status[key] = status
        return True
    return False
