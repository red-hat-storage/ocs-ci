"""
Unit tests for ocs_ci.utility.status_logger.
"""

import logging

from ocs_ci.utility import status_logger


def test_log_status_change_first_time_logs(caplog):
    """
    The first time a key is seen, its status should be logged and True
    returned.
    """
    status_logger._last_logged_status.clear()
    caplog.set_level(logging.INFO)
    result = status_logger.log_status_change("StorageCluster/test", "Ready")
    assert result is True
    assert "StorageCluster/test: Ready" in caplog.text


def test_log_status_change_same_status_not_logged(caplog):
    """
    Logging the same status for the same key a second time should be a
    no-op and return False.
    """
    status_logger._last_logged_status.clear()
    caplog.set_level(logging.INFO)
    status_logger.log_status_change("StorageCluster/test", "Ready")
    caplog.clear()
    result = status_logger.log_status_change("StorageCluster/test", "Ready")
    assert result is False
    assert caplog.text == ""


def test_log_status_change_different_status_logged(caplog):
    """
    Logging a new status value for an already-tracked key should log again
    and return True.
    """
    status_logger._last_logged_status.clear()
    caplog.set_level(logging.INFO)
    status_logger.log_status_change("StorageCluster/test", "Pending")
    caplog.clear()
    result = status_logger.log_status_change("StorageCluster/test", "Ready")
    assert result is True
    assert "StorageCluster/test: Ready" in caplog.text


def test_log_status_change_different_keys_independent(caplog):
    """
    Different keys should be tracked independently of one another.
    """
    status_logger._last_logged_status.clear()
    caplog.set_level(logging.INFO)
    status_logger.log_status_change("StorageCluster/a", "Ready")
    caplog.clear()
    result = status_logger.log_status_change("StorageCluster/b", "Ready")
    assert result is True
    assert "StorageCluster/b: Ready" in caplog.text


def test_log_status_change_custom_message(caplog):
    """
    A custom message should be used instead of the default "key: status"
    format when provided.
    """
    status_logger._last_logged_status.clear()
    caplog.set_level(logging.INFO)
    status_logger.log_status_change(
        "StorageCluster/test", "Ready", message="custom message here"
    )
    assert "custom message here" in caplog.text
    assert "StorageCluster/test: Ready" not in caplog.text


def test_log_status_change_custom_logger(caplog):
    """
    When a custom logger is passed, it should be used to emit the message
    instead of the module-level default logger.
    """
    status_logger._last_logged_status.clear()
    custom_logger = logging.getLogger("custom.test.logger")
    caplog.set_level(logging.INFO)
    status_logger.log_status_change(
        "StorageCluster/test", "Ready", logger=custom_logger
    )
    assert any(rec.name == "custom.test.logger" for rec in caplog.records)


def test_log_status_change_non_string_status(caplog):
    """
    Status values that aren't strings (e.g. dicts) should still be tracked
    and compared correctly.
    """
    status_logger._last_logged_status.clear()
    caplog.set_level(logging.INFO)
    status_a = {"phase": "Ready"}
    status_b = {"phase": "Ready"}
    status_logger.log_status_change("StorageCluster/test", status_a)
    caplog.clear()
    result = status_logger.log_status_change("StorageCluster/test", status_b)
    assert result is False
    assert caplog.text == ""
