"""
Unit tests for ocs_ci.utility.resource_debug.
"""

import logging
from unittest.mock import Mock, patch

import pytest

from ocs_ci.utility import resource_debug


def test_log_resource_description_success(caplog):
    """
    When describe() succeeds, its output should be logged at warning level.
    """
    mock_ocp_instance = Mock()
    mock_ocp_instance.describe.return_value = "detailed description output"
    caplog.set_level(logging.WARNING)

    with patch.object(
        resource_debug, "OCP", return_value=mock_ocp_instance
    ) as mock_ocp_cls:
        resource_debug.log_resource_description(
            "StorageCluster", "ocs-storagecluster", namespace="openshift-storage"
        )

    mock_ocp_cls.assert_called_once_with(
        kind="StorageCluster",
        resource_name="ocs-storagecluster",
        namespace="openshift-storage",
    )
    mock_ocp_instance.describe.assert_called_once_with(
        resource_name="ocs-storagecluster"
    )
    assert "detailed description output" in caplog.text
    assert any(rec.levelno == logging.WARNING for rec in caplog.records)


def test_log_resource_description_failure_is_caught(caplog):
    """
    If retrieving the description itself raises, that exception should be
    caught and logged rather than propagated.
    """
    caplog.set_level(logging.WARNING)

    with patch.object(
        resource_debug, "OCP", side_effect=Exception("cluster unreachable")
    ):
        resource_debug.log_resource_description("StorageCluster", "ocs-storagecluster")

    assert "Failed to retrieve description" in caplog.text
    assert "cluster unreachable" in caplog.text


def test_log_resource_description_custom_logger():
    """
    A custom logger should be used to emit the description instead of the
    module default.
    """
    mock_ocp_instance = Mock()
    mock_ocp_instance.describe.return_value = "output"
    custom_logger = Mock()

    with patch.object(resource_debug, "OCP", return_value=mock_ocp_instance):
        resource_debug.log_resource_description(
            "StorageCluster", "ocs-storagecluster", logger=custom_logger
        )

    custom_logger.warning.assert_called_once()


def test_describe_on_failure_passes_through_return_value():
    """
    When the wrapped function succeeds, its return value should be passed
    through untouched and describe should never be invoked.
    """
    with patch.object(resource_debug, "log_resource_description") as mock_describe:

        @resource_debug.describe_on_failure("StorageCluster", "ocs-storagecluster")
        def func():
            return "ok"

        assert func() == "ok"
        mock_describe.assert_not_called()


def test_describe_on_failure_logs_description_and_reraises():
    """
    When the wrapped function raises, the decorator should log the resource
    description and re-raise the original exception.
    """
    with patch.object(resource_debug, "log_resource_description") as mock_describe:

        @resource_debug.describe_on_failure(
            "StorageCluster", "ocs-storagecluster", namespace="openshift-storage"
        )
        def func():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            func()

        mock_describe.assert_called_once_with(
            "StorageCluster",
            "ocs-storagecluster",
            "openshift-storage",
            resource_debug.logger,
        )


def test_describe_on_failure_custom_logger_passed_through():
    """
    A custom logger supplied to the decorator should be forwarded to
    log_resource_description.
    """
    custom_logger = Mock()
    with patch.object(resource_debug, "log_resource_description") as mock_describe:

        @resource_debug.describe_on_failure(
            "StorageCluster", "ocs-storagecluster", logger=custom_logger
        )
        def func():
            raise RuntimeError("fail")

        with pytest.raises(RuntimeError):
            func()

        mock_describe.assert_called_once_with(
            "StorageCluster", "ocs-storagecluster", None, custom_logger
        )


def test_describe_on_failure_preserves_function_metadata():
    """
    The decorator should preserve the wrapped function's name via
    functools.wraps.
    """

    @resource_debug.describe_on_failure("StorageCluster", "ocs-storagecluster")
    def my_func():
        """My docstring."""
        return 1

    assert my_func.__name__ == "my_func"
    assert my_func.__doc__ == "My docstring."


def test_describe_on_failure_passes_args_and_kwargs():
    """
    Positional and keyword arguments passed to the wrapped function should
    reach it unchanged.
    """
    with patch.object(resource_debug, "log_resource_description"):

        @resource_debug.describe_on_failure("StorageCluster", "ocs-storagecluster")
        def func(a, b, c=None):
            return a, b, c

        assert func(1, 2, c=3) == (1, 2, 3)
