"""
Helpers for capturing a detailed view of a resource for troubleshooting,
typically when a status check has exhausted its retries.
"""

import logging
from functools import wraps

from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


def log_resource_description(kind, resource_name, namespace=None, logger=logger):
    """
    Log the ``oc describe`` output for a resource.

    Failures to retrieve the description are caught and logged so this can be
    used safely in error-handling paths without masking the original error.

    Args:
        kind (str): Resource kind (e.g. "StorageCluster").
        resource_name (str): Name of the resource.
        namespace (str, optional): Namespace of the resource. Defaults to None
            for cluster-scoped resources.
        logger (logging.Logger): Logger used to emit the description. Defaults
            to this module's logger.

    """
    try:
        ocp = OCP(kind=kind, resource_name=resource_name, namespace=namespace)
        description = ocp.describe(resource_name=resource_name)
        logger.warning(
            f"Detailed description of {kind}/{resource_name}:\n{description}"
        )
    except Exception as ex:
        logger.warning(
            f"Failed to retrieve description for {kind}/{resource_name}: {ex}"
        )


def describe_on_failure(kind, resource_name, namespace=None, logger=logger):
    """
    Decorator that logs ``oc describe`` output for a resource when the wrapped
    call raises an exception.

    Intended to wrap a ``retry``-decorated status check (applied *outside* the
    retry decorator) so the detailed view is captured only once, after all
    retry attempts have been exhausted. The original exception is re-raised.

    Args:
        kind (str): Resource kind (e.g. "StorageCluster").
        resource_name (str): Name of the resource.
        namespace (str, optional): Namespace of the resource. Defaults to None
            for cluster-scoped resources.
        logger (logging.Logger): Logger used to emit the description. Defaults
            to this module's logger.

    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                log_resource_description(kind, resource_name, namespace, logger)
                raise

        return wrapper

    return decorator
