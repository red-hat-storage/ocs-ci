"""
Helper functions for ODF upgrade pre-check conditions.
"""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.csv import get_csv_name_start_with_prefix
from ocs_ci.ocs.defaults import OCS_OPERATOR_NAME, ODF_OPERATOR_NAME
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


def get_operator_csv_names(namespace=None):
    """
    Get CSV names for OCS and ODF operators.

    Args:
        namespace (str): Namespace where CSVs are located.
            Defaults to openshift-storage namespace.

    Returns:
        tuple: (ocs_csv_name, odf_csv_name)

    """
    if namespace is None:
        namespace = constants.OPENSHIFT_STORAGE_NAMESPACE

    ocs_csv_name = get_csv_name_start_with_prefix(
        csv_prefix=OCS_OPERATOR_NAME, namespace=namespace
    )
    odf_csv_name = get_csv_name_start_with_prefix(
        csv_prefix=ODF_OPERATOR_NAME, namespace=namespace
    )

    if not ocs_csv_name:
        log.warning(
            f"Could not find CSV for {OCS_OPERATOR_NAME} " f"in namespace {namespace}"
        )
    if not odf_csv_name:
        log.warning(
            f"Could not find CSV for {ODF_OPERATOR_NAME} " f"in namespace {namespace}"
        )

    return ocs_csv_name, odf_csv_name


def check_operatorcondition_upgradeable_false(
    operator_name, csv_name, namespace, timeout=300
):
    """
    Check if OperatorCondition shows Upgradeable=False with health warning.

    Args:
        operator_name (str): Name of the operator (for logging)
        csv_name (str): CSV name of the operator
        namespace (str): Namespace where OperatorCondition is located
        timeout (int): Timeout in seconds to wait for condition

    Returns:
        bool: True if condition is met, False otherwise

    """
    if not csv_name:
        log.warning(
            f"Skipping {operator_name} OperatorCondition check - " "CSV name not found"
        )
        return False

    log.info(f"Checking {operator_name} OperatorCondition: {csv_name}")
    operatorcondition = OCP(
        kind="OperatorCondition",
        namespace=namespace,
    )

    def _check_condition():
        """Check if OperatorCondition shows Upgradeable=False"""
        oc_data = operatorcondition.get(resource_name=csv_name, dont_raise=True)
        if not oc_data:
            return False

        # OperatorCondition can be a single item or list
        items = (
            oc_data.get("items", [])
            if isinstance(oc_data, dict) and "items" in oc_data
            else []
        )
        if not items and isinstance(oc_data, dict) and "status" in oc_data:
            # Single resource, not a list
            items = [oc_data]

        for item in items:
            conditions = item.get("status", {}).get("conditions", [])
            for condition in conditions:
                if condition.get("type") == "Upgradeable":
                    status = condition.get("status")
                    reason = condition.get("reason", "")
                    message = condition.get("message", "")
                    log.info(
                        f"{operator_name} OperatorCondition Upgradeable "
                        f"status: {status}, reason: {reason}, "
                        f"message: {message}"
                    )
                    # Check for exact reason "CephCluster health warning"
                    # or similar
                    if status == "False" and (
                        "CephClusterHealthNotOK" in reason
                        or (
                            "CephCluster health is HEALTH_WARN. "
                            "Details: [MON_DOWN: HEALTH_WARN" in message
                        )
                    ):
                        return True
        return False

    sample = TimeoutSampler(
        timeout=timeout,
        sleep=10,
        func=_check_condition,
    )

    try:
        if sample.wait_for_func_status(result=True):
            log.info(
                f"{operator_name} OperatorCondition shows "
                "Upgradeable=False with health warning reason"
            )
            return True
        else:
            log.warning(
                f"{operator_name} OperatorCondition did not show "
                "Upgradeable=False within timeout."
            )
            return False
    except Exception as e:
        log.warning(
            f"{operator_name} OperatorCondition may not be updated yet "
            f"or not found: {e}."
        )
        return False
