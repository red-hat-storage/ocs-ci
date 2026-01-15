"""
CSV related functionalities
"""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.defaults import OCS_OPERATOR_NAME, ODF_OPERATOR_NAME
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


class CSV(OCP):
    """
    This class represent ClusterServiceVersion (CSV) and contains all related
    methods we need to do with CSV.
    """

    _has_phase = True

    def __init__(self, resource_name="", *args, **kwargs):
        """
        Initializer function for CSV class

        Args:
            resource_name (str): Name of CSV

        """
        super(CSV, self).__init__(
            resource_name=resource_name, kind="csv", *args, **kwargs
        )


def get_csvs_start_with_prefix(csv_prefix, namespace):
    """
    Get CSVs start with prefix

    Args:
        csv_prefix (str): prefix from name
        namespace (str): namespace of CSV

    Returns:
        list: found CSVs

    """

    csvs = CSV(namespace=namespace)
    csv_list = csvs.get()["items"]
    return [csv for csv in csv_list if csv["metadata"]["name"].startswith(csv_prefix)]


def get_csv_name_start_with_prefix(csv_prefix, namespace):
    """
    Get CSV name start with prefix

    Args:
        csv_prefix (str): prefix from name
        namespace (str): namespace of CSV

    Returns:
        str: CSV name

    """
    csvs = CSV(namespace=namespace)
    csv_list = csvs.get()["items"]
    for csv in csv_list:
        csv_name = csv["metadata"]["name"]
        if csv_prefix in csv_name:
            return csv_name


def check_all_csvs_are_succeeded(namespace, timeout=600, cluster_kubeconfig=""):
    """
    Check if all CSVs in namespace are in succeeded phase

    Args:
        namespace (str): namespace of CSV
        timeout (int): Timeout in seconds to wait for CSV to reach succeeded phase
            ! currently not used !
        cluster_kubeconfig (str): Kubeconfig of the cluster

    Returns:
        bool: True if all CSVs are in succeeded phase

    """

    csvs = CSV(namespace=namespace, cluster_kubeconfig=cluster_kubeconfig)
    csv_list = csvs.get()["items"]
    for csv in csv_list:
        csv_name = csv["metadata"]["name"]
        csv_phase = csv["status"]["phase"]
        log.info(f"CSV: {csv_name} is in phase: {csv_phase}")
        if csv_phase != "Succeeded":
            log.warning(
                f"CSV: {csv_name} is not in Succeeded phase! Current phase: {csv_phase}"
            )
            return False
    return True


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
                        or ("CephCluster health is HEALTH_WARN. " in message)
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


def check_operatorcondition_upgradeable_false_version_mismatch(
    operator_name, csv_name, namespace, timeout=300
):
    """
    Check if OperatorCondition shows Upgradeable=False due to ODFVersionAheadOfOCP.

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
                    # Check for version mismatch reasons
                    # Expected: Reason: "ODFVersionAheadOfOCP"
                    # Expected message :
                    # "ODF version is already ahead of OCP."
                    if (
                        status == "False"
                        and "ODFVersionAheadOfOCP" in reason
                        and "ODF version is already ahead of OCP" in message
                        and "Further ODF upgrade would make it incompatible" in message
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
                "Upgradeable=False with ODFVersionAheadOfOCP reason"
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
