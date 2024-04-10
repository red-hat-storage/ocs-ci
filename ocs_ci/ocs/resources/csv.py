"""
CSV related functionalities
"""
import logging

from ocs_ci.ocs.ocp import OCP

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
    return [csv for csv in csv_list if csv_prefix in csv["metadata"]["name"]]


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


def check_all_csvs_are_succeeded(namespace, timeout=600):
    """
    Check if all CSVs in namespace are in succeeded phase

    Args:
        namespace (str): namespace of CSV

    Returns:
        bool: True if all CSVs are in succeeded phase

    """

    csvs = CSV(namespace=namespace)
    csv_list = csvs.get()["items"]
    for csv in csv_list:
        csv_name = csv["metadata"]["name"]
        csv_phase = csv["status"]["phase"]
        log.info(f"CSV: {csv_name} is in phase: {csv_phase}")
        if csv_phase != "Succeeded":
            log.error(
                f"CSV: {csv_name} is not in Succeeded phase! Current phase: {csv_phase}"
            )
            return False
    return True
