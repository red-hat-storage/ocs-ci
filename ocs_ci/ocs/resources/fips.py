import logging
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import FipsNotInstalledException, CommandFailed
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)


def check_fips_enabled(fips_location=constants.FIPS_LOCATION):
    """
    Checks if FIPS is activated on all pods

    Args:
        fips_location: File that refers to fips, written 1 if enabled,
            0 otherwise
    Raises:
        FipsNotInstalledException:
            If the value of fips location file does not include 1
                in all pods within the given namespace.

    """
    # ignore rook-ceph-detect-version pods
    running_pods_object = pod.get_running_state_pods(
        ignore_selector=["rook-ceph-detect-version"]
    )
    for running_pod in running_pods_object:
        fips_value = _get_fips_value_from_pod(running_pod, fips_location)
        if str(fips_value).strip() != "1":
            raise FipsNotInstalledException(
                "Error in the installation of FIPS on the cluster!"
                f"Found value different than 1 in pod {running_pod.name}"
                f"Value: {fips_value}"
            )
        else:
            log.info(f"Pod {running_pod.name} is FIPS enabled!")


@retry(
    CommandFailed,
    tries=3,
    delay=30,
    backoff=1,
    text_in_exception="container not found",
)
def _get_fips_value_from_pod(running_pod, fips_location):
    """
    Get FIPS value from a pod with retry logic for transient container issues.

    This function retries when encountering "container not found" errors which can
    occur due to etcd timeouts or temporary pod state issues during leader election.

    Args:
        running_pod: Pod object to query
        fips_location: Path to the FIPS enabled file

    Returns:
        str: FIPS value from the pod
    """
    return running_pod.exec_sh_cmd_on_pod(f"cat {fips_location}")
