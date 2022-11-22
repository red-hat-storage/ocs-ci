from ocs_ci.ocs.ocp import OCP
import logging
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.resources.storage_cluster import verify_storage_cluster

log = logging.getLogger(__name__)


def clear_overprovision_spec(ignore_errors=False):
    """
    Remove cluster overprovision policy.
    """
    log.info("Removing overprovisionControl from storage cluster.")
    storagecluster_obj = OCP(
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        kind=constants.STORAGECLUSTER,
    )

    params = '[{"op": "remove", path: "/spec/overprovisionControl"}]'
    try:
        storagecluster_obj.patch(params=params, format_type="json")
    except Exception as e:
        print(e)
        if not ignore_errors:
            return False

    log.info("Verify storagecluster on Ready state")
    verify_storage_cluster()
    return True


def set_overprovision_policy(capacity, quota_name, sc_name, label):
    """
    Set OverProvisionControl Policy.

    Args:
        capacity (str): storage capacity e.g. 50Gi
        quota_name (str): quota name.
        sc_name (str): storage class name
        label (dict): storage quota labels.

    Return:
        None
    """
    log.info("Add 'overprovisionControl' section to storagecluster yaml file")
    params = (
        '{"spec": {"overprovisionControl": [{"capacity": "' + capacity + '",'
        '"storageClassName":"' + sc_name + '", "quotaName": "' + quota_name + '",'
        '"selector": {"labels": {"matchLabels": '
        + label.__str__().replace("'", '"')
        + "}}}]}}"
    )

    storagecluster_obj = OCP(
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
        kind=constants.STORAGECLUSTER,
    )

    storagecluster_obj.patch(
        params=params,
        format_type="merge",
    )
    log.info("Verify storagecluster on Ready state")
    verify_storage_cluster()
