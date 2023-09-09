from ocs_ci.framework import config
from ocs_ci.ocs import constants


def storageclass_name(interface, external_mode=False):
    """
    This Function will return the default storage class name from the cluster.
    This will return only "cephfs", "rbd" and "rgw" type storage class names only.
    Args:
        interface (str): storage class interface type
        external_mode (bool) : True if external mode setup else False.
    Returns:
        (str): storage class name
    """

    INTERFACE_TO_SC_MAP = {
        constants.OCS_COMPONENTS_MAP["blockpools"]: {
            True: constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD,
            False: constants.DEFAULT_STORAGECLASS_RBD,
        },
        constants.OCS_COMPONENTS_MAP["cephfs"]: {
            True: constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS,
            False: constants.DEFAULT_STORAGECLASS_CEPHFS,
        },
        constants.OCS_COMPONENTS_MAP["rgw"]: {
            True: constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RGW,
            False: constants.DEFAULT_STORAGECLASS_RGW,
        },
    }

    CLUSTER_NAME = (
        constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE
        if config.DEPLOYMENT["external_mode"]
        else constants.DEFAULT_CLUSTERNAME
    )

    from ocs_ci.ocs.resources import storage_cluster

    sc_obj = storage_cluster.StorageCluster(
        resource_name=CLUSTER_NAME,
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
    )

    custom_sc_name = (
        sc_obj.get()
        .get("spec", {})
        .get("managedResources", {})
        .get(interface, {})
        .get("storageClassName")
    )
    if custom_sc_name:
        return custom_sc_name

    return INTERFACE_TO_SC_MAP.get(interface, "").get(external_mode, "")
