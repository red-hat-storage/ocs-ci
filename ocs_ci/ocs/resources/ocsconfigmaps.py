import logging

from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS

logger = logging.getLogger(__name__)


class OcsOperatorConfigMap(OCS):
    """
    Class to interact with OCS Operator ConfigMap
    """

    def __init__(self, **kwargs):
        """
        Initialize the OcsOperatorConfigMap instance.

        Args:
            **kwargs: Arbitrary keyword arguments.
        """
        super(OcsOperatorConfigMap, self).__init__(**kwargs)
        self.configmap_data = self.get().get("data", {})
        self.metadata = self.get().get("metadata", {})

    def get_csi_cluster_name(self):
        """
        Get the CSI cluster name.

        Returns:
            str: The CSI cluster name.
        """
        return self.configmap_data.get("CSI_CLUSTER_NAME")

    def get_csi_enable_topology(self):
        """
        Get the CSI enable topology.

        Returns:
            str: The CSI enable topology.
        """
        return self.configmap_data.get("CSI_ENABLE_TOPOLOGY")

    def get_csi_topology_domain_labels(self):
        """
        Get the CSI topology domain labels.

        Returns:
            str: The CSI topology domain labels.
        """
        return self.configmap_data.get("CSI_TOPOLOGY_DOMAIN_LABELS")

    def get_rook_csi_disable_driver(self):
        """
        Get the Rook CSI disable driver.

        Returns:
            str: The Rook CSI disable driver.
        """
        return self.configmap_data.get("ROOK_CSI_DISABLE_DRIVER")

    def get_rook_csi_enable_cephfs(self):
        """
        Get the Rook CSI enable CephFS.

        Returns:
            str: The Rook CSI enable CephFS.
        """
        return self.configmap_data.get("ROOK_CSI_ENABLE_CEPHFS")

    def get_rook_csi_enable_nfs(self):
        """
        Get the Rook CSI enable NFS.

        Returns:
            str: The Rook CSI enable NFS.
        """
        return self.configmap_data.get("ROOK_CSI_ENABLE_NFS")

    def get_rook_current_namespace_only(self):
        """
        Get the Rook current namespace only.

        Returns:
            str: The Rook current namespace only.
        """
        return self.configmap_data.get("ROOK_CURRENT_NAMESPACE_ONLY")

    def set_csi_cluster_name(self, value):
        """
        Set the CSI cluster name.

        Args:
            value (str): The CSI cluster name.
        """
        self.configmap_data["CSI_CLUSTER_NAME"] = value
        return self.ocp.patch(
            resource_name=constants.OCS_OPERATOR_CONFIG_MAP,
            params='{"data": {"CSI_CLUSTER_NAME": "%s"}}' % value,
            format_type="merge",
        )

    def set_csi_enable_topology(self, value):
        """
        Set the CSI enable topology.

        Args:
            value (str): The CSI enable topology.
        """
        self.configmap_data["CSI_ENABLE_TOPOLOGY"] = value
        return self.ocp.patch(
            resource_name=constants.OCS_OPERATOR_CONFIG_MAP,
            params='{"data": {"CSI_ENABLE_TOPOLOGY": "%s"}}' % value,
            format_type="merge",
        )

    def set_csi_topology_domain_labels(self, value):
        """
        Set the CSI topology domain labels.

        Args:
            value (str): The CSI topology domain labels.
        """
        self.configmap_data["CSI_TOPOLOGY_DOMAIN_LABELS"] = value
        return self.ocp.patch(
            resource_name=constants.OCS_OPERATOR_CONFIG_MAP,
            params='{"data": {"CSI_TOPOLOGY_DOMAIN_LABELS": "%s"}}' % value,
            format_type="merge",
        )

    def set_rook_csi_enable_cephfs(self, value):
        """
        Set the Rook CSI enable CephFS.

        Args:
            value (str): The Rook CSI enable CephFS.
        """
        self.configmap_data["ROOK_CSI_ENABLE_CEPHFS"] = value
        return self.ocp.patch(
            resource_name=constants.OCS_OPERATOR_CONFIG_MAP,
            params='{"data": {"ROOK_CSI_ENABLE_CEPHFS": "%s"}}' % value,
            format_type="merge",
        )

    def set_rook_csi_enable_nfs(self, value):
        """
        Set the Rook CSI enable NFS.

        Args:
            value (str): The Rook CSI enable NFS.
        """
        self.configmap_data["ROOK_CSI_ENABLE_NFS"] = value
        return self.ocp.patch(
            resource_name=constants.OCS_OPERATOR_CONFIG_MAP,
            params='{"data": {"ROOK_CSI_ENABLE_NFS": "%s"}}' % value,
            format_type="merge",
        )

    def set_rook_current_namespace_only(self, value):
        """
        Set the Rook current namespace only.

        Args:
            value (str): The Rook current namespace only.
        """
        self.configmap_data["ROOK_CURRENT_NAMESPACE_ONLY"] = value
        return self.ocp.patch(
            resource_name=constants.OCS_OPERATOR_CONFIG_MAP,
            params='{"data": {"ROOK_CURRENT_NAMESPACE_ONLY": "%s"}}' % value,
            format_type="merge",
        )

    def get_creation_timestamp(self):
        """
        Get the creation timestamp.

        Returns:
            str: The creation timestamp.
        """
        return self.metadata.get("creationTimestamp")

    def get_owner_references(self):
        """
        Get the owner references.

        Returns:
            list: The owner references.
        """
        return self.metadata.get("ownerReferences")

    def get_resource_version(self):
        """
        Get the resource version.

        Returns:
            str: The resource version.
        """
        return self.metadata.get("resourceVersion")

    def get_uid(self):
        """
        Get the UID.

        Returns:
            str: The UID.
        """
        return self.metadata.get("uid")

    def check_configmap_exists(self, timeout=120):
        """
        Check if the OCS Operator ConfigMap exists. Does not raise TimeoutExpiredError exception.

        Args:
            timeout (int): Time to wait for resource to exist (default: 120)

        Returns:
            bool: True if ConfigMap exists, False otherwise
        """
        return self.ocp.check_resource_existence(
            resource_name=constants.OCS_OPERATOR_CONFIG_MAP,
            timeout=timeout,
            should_exist=True,
        )


class OcsStorageConsumerConfigMap(OCS):
    """
    Class to interact with OCS Storage Consumer ConfigMap
    """

    def __init__(self, **kwargs):
        """
        Initialize the OcsStorageConsumerConfigMap instance.

        Args:
            **kwargs: Arbitrary keyword arguments.
        """
        super(OcsStorageConsumerConfigMap, self).__init__(**kwargs)
        self.configmap_data = self.get().get("data", {})
        self.metadata = self.get().get("metadata", {})

    def get_cephfs_subvolumegroup(self):
        """
        Get the cephfs subvolumegroup.

        Returns:
            str: The cephfs subvolumegroup.
        """
        return self.configmap_data.get("cephfs-subvolumegroup")

    def get_cephfs_subvolumegroup_rados_ns(self):
        """
        Get the cephfs subvolumegroup rados namespace.

        Returns:
            str: The cephfs subvolumegroup rados namespace.
        """
        return self.configmap_data.get("cephfs-subvolumegroup-rados-ns")

    def get_csi_cephfs_node_secret(self):
        """
        Get the csi cephfs node secret.

        Returns:
            str: The csi cephfs node secret.
        """
        return self.configmap_data.get("csi-cephfs-node-secret")

    def get_csi_cephfs_provisioner_secret(self):
        """
        Get the csi cephfs provisioner secret.

        Returns:
            str: The csi cephfs provisioner secret.
        """
        return self.configmap_data.get("csi-cephfs-provisioner-secret")

    def get_csi_rbd_node_secret(self):
        """
        Get the csi rbd node secret.

        Returns:
            str: The csi rbd node secret.
        """
        return self.configmap_data.get("csi-rbd-node-secret")

    def get_csi_rbd_provisioner_secret(self):
        """
        Get the csi rbd provisioner secret.

        Returns:
            str: The csi rbd provisioner secret.
        """
        return self.configmap_data.get("csi-rbd-provisioner-secret")

    def get_csiop_cephfs_client_profile(self):
        """
        Get the csiop cephfs client profile.

        Returns:
            str: The csiop cephfs client profile.
        """
        return self.configmap_data.get("csiop-cephfs-client-profile")

    def get_csiop_rbd_client_profile(self):
        """
        Get the csiop rbd client profile.

        Returns:
            str: The csiop rbd client profile.
        """
        return self.configmap_data.get("csiop-rbd-client-profile")

    def get_rbd_rados_ns(self):
        """
        Get the rbd rados namespace.

        Returns:
            str: The rbd rados namespace.
        """
        return self.configmap_data.get("rbd-rados-ns")

    def set_cephfs_subvolumegroup(self, value):
        """
        Set the cephfs subvolumegroup.

        Args:
            value (str): The cephfs subvolumegroup.
        """
        self.configmap_data["cephfs-subvolumegroup"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"cephfs-subvolumegroup": "%s"}}' % value,
            format_type="merge",
        )

    def set_cephfs_subvolumegroup_rados_ns(self, value):
        """
        Set the cephfs subvolumegroup rados namespace.

        Args:
            value (str): The cephfs subvolumegroup rados namespace.
        """
        self.configmap_data["cephfs-subvolumegroup-rados-ns"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"cephfs-subvolumegroup-rados-ns": "%s"}}' % value,
            format_type="merge",
        )

    def set_csi_cephfs_node_secret(self, value):
        """
        Set the csi cephfs node secret.

        Args:
            value (str): The csi cephfs node secret.
        """
        self.configmap_data["csi-cephfs-node-secret"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"csi-cephfs-node-secret": "%s"}}' % value,
            format_type="merge",
        )

    def set_csi_cephfs_provisioner_secret(self, value):
        """
        Set the csi cephfs provisioner secret.

        Args:
            value (str): The csi cephfs provisioner secret.
        """
        self.configmap_data["csi-cephfs-provisioner-secret"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"csi-cephfs-provisioner-secret": "%s"}}' % value,
            format_type="merge",
        )

    def set_csi_rbd_node_secret(self, value):
        """
        Set the csi rbd node secret.

        Args:
            value (str): The csi rbd node secret.
        """
        self.configmap_data["csi-rbd-node-secret"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"csi-rbd-node-secret": "%s"}}' % value,
            format_type="merge",
        )

    def set_csi_rbd_provisioner_secret(self, value):
        """
        Set the csi rbd provisioner secret.

        Args:
            value (str): The csi rbd provisioner secret.
        """
        self.configmap_data["csi-rbd-provisioner-secret"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"csi-rbd-provisioner-secret": "%s"}}' % value,
            format_type="merge",
        )

    def set_csiop_cephfs_client_profile(self, value):
        """
        Set the csiop cephfs client profile.

        Args:
            value (str): The csiop cephfs client profile.
        """
        self.configmap_data["csiop-cephfs-client-profile"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"csiop-cephfs-client-profile": "%s"}}' % value,
            format_type="merge",
        )

    def set_csiop_rbd_client_profile(self, value):
        """
        Set the csiop rbd client profile.

        Args:
            value (str): The csiop rbd client profile.
        """
        self.configmap_data["csiop-rbd-client-profile"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"csiop-rbd-client-profile": "%s"}}' % value,
            format_type="merge",
        )

    def set_rbd_rados_ns(self, value):
        """
        Set the rbd rados namespace.

        Args:
            value (str): The rbd rados namespace.
        """
        self.configmap_data["rbd-rados-ns"] = value
        return self.ocp.patch(
            resource_name=f"storageconsumer-{self.name}",
            params='{"data": {"rbd-rados-ns": "%s"}}' % value,
            format_type="merge",
        )

    def get_creation_timestamp(self):
        """
        Get the creation timestamp.

        Returns:
            str: The creation timestamp.
        """
        return self.metadata.get("creationTimestamp")

    def get_owner_references(self):
        """
        Get the owner references.

        Returns:
            list: The owner references.
        """
        return self.metadata.get("ownerReferences")

    def get_resource_version(self):
        """
        Get the resource version.

        Returns:
            str: The resource version.
        """
        return self.metadata.get("resourceVersion")

    def get_uid(self):
        """
        Get the UID.

        Returns:
            str: The UID.
        """
        return self.metadata.get("uid")

    def check_configmap_exists(self, timeout=120):
        """
        Check if the OCS Storage Consumer ConfigMap exists. Does not raise TimeoutExpiredError exception.

        Args:
            timeout (int): Time to wait for resource to exist (default: 120)

        Returns:
            bool: True if ConfigMap exists, False otherwise
        """
        return self.ocp.check_resource_existence(timeout=timeout, should_exist=True)


def get_ocs_operator_configmap_obj():
    """
    Get OCS Operator ConfigMap instance

    Returns:
        OcsOperatorConfigMap: OCS Operator ConfigMap instance
    """
    ocp_obj = OCP(
        kind=constants.CONFIGMAP, resource_name=constants.OCS_OPERATOR_CONFIG_MAP
    )
    ocs_obj = OcsOperatorConfigMap(**ocp_obj.data)
    ocs_obj.reload()
    return ocs_obj


def get_ocs_storage_consumer_configmap_obj(storageconsumer_name):
    """
    Get OCS Storage Consumer ConfigMap instance

    Args:
        storageconsumer_name (str): Name of the storage consumer

    Returns:
        OcsStorageConsumerConfigMap: OCS Storage Consumer ConfigMap instance
    """
    ocp_obj = OCP(
        kind=constants.STORAGECONSUMER,
        resource_name=storageconsumer_name,
        namespace=config.ENV_DATA["cluster_namespace"],
    ).get()
    cm_name = ocp_obj.get("status").get("resourceNameMappingConfigMap").get("name")

    provider_cluster_index = config.get_provider_index()
    provider_cluster_kubeconfig = config.get_cluster_kubeconfig_by_index(
        provider_cluster_index
    )

    ocp_obj = OCP(
        kind=constants.CONFIGMAP,
        resource_name=cm_name,
        namespace=config.ENV_DATA["cluster_namespace"],
        cluster_kubeconfig=provider_cluster_kubeconfig,
    ).get()

    return ocp_obj
