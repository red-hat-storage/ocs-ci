import logging
import tempfile

from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.utility import templating
from ocs_ci.utility.templating import dump_data_to_temp_yaml

logger = logging.getLogger(__name__)


class ClientProfile:
    """
    Base ClientProfile class
    """

    def __init__(self, client_profile_name, consumer_context=None):

        self.consumer_context = consumer_context
        self.name = client_profile_name
        self.ocp = ocp.OCP(
            resource_name=self.name,
            kind=constants.CLIENT_PROFILE,
            namespace=config.cluster_ctx.ENV_DATA["cluster_namespace"],
        )
        if self.consumer_context:
            self.provider_context = config.cluster_ctx.MULTICLUSTER[
                "multicluster_index"
            ]
        else:
            self.provider_context = None

    def get_ceph_connection_reference(self):
        """
        Get the CephConnectionReference name

        Returns:
            str: CephConnectionReference name
        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name)
                .get("spec")
                .get("cephConnectionRef")
                .get("name")
            )

    def get_ceph_fs_map(self):
        """
        Get the CephFSMap from the client profile

        SubVolumeGroup          string              json:"subVolumeGroup,omitempty"
        KernelMountOptions      map[string]         string `json:"kernelMountOptions,omitempty"`
        FuseMountOptions        map[string]         string `json:"fuseMountOptions,omitempty"`

        Starting from ODF 4.19 (Converged) this CR has optional Spec field RadosNamespace.
        It is to ensure ceph fs has namespace for storing metadata (OMAP data)
        RadosNamespace          string(can be nil)  json:"radosNamespace,omitempty"

        Returns:
            dict: CephFSMap
        """
        with config.RunWithConfigContext(self.consumer_context):
            return self.ocp.get(resource_name=self.name).get("spec").get("cephFs")

    def get_rbd_map(self):
        """
        Get the RBDMap from the client profile

        Returns:
            dict: RBDMap
        """
        with config.RunWithConfigContext(self.consumer_context):
            return self.ocp.get(resource_name=self.name).get("spec").get("rbd")

    def get_nfs_map(self):
        """
        Get the NFSMap from the client profile

        Returns:
            dict: NFSMap
        """
        with config.RunWithConfigContext(self.consumer_context):
            return self.ocp.get(resource_name=self.name).get("spec").get("nfs")

    def create_client_profile(
        self,
        name,
        ceph_connection_reference,
        ceph_fs_map: dict,
        rbd_map: dict,
        nfs_map: dict,
    ):
        """
        Create a client profile

        Returns:
            dict: ClientProfile
        """
        with config.RunWithConfigContext(self.consumer_context):
            client_profile_data = templating.load_yaml(constants.CLIENT_PROFILE_PATH)
            client_profile_data["metadata"]["name"] = name
            client_profile_data["spec"]["cephConnectionRef"][
                "name"
            ] = ceph_connection_reference
            if ceph_fs_map:
                client_profile_data["spec"]["cephFs"] = ceph_fs_map
            if rbd_map:
                client_profile_data["spec"]["rbd"] = rbd_map
            if nfs_map:
                client_profile_data["spec"]["nfs"] = nfs_map

            client_profile_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="client_profile", delete=False
            )
            dump_data_to_temp_yaml(client_profile_data, client_profile_file.name)

            return self.ocp.create(yaml_file=client_profile_file.name)
