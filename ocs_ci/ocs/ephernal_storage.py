from logging import getLogger

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.constants import (
    CEPHFS_INTERFACE,
    RBD_INTERFACE,
    EPHEMERAL_RBD_POD_YAML,
    EPHEMERAL_FS_POD_YAML,
)

from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.utility.templating import load_yaml

log = getLogger(__name__)


class EphemeralPodFactory:
    @staticmethod
    def create_pod_dict(pod_name: str, storage_type: str, **kwargs):

        if storage_type == CEPHFS_INTERFACE:
            return EphemeralPodFactory.create_cephfs_pod_dict(pod_name, **kwargs)
        elif storage_type == RBD_INTERFACE:
            return EphemeralPodFactory.create_rbd_pod_dict(pod_name, **kwargs)

    @staticmethod
    def create_cephfs_pod_dict(pod_name: str, **kwargs) -> dict:
        """
        This method creates a dictionary representing a CephFS pod configuration.

        Args:
            pod_name (str): The name of the pod.

        Returns:
            pod_dict: A dictionary representing the CephFS pod configuration.

        """
        pod_dict_path = EPHEMERAL_FS_POD_YAML
        pod_dict = load_yaml(pod_dict_path)
        return pod_dict

    @staticmethod
    def create_rbd_pod_dict(pod_name: str, **kwargs) -> dict:
        """
        This method creates a dictionary representing a RBD pod configuration.

        Args:
            pod_name (str): The name of the pod.

        Returns:
            pod_dict: A dictionary representing the RBD pod configuration.

        """
        pod_dict_path = EPHEMERAL_RBD_POD_YAML
        pod_dict = load_yaml(pod_dict_path)
        return pod_dict

    @staticmethod
    def create_ephemeral_pod(pod_name: str, storage_type: str, **kwargs) -> Pod:
        """
        This method creates a new ephemeral pod based on the specified storage type.

        Args:
            pod_name (str): The name of the pod.
            storage_type (str): The type of storage interface (CephFS or RBD).

        Returns:
            created_resources: A new ephemeral pod object.

        """
        pod_dict = EphemeralPodFactory.create_pod_dict(pod_name, storage_type, **kwargs)
        pod_dict["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        pod_obj = Pod(**pod_dict)

        if not pod_name:
            pod_name = create_unique_resource_name(f"test-{storage_type}", "pod")

        pod_dict["metadata"]["name"] = pod_name
        log.info(f"Creating new Pod {pod_name} for test")
        created_resource = pod_obj.create()
        assert created_resource, f"Failed to create Pod {pod_name}"
        return created_resource

    @staticmethod
    def delete_ephemeral_pod(pod_name: str, namespace: str) -> None:
        """
        This method deletes an existing ephemeral pod.

        Args:
            pod_name (str): The name of the pod.
            namespace (str): The namespace of the pod.

        """
        pod_dict = {
            "metadata": {
                "name": pod_name,
                "namespace": namespace,
            }
        }
        pod_obj = Pod(**pod_dict)
        pod_obj.delete()
