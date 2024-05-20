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


class EphernalPodFactory:
    @staticmethod
    def create_pod_dict(pod_name: str, storage_type: str, **kwargs):

        if storage_type == CEPHFS_INTERFACE:
            return EphernalPodFactory.create_cephfs_pod_dict(pod_name, **kwargs)
        elif storage_type == RBD_INTERFACE:
            return EphernalPodFactory.create_rbd_pod_dict(pod_name, **kwargs)

    @staticmethod
    def create_cephfs_pod_dict(pod_name: str, **kwargs) -> dict:
        pod_dict_path = EPHEMERAL_FS_POD_YAML
        pod_dict = load_yaml(pod_dict_path)
        return pod_dict

    @staticmethod
    def create_rbd_pod_dict(pod_name: str, **kwargs) -> dict:
        pod_dict_path = EPHEMERAL_RBD_POD_YAML
        pod_dict = load_yaml(pod_dict_path)
        return pod_dict

    @staticmethod
    def create_ephmeral_pod(pod_name: str, storage_type: str, **kwargs) -> Pod:
        pod_dict = EphernalPodFactory.create_pod_dict(pod_name, storage_type, **kwargs)
        pod_obj = Pod(**pod_dict)

        if not pod_name:
            pod_name = create_unique_resource_name(f"test-{storage_type}", "pod")

        pod_dict["metadata"]["name"] = pod_name
        pod_dict["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        log.info(f"Creating new Pod {pod_name} for test")
        created_resource = pod_obj.create()
        assert created_resource, f"Failed to create Pod {pod_name}"
        return created_resource
