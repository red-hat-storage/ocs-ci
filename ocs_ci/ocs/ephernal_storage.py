from logging import getLogger

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.constants import (
    POD,
    CEPHFS_INTERFACE,
    RBD_INTERFACE,
    CEPHBLOCKPOOL_SC,
    ACCESS_MODE_RWO,
    ACCESS_MODE_RWX,
    CEPHFILESYSTEM_SC,
)

from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import Pod

log = getLogger(__name__)


class EphernalPodFactory:

    ephernal_pod = {
        "kind": POD,
        "apiVersion": "v1",
        "metadata": {
            "name": "pod_name1",
            "namespace": config.ENV_DATA["cluster_namespace"],
        },
        "spec": {
            "containers": [
                {
                    "name": "my-frontend",
                    "image": "quay.io/ocsci/nginx:fio",
                    "volumeMounts": [
                        {"mountPath": "/scratch", "name": "scratch-volume"}
                    ],
                    "command": ["sleep", "1000000"],
                }
            ],
            "volumes": [],
        },
    }

    rbd_volume_dict = {
        "name": "scratch-volume",
        "ephemeral": {
            "volumeClaimTemplate": {
                "metadata": {
                    "labels": {"type": "my-frontend-volume", "test": "ephemeral"},
                },
                "spec": {
                    "accessModes": [ACCESS_MODE_RWO],
                    "storageClassName": CEPHBLOCKPOOL_SC,
                    "resources": {"requests": {"storage": "1Gi"}},
                },
            }
        },
    }

    cephfs_volume_dict = {
        "name": "scratch-volume",
        "ephemeral": {
            "volumeClaimTemplate": {
                "metadata": {"labels": {"type": "my-frontend-volume"}},
                "spec": {
                    "accessModes": [ACCESS_MODE_RWX],
                    "storageClassName": CEPHFILESYSTEM_SC,
                    "resources": {"requests": {"storage": "1Gi"}},
                },
            }
        },
    }

    @staticmethod
    def create_pod_dict(pod_name: str, storage_type: str, **kwargs):
        pod_dict = EphernalPodFactory.ephernal_pod.copy()
        if not pod_name:
            pod_name = create_unique_resource_name(f"test-{storage_type}", "pod")
        pod_dict["metadata"]["name"] = pod_name

        if storage_type == CEPHFS_INTERFACE:
            return EphernalPodFactory.create_cephfs_pod_dict(
                pod_dict, pod_name, **kwargs
            )
        elif storage_type == RBD_INTERFACE:
            return EphernalPodFactory.create_rbd_pod_dict(pod_dict, pod_name, **kwargs)

    @staticmethod
    def create_cephfs_pod_dict(pod_dict: dict, pod_name: str, **kwargs) -> dict:
        volumes_list = [EphernalPodFactory.cephfs_volume_dict]
        pod_dict["spec"]["volumes"] = volumes_list
        return pod_dict

    @staticmethod
    def create_rbd_pod_dict(pod_dict: dict, pod_name: str, **kwargs) -> dict:
        volumes_list = [EphernalPodFactory.rbd_volume_dict]
        pod_dict["spec"]["volumes"] = volumes_list
        return pod_dict

    @staticmethod
    def create_ephmeral_pod(pod_name: str, storage_type: str, **kwargs) -> Pod:
        pod_dict = EphernalPodFactory.create_pod_dict(pod_name, storage_type, **kwargs)
        pod_obj = Pod(**pod_dict)
        pod_name = pod_name
        log.info(f"Creating new Pod {pod_name} for test")
        created_resource = pod_obj.create()
        assert created_resource, f"Failed to create Pod {pod_name}"
        return created_resource
