from logging import getLogger

# from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.constants import POD, CEPHFS_INTERFACE, RBD_INTERFACE
from ocs_ci.framework import config

log = getLogger(__name__)


class EphernalPodFactory:

    ephernal_pod = {
        "kind": POD,
        "apiVersion": "v1",
        "metadata": {
            "name": "pod_name",
            "namespace": config.ENV_DATA["cluster_namespace"],
        },
        "spec": {
            "containers": [
                {
                    "name": "my-frontend",
                    "image": "busybox:1.28",
                    "volumeMounts": [
                        {"mountPath": "/scratch", "name": "scratch-volume"}
                    ],
                    "command": ["sleep", "1000000"],
                }
            ],
            "volumes": [
                {
                    "name": "scratch-volume",
                    "ephemeral": {
                        "volumeClaimTemplate": {
                            "metadata": {"labels": {"type": "my-frontend-volume"}},
                            "spec": {
                                "accessModes": ["ReadWriteOnce"],
                                "storageClassName": "ocs-storagecluster-ceph-rbd",
                                "resources": {"requests": {"storage": "1Gi"}},
                            },
                        }
                    },
                }
            ],
        },
    }

    @staticmethod
    def create_pod(pod_dict, pod_name, namespace, interface_type, **kwargs):
        if interface_type == CEPHFS_INTERFACE:
            return EphernalPodFactory.create_cephfs_pod(
                pod_dict, pod_name, namespace, **kwargs
            )
        elif interface_type == RBD_INTERFACE:
            return EphernalPodFactory.create_rbd_pod(
                pod_dict, pod_name, namespace, **kwargs
            )
        else:
            return EphernalPodFactory.create_generic_pod(
                pod_dict, pod_name, namespace, **kwargs
            )

    @staticmethod
    def create_cephfs_pod(pod_dict, pod_name, namespace, **kwargs):
        # code for creating cephfs pod
        pass

    @staticmethod
    def create_rbd_pod(pod_dict, pod_name, namespace, **kwargs):
        # code for creating rbd pod
        pass

    @staticmethod
    def create_generic_pod(pod_dict, pod_name, namespace, **kwargs):
        # code for creating generic pod
        pass
