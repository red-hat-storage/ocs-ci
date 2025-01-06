"""
A module for all StorageConsumer functionalities and abstractions.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility.utils import exec_cmd

log = logging.getLogger(__name__)


class StorageConsumer:
    """
    Base StorageConsumer class
    """

    def __init__(self, consumer_name, consumer_context=None):
        """
        Args:
            consumer_name (string): name of the StorageConsumer resource
            consumer_context (int): index of cluster context. This is needed for
                consumer operations executed on consumer
                (e.g. manipulation of heartbeat cronjob)
        """
        self.consumer_context = consumer_context
        self.name = consumer_name
        self.ocp = ocp.OCP(
            resource_name=self.name,
            kind=constants.STORAGECONSUMER,
            namespace=config.cluster_ctx.ENV_DATA["cluster_namespace"],
        )
        if self.consumer_context:
            self.provider_context = config.cluster_ctx.MULTICLUSTER[
                "multicluster_index"
            ]
            self.heartbeat_cronjob = self.get_heartbeat_cronjob()
        else:
            self.provider_context = None
            self.heartbeat_cronjob = None

    def get_ocs_version(self):
        """
        Get ocs version from storageconsumer resource.

        Returns:
            string: consumer ocs version

        """
        return (
            self.ocp.get(resource_name=self.name)
            .get("status")
            .get("client")
            .get("operatorVersion")
        )

    def set_ocs_version(self, version):
        """
        Update ocs consumer version in storageconsumer resource. This change assumes
        that the hearthbeat is stopped so that the version is not overwritten by it.

        Args:
            version (str): OCS version to be set

        """
        cmd = [
            "oc",
            "patch",
            "StorageConsumer",
            self.name,
            "--type",
            "json",
            "-p="
            + "'"
            + f'[{{"op": "replace", "path": "/status/client/operatorVersion", "value":"{version}"}}]'
            + "'",
            "--subresource",
            "status",
            "--namespace",
            config.cluster_ctx.ENV_DATA["cluster_namespace"],
        ]
        exec_cmd(" ".join(cmd))

    def stop_heartbeat(self):
        """
        Suspend status reporter cron job.
        """
        with config.RunWithConfigContext(self.consumer_context):
            patch_param = '{"spec": {"suspend": true}}'
            self.heartbeat_cronjob.ocp.patch(
                resource_name=self.heartbeat_cronjob.name, params=patch_param
            )

    def resume_heartbeat(self):
        """
        Resume status reporter cron job.
        """
        with config.RunWithConfigContext(self.consumer_context):
            patch_param = '{"spec": {"suspend": false}}'
            self.heartbeat_cronjob.ocp.patch(
                resource_name=self.heartbeat_cronjob.name, params=patch_param
            )

    def get_heartbeat_cronjob(self):
        """
        Returns:
            object: status reporter cronjob OCS object

        """
        with config.RunWithConfigContext(self.consumer_context):
            cronjobs_obj = ocp.OCP(
                kind=constants.CRONJOB,
                namespace=config.cluster_ctx.ENV_DATA["cluster_namespace"],
            )
            cronjob = [
                OCS(**job)
                for job in cronjobs_obj.get().get("items")
                if job["metadata"]["name"].endswith("status-reporter")
            ][0]
        return cronjob

    def fill_up_quota_percentage(self, percentage, quota=None):
        """
        Create a PVC of such size that the correct percentage of quota is used

        Returns:
            PVC object
        """
        pvc_name = f"pvc-quota-{percentage}"
        if not quota:
            quota = config.ENV_DATA["quota"]
        quota_value = quota.split(" ")[0]
        quota_units = quota.split(" ")[1]
        pvc_size_int = quota_value * percentage // 100
        pvc_size = f"{pvc_size_int}{quota_units}"
        rbd_storageclass = helpers.default_storage_class(constants.CEPHBLOCKPOOL)
        pvc_obj = helpers.create_pvc(
            pvc_name=pvc_name,
            sc_name=rbd_storageclass,
            namespace="default",
            size=pvc_size,
            do_reload=False,
            access_mode=constants.ACCESS_MODE_RWO,
            volume_mode=constants.VOLUME_MODE_BLOCK,
        )
        return pvc_obj


def get_all_client_clusters():
    """
    Get client cluster names of all storage consumers

    Returns:
        array: names of client clusters
    """
    ocp_storageconsumers = ocp.OCP(
        kind=constants.STORAGECONSUMER,
        namespace=config.cluster_ctx.ENV_DATA["cluster_namespace"],
    )
    cluster_names = []
    storageconsumers_data = ocp_storageconsumers.get().get("items")
    for storageconsumer in storageconsumers_data:
        cluster_names.append(storageconsumer["status"]["client"]["clusterName"])
    return cluster_names


def get_storageconsumer_quota(cluster_name):
    """
    Get the quota value from storageconsumer details
    Args:
        clustername(str): name of the client cluster
    Returns"
        str: quota value
    """
    ocp_storageconsumers = ocp.OCP(
        kind=constants.STORAGECONSUMER,
        namespace=config.cluster_ctx.ENV_DATA["cluster_namespace"],
    )
    storageconsumers_data = ocp_storageconsumers.get().get("items")
    for storageconsumer in storageconsumers_data:
        if storageconsumer["status"]["client"]["clusterName"] == cluster_name:
            if "storageQuotaInGiB" not in storageconsumer["spec"]:
                return "Unlimited"
            return storageconsumer["spec"]["storageQuotaInGiB"]
