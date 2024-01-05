"""
A module for all StorageConsumer functionalities and abstractions.
"""
import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
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
        ]
        exec_cmd(" ".join(cmd))

    def stop_heartbeat(self):
        """
        Suspend status reporter cron job.
        """
        self._switch_consumer_cluster()
        patch_param = '{"spec": {"suspend": true}}'
        self.heartbeat_cronjob.ocp.patch(
            resource_name=self.heartbeat_cronjob.name, params=patch_param
        )
        self._switch_provider_cluster()

    def resume_heartbeat(self):
        """
        Resume status reporter cron job.
        """
        self._switch_consumer_cluster()
        patch_param = '{"spec": {"suspend": false}}'
        self.heartbeat_cronjob.ocp.patch(
            resource_name=self.heartbeat_cronjob.name, params=patch_param
        )
        self._switch_provider_cluster()

    def get_heartbeat_cronjob(self):
        """
        Returns:
            object: status reporter cronjob OCS object

        """
        self._switch_consumer_cluster()
        cronjobs_obj = ocp.OCP(
            kind=constants.CRONJOB,
            namespace=config.cluster_ctx.ENV_DATA["cluster_namespace"],
        )
        cronjob = [
            OCS(**job)
            for job in cronjobs_obj.get().get("items")
            if job["metadata"]["name"].endswith("status-reporter")
        ][0]
        self._switch_provider_cluster()
        return cronjob

    def _switch_provider_cluster(self):
        """
        Switch context to provider cluster.
        """
        config.switch_ctx(self.provider_context)
        log.info(f"Switched to provider cluster with index {self.provider_context}")

    def _switch_consumer_cluster(self):
        """
        Switch context to consumer cluster.
        """
        config.switch_ctx(self.consumer_context)
        log.info(f"Switched to consumer cluster with index {self.consumer_context}")
