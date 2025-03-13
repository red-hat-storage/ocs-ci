"""
A module for all StorageConsumer functionalities and abstractions.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.version import if_version
from ocs_ci.utility.utils import exec_cmd

log = logging.getLogger(__name__)


class StorageConsumer:
    """
    Base StorageConsumer class
    """

    def __init__(self, consumer_name, consumer_context=None):
        """
        Starting from ODF 4.19 (Converged) this CR has optional Spec fields:
        StorageQuotaInGiB          int
        StorageClasses             []
        VolumeSnapshotClasses      []
        VolumeGroupSnapshotClasses []

        Starting from ODF 4.19 (Converged) this CR has optional Status fields:
        Client                ClientStatus
        RadosNamespace        RadosNamespaceStatus
        CephFsSubVolumeGroup  SubVolumeGroupStatus
        CephCsiClientProfiles [] ClientProfileStatus

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

    @if_version(">4.18")
    def get_client_status(self):
        """
        Get client status from storageconsumer resource and apply patch.

        Returns:
            dict: client status

        """
        with config.RunWithConfigContext(self.consumer_context):
            return self.ocp.get(resource_name=self.name).get("status").get("client")

    @if_version(">4.18")
    def get_rados_namespace_status(self):
        """
        Get rados namespace status from storageconsumer resource and apply patch.

        Returns:
            dict: rados namespace status

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name)
                .get("status")
                .get("radosNamespace")
            )

    @if_version(">4.18")
    def get_cephfs_subvolume_group_status(self):
        """
        Get cephfs subvolume group status from storageconsumer resource and apply patch.

        Returns:
            dict: cephfs subvolume group status

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name)
                .get("status")
                .get("cephFsSubVolumeGroup")
            )

    @if_version(">4.18")
    def get_ceph_csi_client_profiles(self):
        """
        Get ceph csi client profiles from storageconsumer resource and apply patch.

        Returns:
            dict: ceph csi client profiles

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name)
                .get("status")
                .get("cephCsiClientProfiles")
            )

    def get_storage_quota_in_gib(self):
        """
        Get storage quota in GiB from storageconsumer resource.

        Returns:
            int: storage quota in GiB

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name)
                .get("spec")
                .get("storageQuotaInGiB")
            )

    @if_version(">4.18")
    def get_storage_classes(self):
        """
        Get storage classes from storageconsumer resource and apply patch.

        Returns:
            list: storage classes

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name).get("spec").get("storageClasses")
            )

    def set_storage_quota_in_gib(self, quota):
        """
        Update storage quota in GiB in storageconsumer resource and apply patch.

        Args:
            quota (int): storage quota in GiB

        """
        with config.RunWithConfigContext(self.consumer_context):
            patch_param = f'{{"spec": {{"storageQuotaInGiB": {quota}}}}}'
            self.ocp.patch(resource_name=self.name, params=patch_param)

    @if_version(">4.18")
    def add_custom_storage_class(self, storage_class):
        """
        Add storage class to storageconsumer resource and apply patch.

        Args:
            storage_class (string): storage class

        """
        with config.RunWithConfigContext(self.consumer_context):
            patch_param = f'{{"spec": {{"storageClasses": ["{storage_class}"]}}}}'
            self.ocp.patch(resource_name=self.name, params=patch_param)

    @if_version(">4.18")
    def remove_custom_storage_class(self, storage_class):
        """
        Remove storage class from storageconsumer resource and apply patch.

        Args:
            storage_class (string): storage class

        """
        with config.RunWithConfigContext(self.consumer_context):
            current_storage_classes = (
                self.ocp.get(resource_name=self.name).get("spec").get("storageClasses")
            )
            if storage_class in current_storage_classes:
                current_storage_classes.remove(storage_class)
                patch_param = (
                    f'{{"spec": {{"storageClasses": {current_storage_classes}}}}}'
                )
                self.ocp.patch(resource_name=self.name, params=patch_param)

    @if_version(">4.18")
    def add_custom_volume_snapshot_class(self, snapshot_class):
        """
        Add volume snapshot class to storageconsumer resource and apply patch.

        Args:
            snapshot_class (string): volume snapshot class

        """
        with config.RunWithConfigContext(self.consumer_context):
            patch_param = (
                f'{{"spec": {{"volumeSnapshotClasses": ["{snapshot_class}"]}}}}'
            )
            self.ocp.patch(resource_name=self.name, params=patch_param)

    @if_version(">4.18")
    def remove_custom_volume_snapshot_class(self, snapshot_class):
        """
        Remove volume snapshot class from storageconsumer resource and apply patch.

        Args:
            snapshot_class (string): volume snapshot class

        """
        with config.RunWithConfigContext(self.consumer_context):
            current_snapshot_classes = (
                self.ocp.get(resource_name=self.name)
                .get("spec")
                .get("volumeSnapshotClasses")
            )
            if snapshot_class in current_snapshot_classes:
                current_snapshot_classes.remove(snapshot_class)
                patch_param = f'{{"spec": {{"volumeSnapshotClasses": {current_snapshot_classes}}}}}'
                self.ocp.patch(resource_name=self.name, params=patch_param)

    @if_version(">4.18")
    def add_custom_volume_group_snapshot_class(self, group_snapshot_class):
        """
        Add volume group snapshot class to storageconsumer resource  and apply patch.

        Args:
            group_snapshot_class (string): volume group snapshot class

        """
        with config.RunWithConfigContext(self.consumer_context):
            patch_param = f'{{"spec": {{"volumeGroupSnapshotClasses": ["{group_snapshot_class}"]}}}}'
            self.ocp.patch(resource_name=self.name, params=patch_param)

    @if_version(">4.18")
    def remove_custom_volume_group_snapshot_class(self, group_snapshot_class):
        """
        Remove volume group snapshot class from storageconsumer resource and apply patch.

        Args:
            group_snapshot_class (string): volume group snapshot class

        """
        with config.RunWithConfigContext(self.consumer_context):
            current_group_snapshot_classes = (
                self.ocp.get(resource_name=self.name)
                .get("spec")
                .get("volumeGroupSnapshotClasses")
            )
            if group_snapshot_class in current_group_snapshot_classes:
                current_group_snapshot_classes.remove(group_snapshot_class)
                patch_param = f'{{"spec": {{"volumeGroupSnapshotClasses": {current_group_snapshot_classes}}}}}'
                self.ocp.patch(resource_name=self.name, params=patch_param)
