"""
A module for all StorageConsumer functionalities and abstractions.
"""

import concurrent
import json
import logging
import tempfile
from concurrent.futures.thread import ThreadPoolExecutor

from ocs_ci.framework import config, config_safe_thread_pool_task
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.helpers.helpers import get_cephfs_subvolumegroup_names
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.managedservice import get_consumer_names
from ocs_ci.ocs.rados_utils import fetch_rados_namespaces, fetch_pool_names
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.ocs.version import if_version
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry, catch_exceptions
from ocs_ci.utility.templating import dump_data_to_temp_yaml
from ocs_ci.utility.utils import exec_cmd, TimeoutSampler

log = logging.getLogger(__name__)


class StorageConsumer:
    """
    Base StorageConsumer class
    """

    def __init__(self, consumer_name, namespace=None, consumer_context=None):
        """
        Starting from ODF 4.19 (Converged) this CR has optional Spec fields:
        StorageQuotaInGiB               int
        ResourceNameMappingConfigMap    string
        StorageClasses                  []
        VolumeSnapshotClasses           []
        VolumeGroupSnapshotClasses      []

        Starting from ODF 4.19 (Converged) this CR has optional Status fields:
        Client                          ClientStatus
        OnboardingTicketSecret          string
        LastHeartbeat                   string

        Args:
            consumer_name (string): name of the StorageConsumer resource
            consumer_context (int): index of cluster context. This is needed for
                consumer operations executed on consumer
                (e.g. manipulation of heartbeat cronjob)
        """
        self.consumer_context = consumer_context
        self.name = consumer_name
        self.namespace = namespace or config.ENV_DATA["cluster_namespace"]
        self.ocp = ocp.OCP(
            resource_name=self.name,
            kind=constants.STORAGECONSUMER,
            namespace=self.namespace,
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

    def get_uid(self):
        """
        Get the UID of the StorageConsumer resource.

        Returns:
            str: UID of the StorageConsumer resource

        """
        with config.RunWithConfigContext(self.consumer_context):
            return self.ocp.get(resource_name=self.name).get("metadata").get("uid")

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

    def get_last_heartbeat(self):
        """
        Get the last heartbeat cronjob.

        Returns:
            dict:last heartbeat timestamp

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name).get("status").get("lastHeartbeat")
            )

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
    def get_state(self):
        """
        Get state from storageconsumer resource.

        Returns:
            string: state of the storage consumer

        """
        with config.RunWithConfigContext(self.consumer_context):
            return self.ocp.get(resource_name=self.name).get("status").get("state")

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
    def get_resource_name_mapping_config_map_from_spec(self):
        """
        Get ResourceNameMappingConfigMap from storageconsumer resource.
        It is optional, reflect the configMap we used, user provided or generated
        This is a name of the configmap, resource that stores ceph rns, svg names and more

        Returns:
            string: ResourceNameMappingConfigMap

        """
        with config.RunWithConfigContext(self.consumer_context):
            resource_name_mapping_config_map = (
                self.ocp.get(resource_name=self.name)
                .get("spec")
                .get("resourceNameMappingConfigMap")
            )
        return (
            resource_name_mapping_config_map["name"]
            if resource_name_mapping_config_map
            else None
        )

    @if_version(">4.18")
    def get_resource_name_mapping_config_map_from_status(self):
        """
        Get ResourceNameMappingConfigMap from storageconsumer resource from Status.
        This is a name of the configmap, resource that stores ceph rns, svg names and more

        Returns:
            string: ResourceNameMappingConfigMap

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name)
                .get("status")
                .get("resourceNameMappingConfigMap")
            )

    @if_version(">4.18")
    def get_storage_classes(self):
        """
        Get storage classes from storageconsumer resource and apply patch.

        Returns:
            list: storage classes

        """
        with config.RunWithConfigContext(self.consumer_context):
            sc_list = (
                self.ocp.get(resource_name=self.name).get("spec").get("storageClasses")
            )
        return [sc["name"] for sc in sc_list] if sc_list else []

    @if_version(">4.18")
    def get_volume_snapshot_classes(self):
        """
        Get volume snapshot classes from storageconsumer resource.

        Returns:
            list: volume snapshot classes

        """
        with config.RunWithConfigContext(self.consumer_context):
            vsc_list = (
                self.ocp.get(resource_name=self.name)
                .get("spec")
                .get("volumeSnapshotClasses")
            )
        return [sc["name"] for sc in vsc_list] if vsc_list else []

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
    def set_storage_classes(self, storage_classes):
        """
        Add one or multiple storage classes to the storageconsumer resource and apply patch.

        Args:
            storage_classes (str or list): A single storage class as a string or a list of storage classes.

        """
        if isinstance(storage_classes, str):
            storage_classes = [storage_classes]

        if not isinstance(storage_classes, list):
            raise ValueError("storage_classes must be a string or a list of strings")

        storage_classes_list = [{"name": sc} for sc in storage_classes]
        storage_classes_json = json.dumps(storage_classes_list)
        with config.RunWithConfigContext(self.consumer_context):
            patch_param = f'{{"spec": {{"storageClasses": {storage_classes_json}}}}}'
            self.ocp.patch(
                resource_name=self.name, params=patch_param, format_type="merge"
            )

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
    def set_custom_volume_snapshot_class(self, snapshot_class):
        """
        Set volume snapshot class to storageconsumer resource and apply patch.

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
    def set_custom_volume_group_snapshot_class(self, group_snapshot_class):
        """
        Set volume group snapshot class to storageconsumer resource  and apply patch.

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

    @if_version(">4.18")
    @retry((AttributeError, KeyError), tries=10, delay=5)
    def get_onboarding_ticket_secret(self):
        """
        Get OnboardingTicketSecret from storageconsumer resource status. Optional field.
        Reference to name of an onboarding secret cr.

        Returns:
            string: OnboardingTicketSecret

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name)
                .get("status")
                .get("onboardingTicketSecret")
                .get("name")
            )

    @if_version(">4.18")
    def create_storage_consumer(
        self,
        storage_classes=None,
        volume_snapshot_classes=None,
        volume_group_snapshot_classes=None,
        storage_quota_in_gib=None,
        resource_name_mapping_config_map_name=None,
    ):
        """
        Create a storage consumer

        Args:
            storage_classes (list): List of storage classes
            volume_snapshot_classes (list): List of volume snapshot classes
            volume_group_snapshot_classes (list): List of volume group snapshot classes
            storage_quota_in_gib (int): Storage quota in GiB
            resource_name_mapping_config_map_name (str): Resource name mapping config map

        Returns:
            dict: Dictionary with consumer data

        """
        with config.RunWithConfigContext(self.consumer_context):
            storage_consumer_data = templating.load_yaml(
                constants.STORAGE_CONSUMER_YAML
            )
            storage_consumer_data["metadata"]["name"] = self.name
            storage_consumer_data["metadata"]["namespace"] = self.namespace
            if storage_classes:
                storage_consumer_data["spec"].setdefault(
                    "storageClasses", [{"name": sc} for sc in storage_classes]
                )
            if volume_snapshot_classes:
                storage_consumer_data["spec"].setdefault(
                    "volumeSnapshotClasses",
                    [{"name": vsc} for vsc in volume_snapshot_classes],
                )
            if volume_group_snapshot_classes:
                storage_consumer_data["spec"].setdefault(
                    "volumeGroupSnapshotClasses",
                    [{"name": vgsc} for vgsc in volume_group_snapshot_classes],
                )
            if storage_quota_in_gib:
                storage_consumer_data["spec"].setdefault(
                    "storageQuotaInGiB", storage_quota_in_gib
                )
            if resource_name_mapping_config_map_name:
                storage_consumer_data["spec"].setdefault(
                    "resourceNameMappingConfigMap", {}
                ).setdefault("name", resource_name_mapping_config_map_name)

            storage_consumer_file = tempfile.NamedTemporaryFile(
                mode="w+", prefix="storage_consumer", delete=False
            )
            dump_data_to_temp_yaml(storage_consumer_data, storage_consumer_file.name)

            return self.ocp.create(yaml_file=storage_consumer_file.name)

    def get_owner_references(self):
        """
        Get owner references of the storage consumer.

        Returns:
            list: List of owner references

        """
        with config.RunWithConfigContext(self.consumer_context):
            return (
                self.ocp.get(resource_name=self.name)
                .get("metadata")
                .get("ownerReferences")
            )


def create_storage_consumer_on_default_cluster(
    consumer_name,
    storage_classes=None,
    volume_snapshot_classes=None,
    volume_group_snapshot_classes=None,
    storage_quota_in_gib=None,
    resource_name_mapping_config_map_name=None,
):
    """
    Create a storage consumer on the storage provider cluster

    Args:
        consumer_name (str): Name of the storage consumer
        storage_classes (list): List of storage classes
        volume_snapshot_classes (list): List of volume snapshot classes
        volume_group_snapshot_classes (list): List of volume group snapshot classes
        storage_quota_in_gib (int): Storage quota in GiB
        resource_name_mapping_config_map_name (str): Resource name mapping config map

    Returns:
        StorageConsumer: StorageConsumer object

    """
    # as of ODF 4.19 timeline, StorageConsumer cr must exist on the provider cluster only
    consumer_context = config.get_provider_index()
    storage_consumer = StorageConsumer(
        consumer_name, config.ENV_DATA["cluster_namespace"], consumer_context
    )
    storage_consumer.create_storage_consumer(
        volume_snapshot_classes=volume_snapshot_classes,
        volume_group_snapshot_classes=volume_group_snapshot_classes,
        storage_classes=storage_classes,
        storage_quota_in_gib=storage_quota_in_gib,
        resource_name_mapping_config_map_name=resource_name_mapping_config_map_name,
    )
    return storage_consumer


@if_version(">4.18")
@catch_exceptions(Exception)
def verify_storage_consumer_resources(
    consumer_name,
    distributed_storage_classes=None,
    distributed_volume_snapshot_classes=None,
):
    """
    Function to Verify resources:
    ConfigMap of each client includes name of each cephclient of that client
    OwnerRef of each StorageConsumer is StorageCluster, matches the uid of StorageCluster
    metadata.uid of storageconsumer matches the cephclient names sufixes in postDeployment
    StorageCluster uid matches the internal StorageConsumer uid
    StorageConsumer has StorageClasses and VolumeSnapshotClasses that are available on the cluster

    Args:
        consumer_name (str): Name of the storage consumer
        distributed_storage_classes (list): List of distributed storage classes
        distributed_volume_snapshot_classes (list): List of distributed volume snapshot classes

    Raises:
        AssertionError: If any of the checks fail

    """
    internal_consumer = consumer_name == constants.INTERNAL_STORAGE_CONSUMER_NAME
    if internal_consumer and distributed_storage_classes:
        raise AssertionError(
            "Distributed storage classes arguments are not expected for internal storage consumer."
        )
    if internal_consumer and distributed_volume_snapshot_classes:
        raise AssertionError(
            "Distributed volume snapshot classes arguments are not expected for internal storage consumer."
        )

    # as of ODF 4.19 timeline, StorageConsumer cr must exist on the provider cluster only
    consumer_context = config.cluster_ctx.ENV_DATA.get(
        "default_cluster_context_index", 0
    )
    storage_consumer = StorageConsumer(
        consumer_name, config.ENV_DATA["cluster_namespace"], consumer_context
    )
    storage_consumer_uid = storage_consumer.get_uid()

    log_step(
        f"Collect initial resources and verify StorageConsumer {consumer_name} exists"
    )
    storage_cluster = StorageCluster(
        resource_name=config.cluster_ctx.ENV_DATA["storage_cluster_name"],
        namespace=config.ENV_DATA["cluster_namespace"],
    )
    sc_uid = storage_cluster.get().get("metadata", {}).get("uid")
    if not sc_uid:
        raise AssertionError(
            f"StorageCluster {config.cluster_ctx.ENV_DATA['storage_cluster_name']} has no uid."
        )

    log_step(f"Verifying StorageConsumer Owner References for {consumer_name}")
    owner_ref = storage_consumer.get_owner_references()
    if not owner_ref:
        raise AssertionError(
            f"StorageConsumer {consumer_name} has no owner references."
        )

    storage_cluster_refs = [
        ref for ref in owner_ref if ref.get("kind").lower() == constants.STORAGECLUSTER
    ]
    if not storage_cluster_refs:
        raise AssertionError(
            f"StorageConsumer {consumer_name} has no valid StorageCluster owner references."
        )

    for ref in owner_ref:
        # skip owner references which are not StorageCluster
        # fail if no StorageCluster owner reference
        if ref.get("kind").lower() != constants.STORAGECLUSTER:
            continue

        if ref.get("name") != config.cluster_ctx.ENV_DATA["storage_cluster_name"]:
            raise AssertionError(
                f"StorageConsumer {consumer_name} owner reference name is not "
                f"'{config.cluster_ctx.ENV_DATA['storage_cluster_name']}'."
            )

        if ref.get("uid") != sc_uid:
            raise AssertionError(
                f"StorageConsumer {consumer_name} owner reference uid "
                f"{ref.get('uid')} does not match StorageCluster uid {sc_uid}."
            )

    log_step(
        "Verifying StorageConsumer StorageClasses. "
        "Cluster must have available StorageClasses that are listed in StorageConsumer"
    )
    if internal_consumer:
        storage_classes_on_consumer = storage_consumer.get_storage_classes()
        storage_classes_ocp = ocp.OCP(
            kind=constants.STORAGECLASS, namespace=config.ENV_DATA["cluster_namespace"]
        ).get()
        storage_class_names_on_cluster = [
            item["metadata"]["name"] for item in storage_classes_ocp["items"]
        ]
        for storage_class in storage_classes_on_consumer:
            if storage_class not in storage_class_names_on_cluster:
                raise AssertionError(
                    f"StorageClass {storage_class} is not available on the cluster "
                    f"but listed in storage consumer {consumer_name}."
                )
    else:
        if distributed_storage_classes:
            storage_classes_on_consumer = storage_consumer.get_storage_classes()
            for storage_class in distributed_storage_classes:
                if storage_class not in storage_classes_on_consumer:
                    raise AssertionError(
                        f"StorageClass {storage_class} is not listed in the StorageConsumer {consumer_name}."
                    )
        else:
            log.info(
                f"StorageConsumer {consumer_name} has no StorageClasses provided with function call,"
                "skipping verification of StorageClasses on cluster."
            )

    log_step(
        "Verifying StorageConsumer VolumeSnapshotClasses. "
        "Cluster must have available VolumeSnapshotClasses that are listed in StorageConsumer"
    )

    if internal_consumer:
        volume_snapshot_classes_on_consumer = (
            storage_consumer.get_volume_snapshot_classes()
        )
        volume_snapshot_classes_ocp = ocp.OCP(
            kind=constants.VOLUMESNAPSHOTCLASS,
            namespace=config.ENV_DATA["cluster_namespace"],
        ).get()
        volume_snapshot_class_names_on_cluster = [
            item["metadata"]["name"] for item in volume_snapshot_classes_ocp["items"]
        ]
        for volume_snapshot_class in volume_snapshot_classes_on_consumer or []:
            if volume_snapshot_class not in volume_snapshot_class_names_on_cluster:
                raise AssertionError(
                    f"VolumeSnapshotClass {volume_snapshot_class} is not available on the cluster "
                    f"but listed in storage consumer {consumer_name}."
                )
    else:
        if distributed_volume_snapshot_classes:
            volume_snapshot_classes_on_consumer = (
                storage_consumer.get_volume_snapshot_classes()
            )
            for volume_snapshot_class in distributed_volume_snapshot_classes:
                if volume_snapshot_class not in volume_snapshot_classes_on_consumer:
                    raise AssertionError(
                        f"VolumeSnapshotClass {volume_snapshot_class} "
                        f"is not listed in the StorageConsumer {consumer_name}."
                    )
        else:
            log.info(
                f"StorageConsumer {consumer_name} has no VolumeSnapshotClasses provided with function call, "
                "skipping verification of VolumeSnapshotClasses on cluster."
            )

    log_step("Verifying StorageConsumer ResourceNameMappingConfigMap")
    resource_name_mapping_config_map = (
        storage_consumer.get_resource_name_mapping_config_map_from_spec()
    )
    if not resource_name_mapping_config_map:
        raise AssertionError(
            f"StorageConsumer {consumer_name} has no ResourceNameMappingConfigMap."
        )
    config_map_obj = ocp.OCP(
        kind=constants.CONFIGMAP,
        namespace=config.cluster_ctx.ENV_DATA["cluster_namespace"],
        resource_name=resource_name_mapping_config_map,
    ).get()
    ceph_data = config_map_obj.get("data", {})

    ceph_data_on_consumer_match = {}
    disable_blockpools = config.COMPONENTS["disable_blockpools"]
    disable_cephfs = config.COMPONENTS["disable_cephfs"]
    if internal_consumer and not (disable_blockpools or disable_cephfs):
        """
        check by example:
        cephfs-subvolumegroup-rados-ns: csi
        csi-cephfs-node-ceph-user: cephfs-node-01780250-8de8-4b88-a5e8-fbbd05110986
        csi-cephfs-provisioner-ceph-user: cephfs-provisioner-01780250-8de8-4b88-a5e8-fbbd05110986
        csi-rbd-node-ceph-user: rbd-node-01780250-8de8-4b88-a5e8-fbbd05110986
        csi-rbd-provisioner-ceph-user: rbd-provisioner-01780250-8de8-4b88-a5e8-fbbd05110986
        csiop-cephfs-client-profile: openshift-storage
        csiop-rbd-client-profile: openshift-storage
        rbd-rados-ns: <implicit>

        """
        # basic check with static names
        ceph_data_on_consumer_match["cephfs-subvolumegroup"] = (
            ceph_data.get("cephfs-subvolumegroup", "") == "csi"
        )
        ceph_data_on_consumer_match["cephfs-subvolumegroup-rados-ns"] = (
            ceph_data.get("cephfs-subvolumegroup-rados-ns", "") == "csi"
        )
        ceph_data_on_consumer_match["rbd-rados-ns"] = (
            ceph_data.get("rbd-rados-ns", "") == "<implicit>"
        )

        # other client names are dynamic, so we check if they start with the expected suffix
        ceph_data_on_consumer_match["csi-cephfs-node-ceph-user"] = (
            ceph_data.get("csi-cephfs-node-ceph-user", "")
            == f"cephfs-node-{storage_consumer_uid}"
        )
        ceph_data_on_consumer_match["csi-cephfs-provisioner-ceph-user"] = (
            ceph_data.get("csi-cephfs-provisioner-ceph-user", "")
            == f"cephfs-provisioner-{storage_consumer_uid}"
        )
        ceph_data_on_consumer_match["csi-rbd-node-ceph-user"] = (
            ceph_data.get("csi-rbd-node-ceph-user", "")
            == f"rbd-node-{storage_consumer_uid}"
        )
        ceph_data_on_consumer_match["csi-rbd-provisioner-ceph-user"] = (
            ceph_data.get("csi-rbd-provisioner-ceph-user", "")
            == f"rbd-provisioner-{storage_consumer_uid}"
        )
        ceph_data_on_consumer_match["csiop-cephfs-client-profile"] = (
            ceph_data.get("csiop-cephfs-client-profile", "")
            == config.cluster_ctx.ENV_DATA["cluster_namespace"]
        )
        ceph_data_on_consumer_match["csiop-rbd-client-profile"] = (
            ceph_data.get("csiop-rbd-client-profile", "")
            == config.cluster_ctx.ENV_DATA["cluster_namespace"]
        )
    else:
        """
        check by example:
        cephfs-subvolumegroup: consumer-cl-418-c
        cephfs-subvolumegroup-rados-ns: consumer-cl-418-c
        csi-cephfs-node-ceph-user: cephfs-node-ffb707b4-855e-4e70-a7df-910e56a7b56c
        csi-cephfs-provisioner-ceph-user: cephfs-provisioner-ffb707b4-855e-4e70-a7df-910e56a7b56c
        csi-rbd-node-ceph-user: rbd-node-ffb707b4-855e-4e70-a7df-910e56a7b56c
        csi-rbd-provisioner-ceph-user: rbd-provisioner-ffb707b4-855e-4e70-a7df-910e56a7b56c
        csiop-cephfs-client-profile: ffb707b4-855e-4e70-a7df-910e56a7b56c
        csiop-rbd-client-profile: ffb707b4-855e-4e70-a7df-910e56a7b56c
        rbd-rados-ns: consumer-cl-418-c
        """
        # basic check with static names
        ceph_data_on_consumer_match["cephfs-subvolumegroup"] = (
            ceph_data.get("cephfs-subvolumegroup", "") == f"{consumer_name}"
        )
        ceph_data_on_consumer_match["cephfs-subvolumegroup-rados-ns"] = (
            ceph_data.get("cephfs-subvolumegroup-rados-ns", "") == f"{consumer_name}"
        )
        ceph_data_on_consumer_match["rbd-rados-ns"] = (
            ceph_data.get("rbd-rados-ns", "") == f"{consumer_name}"
        )

        # other client names are dynamic, so we check if they start with the expected suffix
        ceph_data_on_consumer_match["csi-cephfs-node-ceph-user"] = (
            ceph_data.get("csi-cephfs-node-ceph-user", "")
            == f"cephfs-node-{storage_consumer_uid}"
        )
        ceph_data_on_consumer_match["csi-cephfs-provisioner-ceph-user"] = (
            ceph_data.get("csi-cephfs-provisioner-ceph-user", "")
            == f"cephfs-provisioner-{storage_consumer_uid}"
        )
        ceph_data_on_consumer_match["csi-rbd-node-ceph-user"] = (
            ceph_data.get("csi-rbd-node-ceph-user", "")
            == f"rbd-node-{storage_consumer_uid}"
        )
        ceph_data_on_consumer_match["csi-rbd-provisioner-ceph-user"] = (
            ceph_data.get("csi-rbd-provisioner-ceph-user", "")
            == f"rbd-provisioner-{storage_consumer_uid}"
        )
        ceph_data_on_consumer_match["csiop-cephfs-client-profile"] = (
            ceph_data.get("csiop-cephfs-client-profile", "") == storage_consumer_uid
        )
        ceph_data_on_consumer_match["csiop-rbd-client-profile"] = (
            ceph_data.get("csiop-rbd-client-profile", "") == storage_consumer_uid
        )

        log.info(
            f"StorageConsumer config map data match:\n "
            f"{json.dumps(ceph_data_on_consumer_match, indent=2)}"
        )
    assert all(
        ceph_data_on_consumer_match.values()
    ), "StorageConsumer config map data does not match expected values."


def get_ready_storage_consumers():
    """
    Get a list of StorageConsumer objects that are in READY state.

    Returns:
        list[StorageConsumer]: List of StorageConsumer objects in READY state.

    """
    if config.ENV_DATA.get("cluster_type") == "provider":
        cluster_index = config.get_provider_index()
    else:
        cluster_index = config.default_cluster_index

    consumer_names = get_consumer_names()
    ready_consumers = []

    for consumer_name in consumer_names:
        sc = StorageConsumer(
            consumer_name,
            config.ENV_DATA["cluster_namespace"],
            cluster_index,
        )
        if sc.get_state() == constants.STATUS_READY:
            ready_consumers.append(sc)
        else:
            log.warning(f"StorageConsumer {consumer_name} is not in READY state")

    return ready_consumers


def get_ready_consumers_names():
    """
    Get the names of all storage consumers that are in READY state.

    Returns:
        list: List of names of storage consumers in READY state.
    """
    ready_consumers = get_ready_storage_consumers()
    return [consumer.name for consumer in ready_consumers]


def check_consumer_rns(consumer_name, pool_list, rns_list):
    """
    Verify that the Rados namespaces on the consumer match the expected ones.
    Each pool must have one RNS for each Storage Consumer.

    Args:
       consumer_name (str): Name of the storage consumer
       pool_list (list): List of pool names
       rns_list (list): List of Rados namespaces

    Returns:
       bool: True if RNS found for each consumer over all pools (excluding exception list), False otherwise.

    """
    log.info(f"Verifying Rados namespaces for consumer {consumer_name}")
    excluded_pools = {"builtin-mgr", "ocs-storagecluster-cephnfs-builtin-pool"}
    consumer_rns_valid = {}

    for pool in pool_list:
        if pool in excluded_pools:
            continue
        expected_rns_name = (
            f"{pool}-builtin-implicit"
            if consumer_name == "internal"
            else f"{pool}-{consumer_name}"
        )

        if expected_rns_name in rns_list:
            consumer_rns_valid[f"{consumer_name}/{pool}"] = True
        else:
            log.warning(f"No RNS found for pool {pool} and consumer {consumer_name}")
            consumer_rns_valid[f"{consumer_name}/{pool}"] = False

    log.info(f"Consumer RNS: {consumer_rns_valid}")
    return all(consumer_rns_valid.values())


@if_version(">4.18")
def check_consumers_rns():
    """
    Verify that the Rados namespaces on the consumer match the expected ones.
    Function is for all clusters that host ceph and are post-convergence.

    Returns:
        bool: True if RNS found for each consumer over all pools, False otherwise.

    """
    # we can not use 'current' cluster index, because it is more likely not a provider cluster than a 'default'
    if config.ENV_DATA.get("cluster_type") == "provider":
        cluster_index = config.get_provider_index()
    else:
        cluster_index = config.default_cluster_index

    with config.RunWithConfigContext(cluster_index):
        log.info(
            f"Running RNS verification for consumers on cluster {config.cluster_ctx.ENV_DATA['cluster_name']}"
        )
        consumer_names = get_ready_consumers_names()
        pool_names = fetch_pool_names()
        rados_namespaces = fetch_rados_namespaces(config.ENV_DATA["cluster_namespace"])

        for consumer_name in consumer_names:
            log.info(f"Verifying Rados namespaces for consumer {consumer_name}")
            if not check_consumer_rns(consumer_name, pool_names, rados_namespaces):
                return False
        log.info("All Rados namespaces verified successfully.")
        return True


def check_consumer_svg(consumer_name, volume_list, svg_list):
    """
    Verify that the subvolumegroup on the consumer matches the expected one.

    Args:
        consumer_name (str): Name of the storage consumer
        volume_list (list): List of volume names
        svg_list (list): List of subvolumegroup names

    Returns:
        bool: True if subvolumegroup found for each consumer, False otherwise.

    """
    log.info(f"Verifying subvolumegroup for consumer {consumer_name}")
    consumer_svg_valid = {}
    for volume in volume_list:
        expected_svg_name = consumer_name if consumer_name != "internal" else "csi"

        if expected_svg_name in svg_list:
            consumer_svg_valid[f"{consumer_name}/{volume}"] = True
        else:
            log.warning(
                f"No subvolumegroup found for volume {volume} and consumer {consumer_name}"
            )
            consumer_svg_valid[f"{consumer_name}/{volume}"] = False

    log.info(f"Consumer subvolumegroup: {consumer_svg_valid}")
    return all(consumer_svg_valid.values())


@if_version(">4.18")
def check_consumers_svg():
    """
    Verify that the subvolumegroup on the consumer matches the expected one.
    Function is for all clusters that host ceph and are post-convergence.
    Although only one volume/filesystem is currently supported, this function is designed to check
    all volumes have svg dedicated for consumer.

    Returns:
        bool: True if subvolumegroup found for each consumer, False otherwise.

    """
    # we can not use 'current' cluster index, because it is more likely not a provider cluster than a 'default'
    if config.ENV_DATA.get("cluster_type") == "provider":
        cluster_index = config.get_provider_index()
    else:
        cluster_index = config.default_cluster_index

    with config.RunWithConfigContext(cluster_index):
        svg_names = get_cephfs_subvolumegroup_names()
        filesystems = ocp.OCP(
            kind=constants.CEPHFILESYSTEM,
            namespace=config.ENV_DATA["cluster_namespace"],
        ).get()
        consumer_names = get_ready_consumers_names()
        volume_names = [fs["metadata"]["name"] for fs in filesystems.get("items", [])]

        for consumer_name in consumer_names:
            log.info(f"Verifying subvolumegroup for consumer {consumer_name}")

            if not check_consumer_svg(consumer_name, volume_names, svg_names):
                log.error(
                    f"Subvolumegroup verification failed for consumer {consumer_name}."
                )
                return False
            else:
                log.info(
                    f"Subvolumegroup verified successfully for consumer {consumer_name}"
                )

        log.info("All subvolumegroup verified successfully.")
        return True


def check_storage_classes_on_clients(ready_consumer_names: list[str]):
    """
    Verify that the storage classes are distributed and available in the inventory of a hosted cluster.

    Returns:
        bool: True if the storage classes are distributed and available, False otherwise.

    """
    log.info(
        "Verify Storage Classes are distributed and available in inventory of a hosted cluster"
    )
    from ocs_ci.deployment.hosted_cluster import get_autodistributed_storage_classes

    with config.RunWithProviderConfigContextIfAvailable():

        storage_classes_on_provider = get_autodistributed_storage_classes()
        if not storage_classes_on_provider:
            log.error(
                "No storage classes found on the provider. Likely misconfiguration or ODF is not set up."
            )
            return False

    def wait_storage_classes_equal(given_classes):
        """
        This function will fetch StorageClasses with current multicluster config

        Args:
            given_classes (list): List of expected storage classes to match against the provider's storage classes.

        Returns:
            bool: True if the storage classes match, False otherwise.

        """
        sample = TimeoutSampler(
            timeout=300,
            sleep=10,
            func=lambda: set(given_classes)
            == set(get_autodistributed_storage_classes()),
        )
        if not sample.wait_for_func_status(result=True):
            res = False
            log.error(
                "The storage classes on the provider do not match the "
                f"expected storage classes on {config.ENV_DATA['cluster_name']}."
            )
        else:
            res = True
            log.info(
                "The storage classes on the provider match the "
                f"expected storage classes on {config.ENV_DATA['cluster_name']}."
            )
        return res

    with ThreadPoolExecutor() as executor:
        futures = []
        for multicluster_config_index in config.get_consumer_indexes_list():
            log.info(
                f"Checking multicluster config index '{multicluster_config_index}', if Consumer is Ready. "
                "Skip verifying distribution of consumer which is intentionally or not NotReady"
            )
            cluster_name = config.get_cluster_name_by_index(multicluster_config_index)
            # consumer names are built like '<consumer_name>-<cluster_name>'
            if any([rcn for rcn in ready_consumer_names if rcn.endswith(cluster_name)]):
                log.info(
                    "Submitting task to verify storage classes with "
                    f"client {config.get_cluster_name_by_index(multicluster_config_index)}"
                )
                futures.append(
                    executor.submit(
                        config_safe_thread_pool_task,
                        multicluster_config_index,
                        wait_storage_classes_equal,
                        storage_classes_on_provider,
                    )
                )
        results = []
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                log.info(f"Future result: {result}")
                results.append(result)
            except Exception as e:
                log.error(f"Error in future execution: {e}")

        log.info(f"Results of storage classes verification across consumers: {results}")
        return all(results)
