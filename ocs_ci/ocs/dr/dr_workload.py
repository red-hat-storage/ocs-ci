"""
This module will have all DR related workload classes

"""

import logging
import os
import tempfile
import yaml

from subprocess import TimeoutExpired
from time import sleep

from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import create_vm_secret, cal_md5sum_vm
from ocs_ci.helpers.helpers import (
    delete_volume_in_backend,
    verify_volume_deleted_in_backend,
    create_project,
    create_unique_resource_name,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.cnv.virtual_machine import VirtualMachine
from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    CommandFailed,
    UnexpectedBehaviour,
    ResourceNotDeleted,
)
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.ocs.utils import get_primary_cluster_config, get_non_acm_cluster_config
from ocs_ci.utility import templating
from ocs_ci.utility.utils import clone_repo, run_cmd

log = logging.getLogger(__name__)


class DRWorkload(object):
    """
    Base class for all DR workload classes

    """

    def __init__(
        self, workload_name=None, workload_repo_url=None, workload_repo_branch=None
    ):
        self.workload_name = workload_name
        self.workload_repo_url = workload_repo_url
        self.workload_repo_branch = workload_repo_branch

    def deploy_workload(self):
        raise NotImplementedError("Method not implemented")

    def verify_workload_deployment(self):
        raise NotImplementedError("Method not implemented")

    def delete_workload(self, force=False):
        raise NotImplementedError("Method not implemented")

    @staticmethod
    def resources_cleanup(namespace, image_uuids=None):
        """
        Cleanup workload and replication resources in a given namespace from managed clusters.
        Useful for removing leftover resources to avoid further test failures.

        Args:
            namespace (str): The namespace of the workload
            image_uuids (list): List of image UUIDs associated with the PVCs

        """
        resources = [
            constants.DEPLOYMENT,
            constants.POD,
            constants.VOLUME_REPLICATION_GROUP,
            constants.VOLUME_REPLICATION,
            constants.PVC,
            constants.PV,
        ]
        log.info("Cleaning up leftover resources in managed clusters")
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            for resource in resources:
                sleep(10)  # Wait 10 seconds before getting the list of items
                log.info(f"Cleaning up {resource} resources")
                ocp_obj = ocp.OCP(kind=resource, namespace=namespace)
                item_list = ocp_obj.get()["items"]
                for item in item_list:
                    resource_name = item["metadata"]["name"]
                    try:
                        if resource == constants.PV:
                            if item["spec"]["claimRef"]["namespace"] != namespace:
                                continue
                            ocp_obj.delete(resource_name=resource_name)
                            ocp_obj.wait_for_delete(resource_name=resource_name)
                        else:
                            ocp_obj.delete(resource_name=resource_name, wait=False)
                            ocp_obj.patch(
                                resource_name=resource_name,
                                params='{"metadata": {"finalizers":null}}',
                                format_type="merge",
                            )
                            ocp_obj.wait_for_delete(resource_name=resource_name)

                    except CommandFailed as ex:
                        if "NotFound" in str(ex):
                            log.info(
                                f"{resource} {resource_name} got deleted successfully"
                            )
                        else:
                            raise ex

        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            for image_uuid in image_uuids:
                delete_volume_in_backend(
                    img_uuid=image_uuid,
                    pool_name=constants.DEFAULT_CEPHBLOCKPOOL,
                    disable_mirroring=True,
                )


class BusyBox(DRWorkload):
    """
    Class handling everything related to busybox workload

    """

    def __init__(self, **kwargs):
        workload_repo_url = config.ENV_DATA["dr_workload_repo_url"]
        log.info(f"Repo used: {workload_repo_url}")
        workload_repo_branch = config.ENV_DATA["dr_workload_repo_branch"]
        super().__init__("busybox", workload_repo_url, workload_repo_branch)

        self.workload_type = kwargs.get("workload_type", constants.SUBSCRIPTION)
        self.workload_namespace = kwargs.get("workload_namespace", None)
        self.workload_pod_count = kwargs.get("workload_pod_count")
        self.workload_pvc_count = kwargs.get("workload_pvc_count")
        self.dr_policy_name = kwargs.get(
            "dr_policy_name", config.ENV_DATA.get("dr_policy_name")
        ) or (dr_helpers.get_all_drpolicy()[0]["metadata"]["name"])
        self.preferred_primary_cluster = kwargs.get("preferred_primary_cluster") or (
            get_primary_cluster_config().ENV_DATA["cluster_name"]
        )
        self.target_clone_dir = config.ENV_DATA.get(
            "target_clone_dir", constants.DR_WORKLOAD_REPO_BASE_DIR
        )
        self.workload_subscription_dir = os.path.join(
            self.target_clone_dir, kwargs.get("workload_dir"), "subscriptions"
        )
        self.drpc_yaml_file = os.path.join(
            self.workload_subscription_dir, self.workload_name, "drpc.yaml"
        )
        self.channel_yaml_file = os.path.join(
            self.workload_subscription_dir, "channel.yaml"
        )

    def deploy_workload(self):
        """
        Deployment specific to busybox workload

        """
        self._deploy_prereqs()
        self.workload_namespace = self._get_workload_namespace()

        # load drpc.yaml
        drpc_yaml_data = templating.load_yaml(self.drpc_yaml_file)
        drpc_yaml_data["spec"]["preferredCluster"] = self.preferred_primary_cluster
        drpc_yaml_data["spec"]["drPolicyRef"]["name"] = self.dr_policy_name
        templating.dump_data_to_temp_yaml(drpc_yaml_data, self.drpc_yaml_file)

        # TODO
        # drpc_yaml_file needs to be committed back to the repo
        # because ACM would refetch from repo directly

        # load channel.yaml
        channel_yaml_data = templating.load_yaml(self.channel_yaml_file)
        channel_yaml_data["spec"]["pathname"] = self.workload_repo_url
        templating.dump_data_to_temp_yaml(channel_yaml_data, self.channel_yaml_file)

        # Create the resources on Hub cluster
        config.switch_acm_ctx()
        run_cmd(f"oc create -k {self.workload_subscription_dir}")
        run_cmd(f"oc create -k {self.workload_subscription_dir}/{self.workload_name}")

        self.verify_workload_deployment()

    def _deploy_prereqs(self):
        """
        Perform prerequisites

        """
        # Clone workload repo
        clone_repo(
            url=self.workload_repo_url,
            location=self.target_clone_dir,
            branch=self.workload_repo_branch,
        )

    def _get_workload_namespace(self):
        """
        Get the workload namespace

        """
        namespace_yaml_file = os.path.join(
            os.path.join(self.workload_subscription_dir, self.workload_name),
            "namespace.yaml",
        )
        namespace_yaml_data = templating.load_yaml(namespace_yaml_file)
        return namespace_yaml_data["metadata"]["name"]

    def verify_workload_deployment(self):
        """
        Verify busybox workload

        """
        config.switch_to_cluster_by_name(self.preferred_primary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            self.workload_pvc_count, self.workload_pod_count, self.workload_namespace
        )

    def delete_workload(self, force=False, rbd_name="rbd", switch_ctx=None):
        """
        Delete busybox workload

        Args:
            force (bool): If True, force remove the stuck resources, default False
            rbd_name (str): Name of the pool
            switch_ctx (int): The cluster index by the cluster name

        Raises:
            ResourceNotDeleted: In case workload resources not deleted properly

        """
        image_uuids = dr_helpers.get_image_uuids(self.workload_namespace)

        # Skipping drpc.yaml deletion since DRPC is automatically removed.
        kustomization_yaml_file = os.path.join(
            self.workload_subscription_dir, self.workload_name, "kustomization.yaml"
        )
        kustomization_yaml_data = templating.load_yaml(kustomization_yaml_file)
        kustomization_yaml_data["resources"].remove("drpc.yaml")
        templating.dump_data_to_temp_yaml(
            kustomization_yaml_data, kustomization_yaml_file
        )

        try:
            config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
            run_cmd(
                f"oc delete -k {self.workload_subscription_dir}/{self.workload_name}"
            )

            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                dr_helpers.wait_for_all_resources_deletion(
                    namespace=self.workload_namespace,
                    check_replication_resources_state=False,
                )

            log.info("Verify backend images or subvolumes are deleted")
            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                rbd_pool_name = (
                    (config.ENV_DATA.get("rbd_name") or rbd_name)
                    if config.DEPLOYMENT["external_mode"]
                    else constants.DEFAULT_CEPHBLOCKPOOL
                )
                for image_uuid in image_uuids:
                    # TODO: Add a better condition to identify CephFS or RBD
                    if "cephfs" in self.workload_namespace:
                        status = verify_volume_deleted_in_backend(
                            interface=constants.CEPHFILESYSTEM, image_uuid=image_uuid
                        )
                    else:
                        status = verify_volume_deleted_in_backend(
                            interface=constants.CEPHBLOCKPOOL,
                            image_uuid=image_uuid,
                            pool_name=rbd_pool_name,
                        )
                    if not status:
                        raise UnexpectedBehaviour(
                            "Images/subvolumes still exists on backend"
                        )

        except (
            TimeoutExpired,
            TimeoutExpiredError,
            TimeoutError,
            UnexpectedBehaviour,
        ) as ex:
            err_msg = f"Failed to delete the workload: {ex}"
            log.exception(err_msg)
            if force:
                self.resources_cleanup(self.workload_namespace, image_uuids)
            raise ResourceNotDeleted(err_msg)

        finally:
            config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
            run_cmd(f"oc delete -k {self.workload_subscription_dir}")


class BusyBox_AppSet(DRWorkload):
    """
    Class handling everything related to busybox workload Appset

    """

    def __init__(self, **kwargs):
        workload_repo_url = config.ENV_DATA["dr_workload_repo_url"]
        workload_repo_branch = config.ENV_DATA["dr_workload_repo_branch"]
        super().__init__("busybox", workload_repo_url, workload_repo_branch)

        self.workload_type = kwargs.get("workload_type", constants.APPLICATION_SET)
        self.workload_namespace = kwargs.get("workload_namespace", None)
        self.workload_pod_count = kwargs.get("workload_pod_count")
        self.workload_pvc_count = kwargs.get("workload_pvc_count")
        self.dr_policy_name = kwargs.get(
            "dr_policy_name", config.ENV_DATA.get("dr_policy_name")
        ) or (dr_helpers.get_all_drpolicy()[0]["metadata"]["name"])
        self.preferred_primary_cluster = kwargs.get("preferred_primary_cluster") or (
            get_primary_cluster_config().ENV_DATA["cluster_name"]
        )
        self.preferred_primary_cluster = config.ENV_DATA.get(
            "preferred_primary_cluster"
        ) or (get_primary_cluster_config().ENV_DATA["cluster_name"])
        self.target_clone_dir = config.ENV_DATA.get(
            "target_clone_dir", constants.DR_WORKLOAD_REPO_BASE_DIR
        )
        self.workload_appset_dir = os.path.join(
            self.target_clone_dir, kwargs.get("workload_dir")
        )
        self.appset_yaml_file = os.path.join(
            self.workload_appset_dir,
        )
        self.drpc_yaml_file = os.path.join(constants.DRPC_PATH)
        self.appset_placement_name = kwargs.get("workload_placement_name")
        self.appset_pvc_selector = kwargs.get("workload_pvc_selector")
        self.appset_model = kwargs.get("appset_model")

    def deploy_workload(self):
        """
        Deployment specific to busybox workload

        """
        self._deploy_prereqs()
        self.workload_namespace = self._get_workload_namespace()
        # load drpc.yaml
        drpc_yaml_data = templating.load_yaml(self.drpc_yaml_file)
        drpc_yaml_data["metadata"]["name"] = f"{self.appset_placement_name}-drpc"
        drpc_yaml_data["spec"]["preferredCluster"] = self.preferred_primary_cluster
        drpc_yaml_data["spec"]["drPolicyRef"]["name"] = self.dr_policy_name
        drpc_yaml_data["spec"]["placementRef"]["name"] = self.appset_placement_name
        drpc_yaml_data["spec"]["pvcSelector"]["matchLabels"] = self.appset_pvc_selector
        drcp_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="drpc", delete=False
        )
        templating.dump_data_to_temp_yaml(drpc_yaml_data, drcp_data_yaml.name)

        app_set_yaml_data_list = list(
            templating.load_yaml(self.appset_yaml_file, multi_document=True)
        )
        for app_set_yaml_data in app_set_yaml_data_list:
            if app_set_yaml_data["kind"] == constants.PLACEMENT:
                app_set_yaml_data["spec"]["predicates"][0]["requiredClusterSelector"][
                    "labelSelector"
                ]["matchExpressions"][0]["values"][0] = self.preferred_primary_cluster
            elif app_set_yaml_data["kind"] == constants.APPLICATION_SET:
                if self.appset_model == "pull":
                    # load appset_yaml_file, add "annotations" key and add values to it
                    app_set_yaml_data["spec"]["template"]["metadata"].setdefault(
                        "annotations", {}
                    )
                    app_set_yaml_data["spec"]["template"]["metadata"]["annotations"][
                        "apps.open-cluster-management.io/ocm-managed-cluster"
                    ] = "{{name}}"
                    app_set_yaml_data["spec"]["template"]["metadata"]["annotations"][
                        "argocd.argoproj.io/skip-reconcile"
                    ] = "true"

                    # Assign values to the "labels" key
                    app_set_yaml_data["spec"]["template"]["metadata"]["labels"][
                        "apps.open-cluster-management.io/pull-to-ocm-managed-cluster"
                    ] = "true"

        log.info(yaml.dump(app_set_yaml_data_list))
        templating.dump_data_to_temp_yaml(app_set_yaml_data_list, self.appset_yaml_file)
        config.switch_acm_ctx()
        run_cmd(f"oc create -f {self.appset_yaml_file}")
        self.check_pod_pvc_status(skip_replication_resources=True)
        self.add_annotation_to_placement()
        run_cmd(f"oc create -f {drcp_data_yaml.name}")
        self.verify_workload_deployment()

    def _deploy_prereqs(self):
        """
        Perform prerequisites

        """
        # Clone workload repo
        clone_repo(
            url=self.workload_repo_url,
            location=self.target_clone_dir,
            branch=self.workload_repo_branch,
        )

    def add_annotation_to_placement(self):
        """
        Add Annotation to Placement

        """

        config.switch_acm_ctx()
        placcement_obj = ocp.OCP(
            kind=constants.PLACEMENT_KIND,
            resource_name=self.appset_placement_name,
            namespace="openshift-gitops",
        )
        placcement_obj.annotate(
            annotation="cluster.open-cluster-management.io/experimental-scheduling-disable='true'"
        )

    def _get_workload_namespace(self):
        """
        Get the workload namespace

        """

        app_set_data = list(
            templating.load_yaml(self.appset_yaml_file, multi_document=True)
        )

        for _app_set in app_set_data:
            if _app_set["kind"] == constants.APPLICATION_SET:
                return _app_set["spec"]["template"]["spec"]["destination"]["namespace"]

    def _get_applicaionset_name(self):
        """
        Get ApplicationSet name

        """
        app_set_data = list(
            templating.load_yaml(self.appset_yaml_file, multi_document=True)
        )

        for _app_set in app_set_data:
            if _app_set["kind"] == constants.APPLICATION_SET:
                return _app_set["metadata"]["name"]

    def verify_workload_deployment(self):
        """
        Verify busybox workload

        """

        self.check_pod_pvc_status(skip_replication_resources=False)

        appset_resource_name = (
            self._get_applicaionset_name() + "-" + self.preferred_primary_cluster
        )

        if self.appset_model == "pull":
            appset_pull_obj = ocp.OCP(
                kind=constants.APPLICATION_ARGOCD,
                resource_name=appset_resource_name,
                namespace=constants.GITOPS_CLUSTER_NAMESPACE,
            )
            appset_pull_obj._has_phase = True
            appset_pull_obj.wait_for_phase(phase="Succeeded", timeout=120)

    def check_pod_pvc_status(self, skip_replication_resources=False):
        """
        Check for Pod and PVC status

        Args:
            skip_replication_resources (bool): Skip Volumereplication check

        """
        config.switch_to_cluster_by_name(self.preferred_primary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            self.workload_pvc_count,
            self.workload_pod_count,
            self.workload_namespace,
            skip_replication_resources=skip_replication_resources,
        )

    def delete_workload(self, force=False, rbd_name="rbd", switch_ctx=None):
        """
        Delete busybox workload

        Args:
            force (bool): If True, force remove the stuck resources, default False
            rbd_name (str): Name of the pool, default "rbd"
            switch_ctx (int): The cluster index by the cluster name

        Raises:
            ResourceNotDeleted: In case workload resources not deleted properly

        """
        image_uuids = dr_helpers.get_image_uuids(self.workload_namespace)
        try:
            config.switch_ctx(switch_ctx) if switch_ctx else config.switch_acm_ctx()
            run_cmd(cmd=f"oc delete -f {self.appset_yaml_file}", timeout=900)

            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                dr_helpers.wait_for_all_resources_deletion(
                    namespace=self.workload_namespace,
                    check_replication_resources_state=False,
                )

            log.info("Verify backend RBD images are deleted")
            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                rbd_pool_name = (
                    (config.ENV_DATA.get("rbd_name") or rbd_name)
                    if config.DEPLOYMENT["external_mode"]
                    else constants.DEFAULT_CEPHBLOCKPOOL
                )
                for image_uuid in image_uuids:
                    status = verify_volume_deleted_in_backend(
                        interface=constants.CEPHBLOCKPOOL,
                        image_uuid=image_uuid,
                        pool_name=rbd_pool_name,
                    )
                    if not status:
                        raise UnexpectedBehaviour(
                            "RBD image(s) still exists on backend"
                        )

        except (
            TimeoutExpired,
            TimeoutExpiredError,
            TimeoutError,
            UnexpectedBehaviour,
        ) as ex:
            err_msg = f"Failed to delete the workload: {ex}"
            log.exception(err_msg)
            if force:
                self.resources_cleanup(self.workload_namespace, image_uuids)
            raise ResourceNotDeleted(err_msg)


class CnvWorkload(DRWorkload):
    """
    Class handling everything related to CNV workloads covers both Subscription and Appset apps

    """

    _repo_cloned = False

    def __init__(self, **kwargs):
        """
        Initialize CnvWorkload instance

        """
        workload_repo_url = config.ENV_DATA["dr_workload_repo_url"]
        workload_repo_branch = config.ENV_DATA["dr_workload_repo_branch"]
        super().__init__("cnv", workload_repo_url, workload_repo_branch)

        self.workload_name = kwargs.get("workload_name")
        self.vm_name = kwargs.get("vm_name")
        self.vm_secret_name = kwargs.get("vm_secret")
        self.vm_secret_obj = []
        self.vm_obj = None
        self.vm_username = kwargs.get("vm_username")
        self.workload_type = kwargs.get("workload_type")
        self.workload_namespace = create_unique_resource_name("ns", "vm")
        self.workload_pod_count = kwargs.get("workload_pod_count")
        self.workload_pvc_count = kwargs.get("workload_pvc_count")
        self.dr_policy_name = kwargs.get(
            "dr_policy_name", config.ENV_DATA.get("dr_policy_name")
        ) or (dr_helpers.get_all_drpolicy()[0]["metadata"]["name"])
        self.preferred_primary_cluster = config.ENV_DATA.get(
            "preferred_primary_cluster"
        ) or (get_primary_cluster_config().ENV_DATA["cluster_name"])
        self.target_clone_dir = config.ENV_DATA.get(
            "target_clone_dir", constants.DR_WORKLOAD_REPO_BASE_DIR
        )
        self.cnv_workload_dir = os.path.join(
            self.target_clone_dir, kwargs.get("workload_dir")
        )
        if self.workload_type == constants.SUBSCRIPTION:
            self.channel_name = ""
            self.channel_namespace = create_unique_resource_name("ns", "channel")
            self.channel_yaml_file = os.path.join(
                self.target_clone_dir, kwargs.get("workload_dir"), "channel.yaml"
            )
        self.cnv_workload_yaml_file = os.path.join(
            self.cnv_workload_dir, self.workload_name + ".yaml"
        )
        self.drpc_yaml_file = os.path.join(constants.DRPC_PATH)
        self.cnv_workload_placement_name = kwargs.get("workload_placement_name")
        self.cnv_workload_pvc_selector = kwargs.get("workload_pvc_selector")
        self.appset_model = kwargs.get("appset_model", None)

    def deploy_workload(self):
        """
        Deployment specific to cnv workloads

        """
        self._deploy_prereqs()
        self.vm_obj = VirtualMachine(
            vm_name=self.vm_name, namespace=self.workload_namespace
        )
        self.manage_dr_vm_secrets()

        # Load DRPC
        drpc_yaml_data = templating.load_yaml(self.drpc_yaml_file)
        drpc_yaml_data["metadata"]["name"] = f"{self.cnv_workload_placement_name}-drpc"
        drpc_yaml_data["spec"]["preferredCluster"] = self.preferred_primary_cluster
        drpc_yaml_data["spec"]["drPolicyRef"]["name"] = self.dr_policy_name
        drpc_yaml_data["spec"]["placementRef"][
            "name"
        ] = self.cnv_workload_placement_name
        if self.workload_type == constants.SUBSCRIPTION:
            drpc_yaml_data["metadata"]["namespace"] = self.workload_namespace
            drpc_yaml_data["spec"]["placementRef"][
                "namespace"
            ] = self.workload_namespace
        drpc_yaml_data["spec"]["pvcSelector"][
            "matchLabels"
        ] = self.cnv_workload_pvc_selector
        drcp_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="drpc", delete=False
        )
        templating.dump_data_to_temp_yaml(drpc_yaml_data, drcp_data_yaml.name)

        cnv_workload_yaml_data_load = list(
            templating.load_yaml(self.cnv_workload_yaml_file, multi_document=True)
        )
        if self.workload_type == constants.SUBSCRIPTION:
            # load channel.yaml
            channel_yaml_data_load = list(
                templating.load_yaml(self.channel_yaml_file, multi_document=True)
            )
            for channel_yaml_data in channel_yaml_data_load:
                if channel_yaml_data["kind"] == "Namespace":
                    channel_yaml_data["metadata"]["name"] = self.channel_namespace
                elif channel_yaml_data["kind"] == "Channel":
                    self.channel_name = channel_yaml_data["metadata"]["name"]
                    channel_yaml_data["spec"]["pathname"] = self.workload_repo_url
                    channel_yaml_data["metadata"]["namespace"] = self.channel_namespace
                templating.dump_data_to_temp_yaml(
                    channel_yaml_data_load, self.channel_yaml_file
                )
        for cnv_workload_yaml_data in cnv_workload_yaml_data_load:
            if self.workload_type == constants.SUBSCRIPTION:
                if cnv_workload_yaml_data["kind"] == "Namespace":
                    cnv_workload_yaml_data["metadata"]["name"] = self.workload_namespace
                elif cnv_workload_yaml_data["kind"] == "Application":
                    cnv_workload_yaml_data["metadata"][
                        "namespace"
                    ] = self.workload_namespace
                elif cnv_workload_yaml_data["kind"] == "Subscription":
                    cnv_workload_yaml_data["metadata"][
                        "namespace"
                    ] = self.workload_namespace
                    cnv_workload_yaml_data["spec"][
                        "channel"
                    ] = f"{self.channel_namespace}/{self.channel_name}"
                elif cnv_workload_yaml_data["kind"] == "ManagedClusterSetBinding":
                    cnv_workload_yaml_data["metadata"][
                        "namespace"
                    ] = self.workload_namespace
            elif cnv_workload_yaml_data["kind"] == "ApplicationSet":
                cnv_workload_yaml_data["metadata"]["name"] = self.workload_name
                # Change the destination namespace for AppSet workload
                cnv_workload_yaml_data["spec"]["template"]["spec"]["destination"][
                    "namespace"
                ] = self.workload_namespace

                # Change the AppSet placement label
                for generator in cnv_workload_yaml_data["spec"]["generators"]:
                    if (
                        "clusterDecisionResource" in generator
                        and "labelSelector" in generator["clusterDecisionResource"]
                    ):
                        labels = generator["clusterDecisionResource"][
                            "labelSelector"
                        ].get("matchLabels", {})
                        if "cluster.open-cluster-management.io/placement" in labels:
                            labels[
                                "cluster.open-cluster-management.io/placement"
                            ] = self.cnv_workload_placement_name

                if self.appset_model == "pull":
                    # load appset_yaml_file, add "annotations" key and add values to it
                    cnv_workload_yaml_data["spec"]["template"]["metadata"].setdefault(
                        "annotations", {}
                    )
                    cnv_workload_yaml_data["spec"]["template"]["metadata"][
                        "annotations"
                    ][
                        "apps.open-cluster-management.io/ocm-managed-cluster"
                    ] = "{{name}}"
                    cnv_workload_yaml_data["spec"]["template"]["metadata"][
                        "annotations"
                    ]["argocd.argoproj.io/skip-reconcile"] = "true"

                    # Assign values to the "labels" key
                    cnv_workload_yaml_data["spec"]["template"]["metadata"]["labels"][
                        "apps.open-cluster-management.io/pull-to-ocm-managed-cluster"
                    ] = "true"

            if cnv_workload_yaml_data["kind"] == constants.PLACEMENT:
                cnv_workload_yaml_data["metadata"][
                    "name"
                ] = self.cnv_workload_placement_name
                cnv_workload_yaml_data["metadata"]["namespace"] = (
                    self.workload_namespace
                    if self.workload_type == constants.SUBSCRIPTION
                    else constants.GITOPS_CLUSTER_NAMESPACE
                )
                # Update preferred cluster name
                cnv_workload_yaml_data["spec"]["predicates"][0][
                    "requiredClusterSelector"
                ]["labelSelector"]["matchExpressions"][0]["values"][
                    0
                ] = self.preferred_primary_cluster

        templating.dump_data_to_temp_yaml(
            cnv_workload_yaml_data_load, self.cnv_workload_yaml_file
        )
        config.switch_acm_ctx()
        if self.workload_type == constants.SUBSCRIPTION:
            run_cmd(f"oc create -f {self.channel_yaml_file}")
        run_cmd(f"oc create -f {self.cnv_workload_yaml_file}")
        self.add_annotation_to_placement()
        run_cmd(f"oc create -f {drcp_data_yaml.name}")
        self.verify_workload_deployment()

    def _deploy_prereqs(self):
        """
        Perform prerequisites

        """
        # Clone workload repo
        if not CnvWorkload._repo_cloned:
            CnvWorkload._repo_cloned = True
            clone_repo(
                url=self.workload_repo_url,
                location=self.target_clone_dir,
                branch=self.workload_repo_branch,
            )

    def add_annotation_to_placement(self):
        """
        Add annotation to appset and sub placements

        """

        config.switch_acm_ctx()
        placement_obj = ocp.OCP(
            kind=constants.PLACEMENT_KIND,
            resource_name=self.cnv_workload_placement_name,
            namespace=(
                constants.GITOPS_CLUSTER_NAMESPACE
                if self.workload_type == constants.APPLICATION_SET
                else self.workload_namespace
            ),
        )
        placement_obj.annotate(
            annotation="cluster.open-cluster-management.io/experimental-scheduling-disable='true'"
        )

    def _get_workload_name(self):
        """
        Get cnv workload name

        """
        cnv_workload_data = list(
            templating.load_yaml(self.cnv_workload_yaml_file, multi_document=True)
        )

        for _wl_data in cnv_workload_data:
            if (
                _wl_data["kind"] == constants.APPLICATION_SET
                or _wl_data["kind"] == constants.SUBSCRIPTION
            ):
                return _wl_data["metadata"]["name"]
            if self.workload_type == constants.APPLICATION_SET:
                if _wl_data["kind"] == constants.APPLICATION_SET:
                    return _wl_data["metadata"]["name"]
            else:
                if _wl_data["kind"] == constants.SUBSCRIPTION:
                    return _wl_data["metadata"]["name"]

    def verify_workload_deployment(self):
        """
        Verify cnv workload deployment

        """
        config.switch_to_cluster_by_name(self.preferred_primary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            self.workload_pvc_count,
            self.workload_pod_count,
            self.workload_namespace,
        )
        dr_helpers.wait_for_cnv_workload(
            vm_name=self.vm_name,
            namespace=self.workload_namespace,
            phase=constants.STATUS_RUNNING,
        )

    def delete_workload(self, force=False):
        """
        Deletes cnv workload

        Raises:
            ResourceNotDeleted: In case workload resources not deleted properly

        """
        try:
            config.switch_acm_ctx()
            run_cmd(cmd=f"oc delete -f {self.cnv_workload_yaml_file}", timeout=900)
            if self.workload_type == constants.SUBSCRIPTION:
                run_cmd(f"oc delete -f {self.channel_yaml_file}")
            for cluster, secret_obj in zip(
                get_non_acm_cluster_config(), self.vm_secret_obj
            ):
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                secret_obj.delete()
                dr_helpers.wait_for_all_resources_deletion(
                    namespace=self.workload_namespace,
                    check_replication_resources_state=False,
                )
                log.info(f"Verify VM: {self.vm_name} is deletion")
                vm_obj = ocp.OCP(
                    kind=constants.VIRTUAL_MACHINE_INSTANCES,
                    resource_name=self.vm_name,
                    namespace=self.workload_namespace,
                )
                vm_obj.wait_for_delete(timeout=300)

        except (
            TimeoutExpired,
            TimeoutExpiredError,
            TimeoutError,
            UnexpectedBehaviour,
        ) as ex:
            err_msg = f"Failed to delete the workload: {ex}"
            raise ResourceNotDeleted(err_msg)

    def manage_dr_vm_secrets(self):
        """
        Create secrets to access the VMs via SSH. If a secret already exists, delete and recreate it.

        """
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])

            # Create namespace if it doesn't exist
            try:
                create_project(project_name=self.workload_namespace)
            except CommandFailed as ex:
                if "(AlreadyExists)" in str(ex):
                    log.warning("The namespace already exists!")

            # Create or recreate the secret for ssh access
            try:
                log.info(
                    f"Creating secret namespace {self.workload_namespace} for ssh access"
                )
                self.vm_secret_obj.append(
                    create_vm_secret(
                        secret_name=self.vm_secret_name,
                        namespace=self.workload_namespace,
                    )
                )
            except CommandFailed as ex:
                if "(AlreadyExists)" in str(ex):
                    log.warning(
                        f"Secret {self.vm_secret_name} already exists in namespace {self.workload_namespace}, "
                        f"deleting and recreating the secret to fetch the right SSH pub key."
                    )
                    ocp.OCP(
                        kind=constants.SECRET,
                        namespace=self.workload_namespace,
                    ).delete(resource_name=self.vm_secret_name, wait=True)
                    self.vm_secret_obj.append(
                        create_vm_secret(
                            secret_name=self.vm_secret_name,
                            namespace=self.workload_namespace,
                        )
                    )


def validate_data_integrity_vm(
    cnv_workloads, file_name, md5sum_original, app_state="FailOver"
):
    """
    Validates the MD5 checksum of files on VMs after FailOver/Relocate.

    Args:
        cnv_workloads (list): List of workloads, each containing vm_obj, vm_username, and workload_name.
        file_name (str): Name/path of the file to validate md5sum on.
        md5sum_original (list): List of original MD5 checksums for the file.
        app_state (str): State of the app FailOver/Relocate to log it during validation

    """
    for count, cnv_wl in enumerate(cnv_workloads):
        md5sum_new = cal_md5sum_vm(
            cnv_wl.vm_obj, file_path=file_name, username=cnv_wl.vm_username
        )
        log.info(
            f"Comparing original checksum: {md5sum_original[count]} of {file_name} with {md5sum_new}"
            f" on {cnv_wl.workload_name}after {app_state}"
        )
        assert (
            md5sum_original[count] == md5sum_new
        ), f"Failed: MD5 comparison after {app_state}"


def validate_data_integrity(namespace, path="/mnt/test/hashfile", timeout=600):
    """
    Verifies the md5sum values of files are OK

    Args:
        namespace (str): Namespace where the workload running
        path (str): Path of the hashfile saved of each files
        timeout (int): Time taken in seconds to run command inside pod

    Raises: If there is a mismatch in md5sum value or None

    """
    all_pods = get_all_pods(namespace=namespace)
    for pod_obj in all_pods:
        log.info("Verify the md5sum values are OK")
        cmd = f"md5sum -c {path}"
        try:
            pod_obj.exec_cmd_on_pod(command=cmd, out_yaml_format=False, timeout=timeout)
            log.info(f"Pod {pod_obj.name}: All files checksums value matches")
        except CommandFailed as ex:
            if "computed checksums did NOT match" in str(ex):
                log.error(
                    f"Pod {pod_obj.name}: One or more files or datas are modified"
                )
            raise ex
