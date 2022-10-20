"""
This module will have all DR related workload classes

"""

import logging
import os
from subprocess import TimeoutExpired
from time import sleep

from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.helpers import delete_volume_in_backend
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError, CommandFailed
from ocs_ci.ocs.utils import get_primary_cluster_config, get_non_acm_cluster_config
from ocs_ci.utility.utils import clone_repo, run_cmd
from ocs_ci.utility import templating

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
    def resources_cleanup(namespace):
        """
        Cleanup workload and replication resources in a given namespace from managed clusters.
        Useful for removing leftover resources to avoid further test failures.

        Args:
            namespace(str): the namespace of the workload

        """
        resources = [
            constants.POD,
            constants.VOLUME_REPLICATION_GROUP,
            constants.VOLUME_REPLICATION,
            constants.PVC,
            constants.PV,
        ]
        rbd_images = []
        cephfs_subvolumes = []
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
                            if (
                                item["spec"]["storageClassName"]
                                == constants.DEFAULT_STORAGECLASS_RBD
                            ):
                                rbd_images.append(
                                    item["spec"]["csi"]["volumeAttributes"]["imageName"]
                                )
                            else:
                                cephfs_subvolumes.append(
                                    item["spec"]["csi"]["volumeAttributes"][
                                        "subvolumeName"
                                    ]
                                )
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
            for image in rbd_images:
                delete_volume_in_backend(
                    img_uuid="-".join(image.split("-")[2:]),
                    pool_name=constants.DEFAULT_CEPHBLOCKPOOL,
                    disable_mirroring=True,
                )
            for subvolume in cephfs_subvolumes:
                delete_volume_in_backend(
                    img_uuid="-".join(subvolume.split("-")[2:]),
                    pool_name=defaults.CEPHFILESYSTEM_NAME,
                )


class BusyBox(DRWorkload):
    """
    Class handling everything related to busybox workload

    """

    def __init__(self, **kwargs):
        workload_repo_url = config.ENV_DATA["dr_workload_repo_url"]
        workload_repo_branch = config.ENV_DATA["dr_workload_repo_branch"]
        super().__init__("busybox", workload_repo_url, workload_repo_branch)

        self.workload_namespace = None
        self.workload_pod_count = config.ENV_DATA["dr_workload_pod_count"]
        self.workload_pvc_count = config.ENV_DATA["dr_workload_pvc_count"]

        self.dr_policy_name = config.ENV_DATA.get("dr_policy_name") or (
            dr_helpers.get_all_drpolicy()[0]["metadata"]["name"]
        )
        self.preferred_primary_cluster = config.ENV_DATA.get(
            "preferred_primary_cluster"
        ) or (get_primary_cluster_config().ENV_DATA["cluster_name"])
        self.target_clone_dir = config.ENV_DATA.get(
            "target_clone_dir", constants.DR_WORKLOAD_REPO_BASE_DIR
        )
        self.workload_subscription_dir = os.path.join(
            self.target_clone_dir, config.ENV_DATA.get("dr_workload_subscription_dir")
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
            self.workload_repo_url, self.target_clone_dir, self.workload_repo_branch
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
        dr_helpers.wait_for_mirroring_status_ok()

    def delete_workload(self, force=False):
        """
        Delete busybox workload

        """
        try:
            config.switch_acm_ctx()
            run_cmd(
                f"oc delete -k {self.workload_subscription_dir}/{self.workload_name}"
            )

            for cluster in get_non_acm_cluster_config():
                config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
                dr_helpers.wait_for_all_resources_deletion(
                    namespace=self.workload_namespace,
                    check_replication_resources_state=False,
                )

        except (TimeoutExpired, TimeoutExpiredError, TimeoutError):
            if force:
                self.resources_cleanup(self.workload_namespace)
            else:
                raise

        finally:
            config.switch_acm_ctx()
            run_cmd(f"oc delete -k {self.workload_subscription_dir}")
