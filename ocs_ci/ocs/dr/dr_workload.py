"""
This module will have all DR related workload classes

"""

import os

from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.utils import get_primary_cluster_config
from ocs_ci.utility.utils import clone_repo, run_cmd
from ocs_ci.utility import templating


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

        # Name of the preferred primary cluster
        self.preferred_primary_cluster = config.ENV_DATA.get(
            "preferred_primary_cluster"
        ) or (get_primary_cluster_config().ENV_DATA["cluster_name"])
        self.target_clone_dir = config.ENV_DATA.get(
            "target_clone_dir", constants.DR_WORKLOAD_REPO_BASE_DIR
        )
        self.workload_subscription_dir = os.path.join(
            self.target_clone_dir, "subscriptions"
        )
        self.drpc_yaml_file = os.path.join(
            os.path.join(self.workload_subscription_dir, self.workload_name),
            "drpc.yaml",
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
        templating.dump_data_to_temp_yaml(drpc_yaml_data, self.drpc_yaml_file)

        # TODO
        # drpc_yaml_file needs to be committed back to the repo
        # because ACM would refetch from repo directly

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
        dr_helpers.wait_for_workload_resource_creation(
            self.workload_pvc_count, self.workload_pod_count, self.workload_namespace
        )
        dr_helpers.wait_for_vr_creation(
            self.workload_pvc_count, self.workload_namespace
        )
        dr_helpers.wait_for_mirroring_status_ok()

    def delete_workload(self):
        """
        Delete busybox workload

        """
        primary_cluster_name = dr_helpers.get_primary_cluster_name(
            self.workload_namespace
        )

        config.switch_acm_ctx()
        run_cmd(f"oc delete -k {self.workload_subscription_dir}/{self.workload_name}")
        run_cmd(f"oc delete -k {self.workload_subscription_dir}")

        config.switch_to_cluster_by_name(primary_cluster_name)
        dr_helpers.wait_for_workload_resource_deletion(self.workload_namespace)
