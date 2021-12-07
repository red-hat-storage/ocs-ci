"""
This module will have all DR related workload classes

"""

import os


from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import clone_repo, run_cmd
from ocs_ci.utility import templating


class DRWorkload(object):
    """
    Base class for all DR workload classes

    """

    def __init__(self, workload_name=None, workload_repo_url=None):
        self.workload_name = workload_name
        self.workload_repo_url = workload_repo_url

    def deploy_workload(self):
        raise NotImplementedError("Method not implemented")

    def verify_workload_deployment(self):
        raise NotImplementedError("Method not implemented")


class BusyBox(DRWorkload):
    """
    Class handlig everything related to busybox workload

    """

    def __init__(self, **kwargs):
        workload_repo_url = kwargs.get("dr_workload_repo_url")
        super().__init__("busybox", workload_repo_url)
        # Name of the preferred primary cluster
        self.preferred_primary_cluster = kwargs.get("preferred_primary_cluster")
        self.target_clone_dir = kwargs.get("target_clone_dir")
        self.drpc_yaml_file = os.path.join(
            os.path.join(self.target_clone_dir, constants.DR_WORKLOAD_REPO_BASE_DIR),
            kwargs.get("subscription_busybox_drpc_yaml"),
        )

    def deploy_workload(self):
        """
        Deployment specific to busybox workload

        """
        self._deploy_prereqs()
        # load drpc.yaml
        drpc_yaml_data = templating.load_yaml(self.drpc_yaml_file)
        drpc_yaml_data["spec"]["preferredCluster"] = self.preferred_primary_cluster
        templating.dump_to_temp_yaml(drpc_yaml_data, self.drpc_yaml_file)

        # TODO
        # drpc_yaml_file needs to be committed back to the repo
        # because ACM would refetch from repo directly

        # Create the resources on Hub cluster
        config.switch_acm_ctx()
        workload_subscription_dir = os.path.join(
            os.path.join(self.target_clone_dir, constants.DR_WORKLOAD_REPO_BASE_DIR),
            "subscription",
        )
        run_cmd(f"oc create -k {workload_subscription_dir}")
        run_cmd(f"oc create -k {workload_subscription_dir}/{self.workload_name}")

    def _deploy_prereqs(self):
        """
        Perform prerequisites

        """
        # Clone workload repo
        clone_repo(self.workload_repo_url, self.target_clone_dir)
