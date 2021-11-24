# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Openshfit Dedicated Platform.
"""


import os

from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.utility import openshift_dedicated
from ocs_ci.deployment.cloud import CloudDeploymentBase


class OpenshiftDedicatedOCP(BaseOCPDeployment):
    """
    Openshift Dedicated deployment class.

    """

    def __init__(self):
        super(OpenshiftDedicatedOCP, self).__init__()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for Openshfit Dedciated deployment.
        """
        super(OpenshiftDedicatedOCP, self).deploy_prereq()

        openshiftdedicated = config.AUTH.get("openshiftdedicated", {})
        env_vars = {
            "ADDON_IDS": config.ENV_DATA["addon_id"],
            "OCM_COMPUTE_MACHINE_TYPE": config.ENV_DATA.get("worker_instance_type"),
            "NUM_WORKER_NODES": config.ENV_DATA["worker_replicas"],
            "CLUSTER_EXPIRY_IN_MINUTES": config.ENV_DATA["cluster_expiry_in_minutes"],
            "CLUSTER_NAME": config.ENV_DATA["cluster_name"],
            "OCM_CCS": config.ENV_DATA["ocm_ccs"],
            "OCM_TOKEN": openshiftdedicated["token"],
            "OCM_AWS_ACCOUNT": openshiftdedicated["OCM_AWS_ACCOUNT"],
            "OCM_AWS_ACCESS_KEY": openshiftdedicated["OCM_AWS_ACCESS_KEY"],
            "OCM_AWS_SECRET_KEY": openshiftdedicated["OCM_AWS_SECRET_KEY"],
        }

        for key, value in env_vars.items():
            if value:
                os.environ[key] = value

    def deploy(self, log_level=""):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level

        """
        openshift_dedicated.create_cluster(
            self.cluster_name,
        )
        kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        openshift_dedicated.get_kubeconfig(self.cluster_name, kubeconfig_path)
        self.test_cluster()

    def destroy(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        openshift_dedicated.destroy_cluster(self.cluster_name)


class OpenshiftDedicated(CloudDeploymentBase):
    """
    Deployment class for Openshift Dedicated.
    """

    OCPDeployment = OpenshiftDedicatedOCP

    def __init__(self):
        self.name = self.__class__.__name__
        super(OpenshiftDedicated, self).__init__()
        openshift_dedicated.download_ocm_cli()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        openshift_dedicated.login()
        super(OpenshiftDedicated, self).deploy_ocp(log_cli_level)

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence based on a cluster name.

        Args:
            cluster_name_prefix (str): name prefix which identifies a cluster

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        cluster_list = openshift_dedicated.list_cluster()
        for cluster in cluster_list:
            name, state = cluster
            if state != "uninstalling" and name.startswith(cluster_name_prefix):
                return True
        return False
