# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Openshfit Dedicated Platform.
"""


import logging
import os

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.utility import openshift_dedicated as ocm, rosa
from ocs_ci.ocs import ocp
from ocs_ci.ocs.exceptions import CommandFailed

logger = logging.getLogger(name=__file__)


class ROSAOCP(BaseOCPDeployment):
    """
    ROSA deployment class.
    """

    def __init__(self):
        super(ROSAOCP, self).__init__()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for Openshfit Dedciated deployment.
        """
        super(ROSAOCP, self).deploy_prereq()

        openshiftdedicated = config.AUTH.get("openshiftdedicated", {})
        env_vars = {
            "ADDON_NAME": config.ENV_DATA["addon_name"],
            "OCM_COMPUTE_MACHINE_TYPE": config.ENV_DATA.get("worker_instance_type"),
            "NUM_WORKER_NODES": config.ENV_DATA["worker_replicas"],
            "CLUSTER_EXPIRY_IN_MINUTES": config.ENV_DATA["cluster_expiry_in_minutes"],
            "CLUSTER_NAME": config.ENV_DATA["cluster_name"],
            "OCM_TOKEN": openshiftdedicated["token"],
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
        rosa.create_cluster(
            self.cluster_name,
        )
        kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        ocm.get_kubeconfig(self.cluster_name, kubeconfig_path)
        self.test_cluster()

    def destroy(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        ocm.destroy_cluster(self.cluster_name)


class ROSA(CloudDeploymentBase):
    """
    Deployment class for ROSA.
    """

    OCPDeployment = ROSAOCP

    def __init__(self):
        self.name = self.__class__.__name__
        super(ROSA, self).__init__()
        ocm.download_ocm_cli()
        rosa.download_rosa_cli()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        ocm.login()
        super(ROSA, self).deploy_ocp(log_cli_level)

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence based on a cluster name.

        Args:
            cluster_name_prefix (str): name prefix which identifies a cluster

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        cluster_list = ocm.list_cluster()
        for cluster in cluster_list:
            name, state = cluster
            if state != "uninstalling" and name.startswith(cluster_name_prefix):
                return True
        return False

    def deploy_ocs(self):
        """
        Deployment of ODF Managed Service addon on ROSA.
        """
        ceph_cluster = ocp.OCP(kind="CephCluster", namespace=self.namespace)
        try:
            ceph_cluster.get().get("items")[0]
            logger.warning("OCS cluster already exists")
            return
        except (IndexError, CommandFailed):
            logger.info("Running OCS basic installation")
        rosa.install_odf_addon(self.cluster_name)
