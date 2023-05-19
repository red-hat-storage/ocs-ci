# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Fusion aaS
"""

import logging
import os
from time import sleep

from ocs_ci.deployment.helpers.rosa_prod_cluster_helpers import ROSAProdEnvCluster
from ocs_ci.deployment import rosa as rosa_deployment
from ocs_ci.framework import config
from ocs_ci.utility import openshift_dedicated as ocm, rosa
from ocs_ci.utility.aws import AWS as AWSUtil
from ocs_ci.utility.utils import get_ocp_version, wait_for_machineconfigpool_status
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
)
from ocs_ci.ocs.fusion import create_fusion_monitoring_resources, deploy_odf
from ocs_ci.ocs.managedservice import update_pull_secret
from ocs_ci.ocs.resources import pvc

logger = logging.getLogger(name=__file__)


class FUSIONAASOCP(rosa_deployment.ROSAOCP):
    """
    Fusion aaS deployment class.
    """

    def __init__(self):
        super(FUSIONAASOCP, self).__init__()
        self.ocp_version = get_ocp_version()
        self.region = config.ENV_DATA["region"]

    def deploy(self, log_level=""):
        """
        Deployment specific to OCP cluster on a Fusion aaS platform.

        Args:
            log_level (str): openshift installer's log level that is expected from
                inherited class

        """
        rosa.create_cluster(self.cluster_name, self.ocp_version, self.region)

        kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        password_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["password_location"]
        )

        # generate kubeconfig and kubeadmin-password files
        if config.ENV_DATA["ms_env_type"] == "staging":
            ocm.get_kubeconfig(self.cluster_name, kubeconfig_path)
            ocm.get_kubeadmin_password(self.cluster_name, password_path)
        if config.ENV_DATA["ms_env_type"] == "production":
            if config.ENV_DATA.get("appliance_mode"):
                logger.info(
                    "creating admin account for cluster in production environment with "
                    "appliance mode deployment is not supported"
                )
                return
            else:
                rosa_prod_cluster = ROSAProdEnvCluster(self.cluster_name)
                rosa_prod_cluster.create_admin_and_login()
                rosa_prod_cluster.generate_kubeconfig_file(skip_tls_verify=True)
                rosa_prod_cluster.generate_kubeadmin_password_file()

        self.test_cluster()


class FUSIONAAS(rosa_deployment.ROSA):
    """
    Deployment class for Fusion aaS.
    """

    OCPDeployment = FUSIONAASOCP

    def __init__(self):
        self.name = self.__class__.__name__
        super(FUSIONAAS, self).__init__()
        ocm.download_ocm_cli()
        rosa.download_rosa_cli()
        self.aws = AWSUtil(self.region)

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        ocm.login()
        super(FUSIONAAS, self).deploy_ocp(log_cli_level)
        self.host_network_update()

    def deploy_ocs(self):
        """
        Deployment of ODF Managed Service addon on Fusion aaS.
        """
        managed_fusion_offering = ocp.OCP(
            kind=constants.MANAGED_FUSION_OFFERING, namespace=self.namespace
        )
        try:
            managed_fusion_offering.get().get("items")[0]
            logger.warning("ManagedFusionOffering exists. Skipping installation.")
            return
        except (IndexError, CommandFailed):
            logger.info("Running OCS basic installation")
        create_fusion_monitoring_resources()
        if config.DEPLOYMENT.get("pullsecret_workaround"):
            # The pull secret may not get updated on all nodes if any node is not updated. Ensure it by checking the
            # status of machineconfigpool
            wait_for_machineconfigpool_status("all", timeout=2800)
            update_pull_secret()
            # Add an initial wait time for the change. Otherwise wait_for_machineconfigpool_status will return
            # before the starting of update
            sleep(20)
            wait_for_machineconfigpool_status("all", timeout=900)
        deploy_odf()

    def destroy_ocs(self):
        """
        Uninstall ODF Managed Service addon via rosa cli.
        """
        cluster_namespace = config.ENV_DATA["cluster_namespace"]

        # Deleting PVCs
        rbd_pvcs = [
            p
            for p in pvc.get_all_pvcs_in_storageclass(constants.CEPHBLOCKPOOL_SC)
            if not (
                p.data["metadata"]["namespace"] == cluster_namespace
                and p.data["metadata"]["labels"]["app"] == "noobaa"
            )
        ]
        pvc.delete_pvcs(rbd_pvcs)
        cephfs_pvcs = pvc.get_all_pvcs_in_storageclass(constants.CEPHFILESYSTEM_SC)
        pvc.delete_pvcs(cephfs_pvcs)
        logger.error("OCS NOT IMPLEMENTED")
        logger.error("OCS needs to be deleted manually")
