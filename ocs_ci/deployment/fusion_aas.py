# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Openshfit Dedicated Platform.
"""

import logging
import os

from botocore.exceptions import ClientError

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.helpers.rosa_prod_cluster_helpers import ROSAProdEnvCluster
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.deployment import rosa as rosa_deployment
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import get_operator_pods
from ocs_ci.utility import openshift_dedicated as ocm, rosa
from ocs_ci.utility.aws import AWS as AWSUtil
from ocs_ci.utility.utils import (
    ceph_health_check,
    get_ocp_version,
    TimeoutSampler,
    retry,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ManagedServiceSecurityGroupNotFound,
    TimeoutExpiredError,
)
from ocs_ci.ocs.managedservice import (
    update_non_ga_version,
    update_pull_secret,
    patch_consumer_toolbox,
)
from ocs_ci.ocs.resources import pvc

logger = logging.getLogger(name=__file__)


class FUSIONAASOCP(rosa_deployment.ROSADEPLOYMENT):
    """
    Fusion aaS deployment class.
    """

    def __init__(self):
        super(FUSIONAASOCP, self).__init__()
        self.ocp_version = get_ocp_version()
        self.region = config.ENV_DATA["region"]

    def deploy(self, log_level=""):
        """
        Deployment specific to OCP cluster on a ROSA Managed Service platform.

        Args:
            log_cli_level (str): openshift installer's log level

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
    Deployment class for ROSA.
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
        pod = ocp.OCP(kind=constants.POD, namespace=self.namespace)

        if config.ENV_DATA.get("cluster_type") != "consumer":
            # Check for Ceph pods
            assert pod.wait_for_resource(
                condition="Running",
                selector=constants.MON_APP_LABEL,
                resource_count=3,
                timeout=600,
            )
            assert pod.wait_for_resource(
                condition="Running", selector=constants.MGR_APP_LABEL, timeout=600
            )
            assert pod.wait_for_resource(
                condition="Running",
                selector=constants.OSD_APP_LABEL,
                resource_count=3,
                timeout=600,
            )

        if config.DEPLOYMENT.get("pullsecret_workaround") or config.DEPLOYMENT.get(
            "not_ga_wa"
        ):
            update_pull_secret()
        if config.DEPLOYMENT.get("not_ga_wa"):
            update_non_ga_version()
        if config.ENV_DATA.get("cluster_type") == "consumer":
            retry((CommandFailed, AssertionError), tries=5, delay=30, backoff=1)(
                patch_consumer_toolbox
            )()

        # Verify health of ceph cluster
        ceph_health_check(namespace=self.namespace, tries=60, delay=10)

        # Workaround for the bug 2166900
        if config.ENV_DATA.get("cluster_type") == "consumer":
            configmap_obj = ocp.OCP(
                kind=constants.CONFIGMAP,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            rook_ceph_mon_configmap = configmap_obj.get(
                resource_name=constants.ROOK_CEPH_MON_ENDPOINTS
            )
            rook_ceph_csi_configmap = configmap_obj.get(
                resource_name=constants.ROOK_CEPH_CSI_CONFIG
            )
            for configmap in (rook_ceph_csi_configmap, rook_ceph_mon_configmap):
                if not configmap.get("data").get("csi-cluster-config-json"):
                    logger.warning(
                        f"Configmap {configmap['metadata']['name']} do not contain csi-cluster-config-json."
                    )
                    logger.warning(configmap)
                    logger.info("Deleting rook-ceph-operator as a workaround")
                    rook_operator_pod = get_operator_pods(
                        operator_label=constants.OPERATOR_LABEL,
                        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
                    )
                    rook_operator_pod[0].delete(wait=False)

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
