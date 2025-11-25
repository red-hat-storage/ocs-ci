# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Azure platform.
"""

import json
import logging
import os
import shutil

from ocs_ci.framework import config
from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.cloud import IPIOCPDeployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.ocs import constants
from ocs_ci.utility import cco
from ocs_ci.utility.azure_utils import AZURE as AzureUtil, AzureAroUtil
from ocs_ci.utility.deployment import get_ocp_release_image_from_installer
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


__all__ = ["AZUREIPI"]


class AZUREBase(CloudDeploymentBase):
    """
    Azure deployment base class, with code common to both IPI and UPI.

    Having this base class separate from AZUREIPI even when we have implemented
    IPI only makes adding UPI class later easier, moreover code structure is
    comparable with other platforms.
    """

    def __init__(self):
        super(AZUREBase, self).__init__()
        self.azure_util = AzureUtil()

    def add_node(self):
        # TODO: implement later
        super(AZUREBase, self).add_node()

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence based on a cluster name prefix.

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise
        """
        logger.info("checking existence of cluster with prefix %s", cluster_name_prefix)
        # ask about all azure resource groups, and filter it by cluster name
        # prefix (there is azure resource group for each cluster, which
        # contains all other azure resources of the cluster)
        resource_groups = self.azure_util.resource_client.resource_groups.list()
        for rg in resource_groups:
            if rg.name.startswith(cluster_name_prefix):
                logger.info(
                    "For given cluster name prefix %s, there is a resource group %s already.",
                    cluster_name_prefix,
                    rg.name,
                )
                return True
        logger.info(
            "For given cluster name prefix %s, there is no resource group.",
            cluster_name_prefix,
        )
        return False


class AZUREIPI(AZUREBase):
    """
    A class to handle Azure IPI specific deployment.
    """

    def __init__(self):
        self.name = self.__class__.__name__
        super(AZUREIPI, self).__init__()
        # Set custom storage class path for Azure Performance Plus feature
        if config.ENV_DATA.get("azure_performance_plus") or config.DEPLOYMENT.get(
            "azure_performance_plus"
        ):
            self.custom_storage_class_path = os.path.join(
                constants.TEMPLATE_DEPLOYMENT_DIR, "azure_storageclass_perfplus.yaml"
            )
            logger.info(
                "Azure Performance Plus enabled. Will use custom storage class: %s",
                self.custom_storage_class_path,
            )

    class OCPDeployment(IPIOCPDeployment):
        def deploy_prereq(self):
            super().deploy_prereq()
            if config.DEPLOYMENT.get("sts_enabled"):
                self.sts_setup()

        def sts_setup(self):
            """
            Perform setup procedure for STS Mode deployments.
            """
            cluster_path = config.ENV_DATA["cluster_path"]
            output_dir = os.path.join(cluster_path, "output-dir")
            pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
            credentials_requests_dir = os.path.join(cluster_path, "creds_reqs")
            install_config = os.path.join(cluster_path, "install-config.yaml")

            release_image = get_ocp_release_image_from_installer()
            cco_image = cco.get_cco_container_image(release_image, pull_secret_path)
            cco.extract_ccoctl_binary(cco_image, pull_secret_path)
            cco.extract_credentials_requests(
                release_image,
                install_config,
                pull_secret_path,
                credentials_requests_dir,
            )
            cco.set_credentials_mode_manual(install_config)
            cco.set_resource_group_name(install_config, self.cluster_name)
            cco.create_manifests(self.installer, cluster_path)
            azure_util = AzureUtil()
            azure_util.set_auth_env_vars()
            cco.process_credentials_requests_azure(
                self.cluster_name,
                config.ENV_DATA["region"],
                credentials_requests_dir,
                output_dir,
                config.AUTH["azure_auth"]["subscription_id"],
                config.ENV_DATA["azure_base_domain_resource_group_name"],
                config.AUTH["azure_auth"]["tenant_id"],
            )
            manifests_source_dir = os.path.join(output_dir, "manifests")
            manifests_target_dir = os.path.join(cluster_path, "manifests")
            file_names = os.listdir(manifests_source_dir)
            for file_name in file_names:
                shutil.move(
                    os.path.join(manifests_source_dir, file_name), manifests_target_dir
                )

            tls_source_dir = os.path.join(output_dir, "tls")
            tls_target_dir = os.path.join(cluster_path, "tls")
            shutil.move(tls_source_dir, tls_target_dir)

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific to Azure IPI

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        if config.DEPLOYMENT.get("sts_enabled"):
            self.azure_util.set_auth_env_vars()
            cco.delete_oidc_resource_group(
                self.cluster_name,
                config.ENV_DATA["region"],
                config.AUTH["azure_auth"]["subscription_id"],
            )
        super(AZUREIPI, self).destroy_cluster(log_level)

    # For Azure IPI there is no need to implement custom:
    # - deploy_ocp() method (as long as we don't tweak host network)


class AzureCloudAroOCPDeployment(BaseOCPDeployment):
    """
    Azure ARO Managed deployment class.

    """

    def __init__(self):
        super(AzureCloudAroOCPDeployment, self).__init__()
        self.azure_util = AzureAroUtil()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for Azure ARO cloud deployment.
        """
        super(AzureCloudAroOCPDeployment, self).deploy_prereq()

    def deploy(self, log_level=""):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level

        """
        # TODO: Add log level to az command

        self.azure_util.create_cluster(self.cluster_name)
        self.test_cluster()

    def destroy(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        # TODO: Add log level to az command
        self.azure_util.destroy_cluster(
            self.cluster_name,
            config.ENV_DATA["azure_base_domain_resource_group_name"],
        )


class AZUREAroManaged(AZUREBase):
    """
    Deployment class for Azure Aro
    """

    OCPDeployment = AzureCloudAroOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        self.azure_util = AzureAroUtil()
        super(AZUREAroManaged, self).__init__()
        # Set custom storage class path for Azure Performance Plus feature
        if config.ENV_DATA.get("azure_performance_plus") or config.DEPLOYMENT.get(
            "azure_performance_plus"
        ):
            self.custom_storage_class_path = os.path.join(
                constants.TEMPLATE_DEPLOYMENT_DIR, "azure_storageclass_perfplus.yaml"
            )
            logger.info(
                "Azure Performance Plus enabled. Will use custom storage class: %s",
                self.custom_storage_class_path,
            )

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        super(AZUREAroManaged, self).deploy_ocp(log_cli_level)

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence based on a cluster name prefix.

        Args:
            cluster_name_prefix (str): name prefix which identifies a cluster

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        logger.info(
            "checking existence of Azure cluster with prefix %s",
            cluster_name_prefix,
        )
        data = json.loads(exec_cmd("az aro list -o json").stdout)
        for cluster in data:
            if cluster_name_prefix in cluster["name"]:
                return True
        return False
