# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Google Cloud Platform (aka GCP).
"""

import logging
import os
import shutil

from libcloud.compute.types import NodeState

from ocs_ci.deployment.cloud import CloudDeploymentBase, IPIOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.utility import cco
from ocs_ci.utility.deployment import get_ocp_release_image_from_installer
from ocs_ci.utility.gcp import (
    GoogleCloudUtil,
    load_service_account_key_dict,
    SERVICE_ACCOUNT_KEY_FILEPATH,
)
from ocs_ci.utility.utils import get_infra_id_from_openshift_install_state


logger = logging.getLogger(__name__)


__all__ = ["GCPIPI"]


class GCPBase(CloudDeploymentBase):
    """
    Google Cloud deployment base class, with code common to both IPI and UPI.

    Having this base class separate from GCPIPI even when we have implemented
    IPI only makes adding UPI class later easier, moreover code structure is
    comparable with other platforms.
    """

    def __init__(self):
        super(GCPBase, self).__init__()
        self.util = GoogleCloudUtil()

    def add_node(self):
        # TODO: implement later
        super(GCPBase, self).add_node()

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
            "checking existence of GCP cluster with prefix %s", cluster_name_prefix
        )
        non_term_cluster_nodes = []
        for node in self.util.compute_driver.list_nodes():
            if (
                node.name.startswith(cluster_name_prefix)
                and node.state != NodeState.TERMINATED
            ):
                non_term_cluster_nodes.append(node)
        if len(non_term_cluster_nodes) > 0:
            logger.warning(
                "Non terminated nodes with the same name prefix were found: %s",
                non_term_cluster_nodes,
            )
            return True
        return False


class GCPIPI(GCPBase):
    """
    A class to handle GCP IPI specific deployment
    """

    def __init__(self):
        self.name = self.__class__.__name__
        super(GCPIPI, self).__init__()

    class OCPDeployment(IPIOCPDeployment):
        def deploy_prereq(self):
            super().deploy_prereq()
            if config.DEPLOYMENT.get("sts_enabled"):
                self.sts_setup()

        def sts_setup(self):
            """
            Perform setup procedure for STS Mode (Workload Identity
            Federation) deployments on GCP.
            """
            cluster_path = config.ENV_DATA["cluster_path"]
            output_dir = os.path.join(cluster_path, "output-dir")
            pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
            credentials_requests_dir = os.path.join(cluster_path, "creds_reqs")
            install_config = os.path.join(cluster_path, "install-config.yaml")

            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_FILEPATH
            sa_dict = load_service_account_key_dict()
            gcp_project = config.ENV_DATA.get("gcp_project_id") or sa_dict["project_id"]

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
            cco.create_manifests(self.installer, cluster_path)
            infra_id = get_infra_id_from_openshift_install_state(cluster_path)
            cco.process_credentials_requests_gcp(
                infra_id,
                config.ENV_DATA["region"],
                gcp_project,
                credentials_requests_dir,
                output_dir,
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
        Destroy OCP cluster specific to GCP IPI.

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        if config.DEPLOYMENT.get("sts_enabled"):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_FILEPATH
            sa_dict = load_service_account_key_dict()
            gcp_project = config.ENV_DATA.get("gcp_project_id") or sa_dict["project_id"]
            cluster_path = config.ENV_DATA["cluster_path"]
            credentials_requests_dir = os.path.join(cluster_path, "creds_reqs")
            if not os.path.isdir(credentials_requests_dir):
                logger.info("Credentials requests directory not found, re-extracting")
                pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
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
            cco.delete_gcp_sts_resources(
                self.cluster_name,
                gcp_project,
                credentials_requests_dir,
            )
        super(GCPIPI, self).destroy_cluster(log_level)
