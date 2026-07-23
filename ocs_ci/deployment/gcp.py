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
    A class to handle GCP IPI specific deployment.

    Supports both standard and STS (Workload Identity Federation)
    deployments. STS behavior is activated when
    config.DEPLOYMENT["sts_enabled"] is True.
    """

    def __init__(self):
        self.name = self.__class__.__name__
        super(GCPIPI, self).__init__()

    @staticmethod
    def _get_gcp_project():
        """
        Set GCP authentication and return the project ID.

        Returns:
            str: GCP project ID

        """
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_FILEPATH
        sa_dict = load_service_account_key_dict()
        return config.ENV_DATA.get("gcp_project_id") or sa_dict["project_id"]

    @staticmethod
    def _ensure_ccoctl_and_credentials_requests(credentials_requests_dir):
        """
        Ensure the ccoctl binary and CredentialsRequest manifests are
        available. Both are extracted from the OCP release image if missing.

        Args:
            credentials_requests_dir (str): Path to the CredentialsRequest
                directory. If this directory does not exist, the manifests
                are re-extracted.

        """
        cluster_path = config.ENV_DATA["cluster_path"]
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        release_image = get_ocp_release_image_from_installer()
        cco_image = cco.get_cco_container_image(release_image, pull_secret_path)
        cco.extract_ccoctl_binary(cco_image, pull_secret_path)
        if not os.path.isdir(credentials_requests_dir):
            logger.info("Credentials requests directory not found, re-extracting")
            install_config = os.path.join(cluster_path, "install-config.yaml")
            cco.extract_credentials_requests(
                release_image,
                install_config,
                pull_secret_path,
                credentials_requests_dir,
            )

    class OCPDeployment(IPIOCPDeployment):
        """
        GCP-specific OCP deployment that adds Workload Identity
        Federation (WIF) setup when STS mode is enabled.

        For non-STS deployments, behaves identically to the
        base IPIOCPDeployment.
        """

        def deploy_prereq(self):
            """Run base prerequisites, then WIF setup if STS is enabled."""
            super().deploy_prereq()
            if config.DEPLOYMENT.get("sts_enabled"):
                self.sts_setup()

        def sts_setup(self):
            """
            Set up GCP Workload Identity Federation via ccoctl.

            Steps:
                1. Set GCP authentication for ccoctl
                2. Extract ccoctl binary and CredentialsRequest manifests
                3. Configure manual credentials mode
                4. Generate install manifests
                5. Run ccoctl gcp create-all to create WIF resources
                6. Copy generated manifests and TLS into the cluster dir
            """
            cluster_path = config.ENV_DATA["cluster_path"]
            output_dir = os.path.join(cluster_path, "output-dir")
            credentials_requests_dir = os.path.join(cluster_path, "creds_reqs")
            install_config = os.path.join(cluster_path, "install-config.yaml")

            # 1. Set GCP authentication
            gcp_project = GCPIPI._get_gcp_project()

            # 2. Extract ccoctl binary and CredentialsRequest manifests
            GCPIPI._ensure_ccoctl_and_credentials_requests(credentials_requests_dir)

            # 3-4. Configure manual credentials mode and generate manifests
            cco.set_credentials_mode_manual(install_config)
            cco.create_manifests(self.installer, cluster_path)

            # 5. Run ccoctl gcp create-all
            infra_id = get_infra_id_from_openshift_install_state(cluster_path)
            cco.process_credentials_requests_gcp(
                infra_id,
                config.ENV_DATA["region"],
                gcp_project,
                credentials_requests_dir,
                output_dir,
            )

            # 6. Copy generated manifests and TLS into the cluster dir
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
        Destroy OCP cluster on GCP.

        For STS deployments, deletes the WIF resources created by
        ccoctl before running the standard cluster destroy.

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        if config.DEPLOYMENT.get("sts_enabled"):
            try:
                # 1. Set GCP authentication
                gcp_project = self._get_gcp_project()

                # 2. Ensure ccoctl binary and CredentialsRequest manifests
                # are available (may be missing if teardown runs on a
                # different agent than the one that deployed)
                cluster_path = config.ENV_DATA["cluster_path"]
                credentials_requests_dir = os.path.join(cluster_path, "creds_reqs")
                self._ensure_ccoctl_and_credentials_requests(credentials_requests_dir)

                # 3. Run ccoctl gcp delete to remove WIF resources
                infra_id = get_infra_id_from_openshift_install_state(cluster_path)
                cco.delete_gcp_sts_resources(
                    infra_id,
                    gcp_project,
                    credentials_requests_dir,
                )
            except Exception:
                logger.warning(
                    "Failed to delete GCP STS resources. "
                    "Proceeding with cluster destroy.",
                    exc_info=True,
                )
        super().destroy_cluster(log_level)
