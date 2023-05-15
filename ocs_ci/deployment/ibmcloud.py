# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on IBM Cloud Platform.
"""

import json
import logging
import os

from ocs_ci.deployment.cloud import CloudDeploymentBase, IPIOCPDeployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    UnsupportedPlatformVersionError,
    VolumesExistError,
)
from ocs_ci.utility import ibmcloud, version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    delete_file,
    download_file,
    exec_cmd,
)


logger = logging.getLogger(__name__)


__all__ = ["IBMCloud", "IBMCloudIPI"]


class IBMCloudOCPDeployment(BaseOCPDeployment):
    """
    IBM Cloud deployment class.

    """

    def __init__(self):
        super(IBMCloudOCPDeployment, self).__init__()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for IBM cloud deployment.
        """
        super(IBMCloudOCPDeployment, self).deploy_prereq()

    def deploy(self, log_level=""):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level

        """
        # TODO: Add log level to ibmcloud command
        ibmcloud.create_cluster(self.cluster_name)
        kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        ibmcloud.get_kubeconfig(self.cluster_name, kubeconfig_path)
        self.test_cluster()

    def destroy(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        # TODO: Add log level to ibmcloud command
        ibmcloud.destroy_cluster(self.cluster_name)


class IBMCloud(CloudDeploymentBase):
    """
    Deployment class for IBM Cloud
    """

    DEFAULT_STORAGECLASS = "ibmc-vpc-block-10iops-tier"

    OCPDeployment = IBMCloudOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        super(IBMCloud, self).__init__()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        ibmcloud.login()
        super(IBMCloud, self).deploy_ocp(log_cli_level)

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
            "checking existence of IBM Cloud cluster with prefix %s",
            cluster_name_prefix,
        )
        all_clusters = ibmcloud.list_clusters(provider=config.ENV_DATA["provider"])
        non_term_clusters_with_prefix = [
            cl
            for cl in all_clusters
            if cl["state"] != "deleting" and cl["name"].startswith(cluster_name_prefix)
        ]
        return bool(non_term_clusters_with_prefix)


class IBMCloudIPI(CloudDeploymentBase):
    """
    A class to handle IBM Cloud IPI specific deployment
    """

    DEFAULT_STORAGECLASS = "ibmc-vpc-block-10iops-tier"
    OCPDeployment = IPIOCPDeployment

    def __init__(self):
        self.name = self.__class__.__name__
        self.installer = None
        super(IBMCloudIPI, self).__init__()
        self.credentials_requests_dir = os.path.join(self.cluster_path, "creds_reqs")
        self.pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Perform IBMCloudIPI OCP deployment.

        Args:
            log_cli_level (str): log level for installer (default: DEBUG)
        """
        if version.get_semantic_ocp_version_from_config() < version.VERSION_4_10:
            raise UnsupportedPlatformVersionError(
                "IBM Cloud IPI deployments are only supported on OCP versions >= 4.10"
            )
        self.ocp_deployment = self.OCPDeployment()
        self.ocp_deployment.deploy_prereq()

        # IBM Cloud specific prereqs
        ibmcloud.login()
        self.configure_cloud_credential_operator()
        self.export_api_key()
        self.manually_create_iam_for_vpc()

        self.ocp_deployment.deploy(log_cli_level)
        # logging the cluster UUID so that we can ask for its telemetry data
        cluster_id = exec_cmd(
            "oc get clusterversion version -o jsonpath='{.spec.clusterID}'"
        )
        logger.info(f"clusterID (UUID): {cluster_id}")

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy the OCP cluster.

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        self.export_api_key()
        logger.info("Destroying the IBM Cloud cluster")
        super(IBMCloudIPI, self).destroy_cluster(log_level)
        self.delete_service_id()
        resource_group = self.get_resource_group()
        self.delete_volumes(resource_group)
        self.delete_resource_group(resource_group)

    def manually_create_iam_for_vpc(self):
        """
        Manually specify the IAM secrets for the cloud provider
        """
        logger.info("Creating manifests")
        cmd = f"{self.ocp_deployment.installer} create manifests --dir {self.cluster_path}"
        exec_cmd(cmd)

        release_image = self.get_release_image()

        logger.info("Extracting CredentialsRequests")
        cmd = (
            f"oc adm release extract --cloud=ibmcloud --credentials-requests {release_image} "
            f"--to={self.credentials_requests_dir} --registry-config={self.pull_secret_path}"
        )
        exec_cmd(cmd)

        logger.info("Creating service ID")
        cmd = (
            f"ccoctl ibmcloud create-service-id --credentials-requests-dir {self.credentials_requests_dir} "
            f"--name {self.cluster_name} --output-dir {self.cluster_path}"
        )
        exec_cmd(cmd)

    def get_release_image(self):
        """
        Retrieve release image using the openshift installer.
        """
        logger.info("Retrieving release image")
        cmd = f"{self.ocp_deployment.installer} version"
        proc = exec_cmd(cmd)
        for line in proc.stdout.decode().split("\n"):
            if "release image" in line:
                return line.split(" ")[2].strip()

    def get_resource_group(self):
        """
        Retrieve and set the resource group being utilized for the cluster assets.
        """
        cmd = "ibmcloud resource groups --output json"
        proc = exec_cmd(cmd)
        logger.info("Retrieving cluster resource group")
        resource_data = json.loads(proc.stdout)
        for group in resource_data:
            if group["name"].startswith(self.cluster_name):
                # TODO: error prone if cluster_name is a substring of another cluster
                logger.info(f"Found resource group: {group['name']}")
                return group["name"]
        logger.info(f"No resource group found with cluster name: {self.cluster_name}")

    def delete_service_id(self):
        """
        Delete the Service ID.
        """
        logger.info("Deleting service ID")
        cmd = (
            f"ccoctl ibmcloud delete-service-id --credentials-requests-dir {self.credentials_requests_dir} "
            f"--name {self.cluster_name}"
        )
        exec_cmd(cmd)

    def delete_volumes(self, resource_group):
        """
        Delete the pvc volumes created in IBM Cloud that the openshift installer doesn't remove.

        Args:
            resource_group: Resource group in IBM Cloud that contains the cluster resources.

        """

        def _get_volume_ids(resource_group):
            """
            Return a list of volume IDs for the specified Resource Group
            """
            cmd = (
                f"ibmcloud is vols --resource-group-name {resource_group} --output json"
            )
            proc = exec_cmd(cmd)

            volume_data = json.loads(proc.stdout)
            return [volume["id"] for volume in volume_data]

        @retry(VolumesExistError, tries=12, delay=10, backoff=1)
        def _verify_volumes_deleted(resourece_group):
            """
            Verify all volumes in the specified Resource Group are deleted.
            """
            volume_ids = _get_volume_ids(resource_group)
            if volume_ids:
                raise VolumesExistError("Volumes still exist in resource group")

        if resource_group:
            logger.info("Deleting volumes")
            volume_ids = _get_volume_ids(resource_group)

            if volume_ids:
                cmd = f"ibmcloud is vold -f {' '.join(volume_ids)}"
                exec_cmd(cmd)
            else:
                logger.info(f"No volumes found in resource group: {resource_group}")

            _verify_volumes_deleted(resource_group)

    def delete_resource_group(self, resource_group):
        """
        Delete the resource group that contained the cluster assets.

        Args:
            resource_group: Resource group in IBM Cloud that contains the cluster resources.

        """

        @retry(CommandFailed, tries=3, delay=30, backoff=1)
        def _delete_group():
            cmd = f"ibmcloud resource group-delete {resource_group} -f"
            exec_cmd(cmd)

        if resource_group:
            logger.info(f"Deleting resource group: {resource_group}")
            _delete_group()

    @staticmethod
    def check_cluster_existence(cluster_name_prefix):
        """
        Check cluster existence based on a cluster name prefix.

        Args:
            cluster_name_prefix (str): name prefix which identifies a cluster

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        logger.info(
            "Checking existence of IBM Cloud cluster with prefix %s",
            cluster_name_prefix,
        )
        all_clusters = ibmcloud.list_clusters(provider=config.ENV_DATA["provider"])
        cluster_matches = [
            cluster
            for cluster in all_clusters
            if cluster["state"] != "deleting"
            and cluster["name"].startswith(cluster_name_prefix)
        ]
        return bool(cluster_matches)

    @staticmethod
    def configure_cloud_credential_operator():
        """
        Extract and Prepare the CCO utility (ccoctl) binary. This utility
        allows us to create and manage cloud credentials from outside of
        the cluster while in manual mode.

        """
        # retrieve ccoctl binary from https://mirror.openshift.com
        version = config.DEPLOYMENT.get("ccoctl_version")
        source = f"https://mirror.openshift.com/pub/openshift-v4/clients/ocp/{version}/ccoctl-linux.tar.gz"
        bin_dir = config.RUN["bin_dir"]
        tarball = os.path.join(bin_dir, "ccoctl-linux.tar.gz")
        logger.info("Downloading ccoctl tarball from %s", source)
        download_file(source, tarball)
        cmd = f"tar -xzC {bin_dir} -f {tarball} ccoctl"
        logger.info("Extracting ccoctl binary from %s", tarball)
        exec_cmd(cmd)
        delete_file(tarball)

    @staticmethod
    def export_api_key():
        """
        Exports the IBM CLoud API key as an environment variable.
        """
        logger.info("Exporting IC_API_KEY environment variable")
        api_key = config.AUTH["ibmcloud"]["api_key"]
        os.environ["IC_API_KEY"] = api_key
