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
    LeftoversExistError,
    VolumesExistError,
)
from ocs_ci.ocs.resources.pvc import (
    scale_down_pods_and_remove_pvcs,
)
from ocs_ci.utility import ibmcloud, version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    delete_file,
    download_file,
    exec_cmd,
    get_infra_id_from_openshift_install_state,
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
        resource_group = self.get_resource_group()
        if resource_group:
            try:
                scale_down_pods_and_remove_pvcs(self.DEFAULT_STORAGECLASS)
            except Exception as err:
                logger.warning(
                    f"Failed to scale down mon/osd pods or failed to remove PVC's. Error: {err}"
                )
            logger.info("Destroying the IBM Cloud cluster")
            super(IBMCloudIPI, self).destroy_cluster(log_level)

        else:
            logger.warning(
                "Resource group for the cluster doesn't exist! Will not run installer to destroy the cluster!"
            )
        # Make sure ccoctl is downloaded before using it in destroy job.
        self.configure_cloud_credential_operator()
        self.delete_service_id()
        if resource_group:
            resource_group = self.get_resource_group()
        # Based on docs:
        # https://docs.openshift.com/container-platform/4.13/installing/installing_ibm_cloud_public/uninstalling-cluster-ibm-cloud.html
        # The volumes should be removed before running openshift-installer for destroy, but it's not
        # working and failing, hence moving this step back after openshift-installer.
        self.delete_volumes(resource_group)
        self.delete_leftover_resources(resource_group)
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

        # get infraID
        infra_id = get_infra_id_from_openshift_install_state(self.cluster_path)

        logger.info("Creating service ID")
        cmd = (
            f"ccoctl ibmcloud create-service-id --credentials-requests-dir {self.credentials_requests_dir} "
            f"--name {infra_id} --output-dir {self.cluster_path}"
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

    def get_resource_group(self, return_id=False):
        """
        Retrieve and set the resource group being utilized for the cluster assets.

        Args:
            return_id (bool): If True, it will return ID instead of name.

        Returns:
            str: name or ID of resource group if found.
            None: in case no RG found.

        """
        cmd = "ibmcloud resource groups --output json"
        proc = exec_cmd(cmd)
        logger.info("Retrieving cluster resource group")
        resource_data = json.loads(proc.stdout)
        for group in resource_data:
            if group["name"][:-6] == self.cluster_name:
                logger.info(f"Found resource group: {group['name']}")
                if not return_id:
                    return group["name"]
                else:
                    return group["id"]
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
            resource_group (str): Resource group in IBM Cloud that contains the cluster resources.

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

    @retry((LeftoversExistError, CommandFailed), tries=3, delay=30, backoff=1)
    def delete_leftover_resources(self, resource_group):
        """
        Delete leftovers from IBM Cloud.

        Args:
            resource_group (str): Resource group in IBM Cloud that contains the cluster resources.

        Raises:
            LeftoversExistError: In case the leftovers after attempt to clean them out.

        """

        def _get_resources(resource_group):
            """
            Return a list leftover resources for the specified Resource Group
            """
            cmd = f"ibmcloud resource service-instances --type all -g {resource_group} --output json"
            proc = exec_cmd(cmd)

            return json.loads(proc.stdout)

        def _get_reclamations(resource_group):
            """
            Get reclamations for resource group.

            Args:
                rsource_group (str): Resource group name

            Returns:
                list: Reclamations for resource group if found.
            """
            rg_id = self.get_resource_group(return_id=True)
            cmd = "ibmcloud resource reclamations --output json"
            proc = exec_cmd(cmd)
            reclamations = json.loads(proc.stdout)
            rg_reclamations = []
            for reclamation in reclamations:
                if reclamation["resource_group_id"] == rg_id:
                    rg_reclamations.append(reclamation)
            return rg_reclamations

        def _delete_reclamations(reclamations):
            """
            Delete reclamations

            Args:
                reclamations (list): Reclamations to delete

            """
            for reclamation in reclamations:
                logger.info(f"Deleting reclamation: {reclamation}")
                cmd = (
                    f"ibmcloud resource reclamation-delete {reclamation['id']} "
                    "--comment 'Force deleting leftovers' -f"
                )
                exec_cmd(cmd)

        def _delete_resources(resources, ignore_errors=False):
            """
            Deleting leftover resources.

            Args:
                resources (list): Resource leftover names.
                ignore_errors (bool): If True, it will be ignoring errors from ibmcloud cmd.

            """
            for resource in resources:
                logger.info(f"Deleting leftover {resource}")
                delete_cmd = f"ibmcloud resource service-instance-delete -g {resource_group} -f --recursive {resource}"
                if ignore_errors:
                    try:
                        exec_cmd(delete_cmd)
                    except CommandFailed as ex:
                        logger.debug(
                            f"Exception will be ignored because ignore_error is set to true! Exception: {ex}"
                        )
                else:
                    exec_cmd(delete_cmd)

        if resource_group:
            leftovers = _get_resources(resource_group)
            if not leftovers:
                logger.info("No leftovers found")
            else:
                resource_names = set([r["name"] for r in leftovers])
                logger.info(f"Deleting leftovers {resource_names}")
                _delete_resources(resource_names, ignore_errors=True)
            reclamations = _get_reclamations(resource_group)
            if reclamations:
                _delete_reclamations(reclamations)
            # Additional check if all resources got really deleted:
            if leftovers:
                leftovers = _get_resources(resource_group)
                if leftovers:
                    raise LeftoversExistError(
                        "Leftovers detected, you can use the details below to report support case in IBM Cloud:\n"
                        f"{leftovers}"
                    )

    def delete_resource_group(self, resource_group):
        """
        Delete the resource group that contained the cluster assets.

        Args:
            resource_group (str): Resource group in IBM Cloud that contains the cluster resources.

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
        bin_dir = config.RUN["bin_dir"]
        ccoctl_path = os.path.join(bin_dir, "ccoctl")
        if not os.path.isfile(ccoctl_path):
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
