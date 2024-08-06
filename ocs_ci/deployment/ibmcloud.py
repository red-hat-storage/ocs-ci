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
from ocs_ci.ocs.defaults import IBM_CLOUD_REGIONS
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
from ocs_ci.utility import cco
from ocs_ci.utility.deployment import get_ocp_release_image_from_installer
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    get_random_str,
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
        # By default, IBM cloud has load balancer limit of 50 per region.
        # switch to us-south, if current load balancers are more than 45.
        # https://cloud.ibm.com/docs/vpc?topic=vpc-quotas
        ibmcloud.login()
        current_region = config.ENV_DATA["region"]
        other_region = list(IBM_CLOUD_REGIONS - {current_region})[0]
        if config.ENV_DATA.get("enable_region_dynamic_switching"):
            current_region_lb_count = self.get_load_balancers_count()
            ibmcloud.login(region=other_region)
            other_region_lb_count = self.get_load_balancers_count(other_region)
            if current_region_lb_count > other_region_lb_count:
                logger.info(
                    f"Switching region to {other_region} due to lack of load balancers"
                )
                ibmcloud.set_region(other_region)
        if config.ENV_DATA.get("custom_vpc"):
            self.prepare_custom_vpc_and_network()
        self.ocp_deployment = self.OCPDeployment()
        self.ocp_deployment.deploy_prereq()

        # IBM Cloud specific prereqs
        cco.configure_cloud_credential_operator()
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
        try:
            # Make sure ccoctl is downloaded before using it in destroy job.
            cco.configure_cloud_credential_operator()
            cco.delete_service_id(self.cluster_name, self.credentials_requests_dir)
            if resource_group:
                resource_group = self.get_resource_group()
            # Based on docs:
            # https://docs.openshift.com/container-platform/4.13/installing/installing_ibm_cloud_public/uninstalling-cluster-ibm-cloud.html
            # The volumes should be removed before running openshift-installer for destroy, but it's not
            # working and failing, hence moving this step back after openshift-installer.
            self.delete_volumes(resource_group)
            self.delete_leftover_resources(resource_group)
            self.delete_resource_group(resource_group)
        except Exception as ex:
            logger.error(f"During IBM Cloud cleanup some exception occurred {ex}")
            raise
        finally:
            logger.info("Force cleaning up Service IDs and Account Policies leftovers")
            ibmcloud.cleanup_policies_and_service_ids(self.cluster_name)

    def manually_create_iam_for_vpc(self):
        """
        Manually specify the IAM secrets for the cloud provider
        """
        cco.create_manifests(self.ocp_deployment.installer, self.cluster_path)
        release_image = get_ocp_release_image_from_installer()
        cco.extract_credentials_requests_ibmcloud(
            release_image, self.credentials_requests_dir, self.pull_secret_path
        )
        # get infraID
        infra_id = get_infra_id_from_openshift_install_state(self.cluster_path)

        cco.create_service_id(
            infra_id, self.cluster_path, self.credentials_requests_dir
        )

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
    def export_api_key():
        """
        Exports the IBM CLoud API key as an environment variable.
        """
        logger.info("Exporting IC_API_KEY environment variable")
        api_key = config.AUTH["ibmcloud"]["api_key"]
        os.environ["IC_API_KEY"] = api_key

    def get_load_balancers(self):
        """
        Gets the load balancers

        Returns:
            json: load balancers in json format

        """
        cmd = "ibmcloud is lbs --output json"
        out = exec_cmd(cmd)
        load_balancers = json.loads(out.stdout)
        logger.debug(f"load balancers: {load_balancers}")
        return load_balancers

    def get_load_balancers_count(self, region=None):
        """
        Gets the number of load balancers

        Args:
            region (str): region (e.g. us-south), if not defined it will take from config.
        Return:
            int: number of load balancers

        """
        load_balancers_count = len(self.get_load_balancers())
        if not region:
            region = config.ENV_DATA.get("region")
        logger.info(
            f"Current load balancers count in region {region} is {load_balancers_count}"
        )
        return load_balancers_count

    def prepare_custom_vpc_and_network(self):
        """
        Prepare resource group, VPC, address prefixes, subnets, public gateways
        and attach subnets to public gateways. All for using custom VPC for
        IBM Cloud IPI deployment described here:
        https://docs.openshift.com/container-platform/4.15/installing/installing_ibm_cloud_public/installing-ibm-cloud-vpc.html
        """
        cluster_id = get_random_str(size=5)
        config.ENV_DATA["cluster_id"] = cluster_id
        cluster_name = f"{config.ENV_DATA['cluster_name']}-{cluster_id}"
        resource_group = cluster_name
        vpc_name = f"{cluster_name}-vpc"
        worker_zones = config.ENV_DATA["worker_availability_zones"]
        master_zones = config.ENV_DATA["master_availability_zones"]
        ip_prefix = config.ENV_DATA.get("ip_prefix", 27)
        region = config.ENV_DATA["region"]
        zones = set(worker_zones + master_zones)
        ip_prefixes_and_subnets = {}
        ibmcloud.create_resource_group(resource_group)
        ibmcloud.create_vpc(vpc_name, resource_group)
        ibm_cloud_subnets = constants.IBM_CLOUD_SUBNETS[region]
        for zone in zones:
            ip_prefixes_and_subnets[zone] = ibmcloud.find_free_network_subnets(
                ibm_cloud_subnets[zone], ip_prefix
            )
        for zone, ip_prefix_and_subnets in ip_prefixes_and_subnets.items():
            gateway_name = f"{cluster_name}-public-gateway-{zone}"
            address_prefix, subnet_split1, subnet_split2 = ip_prefix_and_subnets
            ibmcloud.create_address_prefix(
                f"{cluster_name}-{zone}", vpc_name, zone, address_prefix
            )
            ibmcloud.create_public_gateway(gateway_name, vpc_name, zone, resource_group)
            for subnet_type, subnet in (
                ("control-plane", subnet_split1),
                ("compute", subnet_split2),
            ):
                subnet_type_key = f"{subnet_type.replace('-', '_')}_subnets"
                if not config.ENV_DATA.get(subnet_type_key):
                    config.ENV_DATA[subnet_type_key] = []
                subnet_name = f"{cluster_name}-subnet-{subnet_type}-{zone}"
                config.ENV_DATA[subnet_type_key].append(subnet_name)
                ibmcloud.create_subnet(
                    subnet_name, vpc_name, zone, subnet, resource_group
                )
                ibmcloud.attach_subnet_to_public_gateway(
                    subnet_name, gateway_name, vpc_name
                )
