# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on IBM Cloud Platform.
"""

import json
import logging
import os
import time

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
    ResourceNotSupported,
)
from ocs_ci.ocs.resources.backingstore import get_backingstore
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
    get_infra_id,
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
            else:
                ibmcloud.login()
        if config.ENV_DATA.get("custom_vpc"):
            self.prepare_custom_vpc_and_network()
        if config.ENV_DATA.get("existing_vpc"):
            self.prepare_existing_vpc_and_network()
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
        # adding odf-qe security group to the instances
        if config.ENV_DATA.get("existing_vpc"):
            instance_names = self.get_instance_names_by_prefix(
                f"{config.ENV_DATA['cluster_name']}-"
            )
            for instance_name in instance_names:
                self.add_security_group_to_vsi(instance_name)

    def destroy_cluster(self, log_level="DEBUG"):
        """
        Destroy the OCP cluster.

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        self.export_api_key()
        if config.ENV_DATA.get("existing_vpc"):
            logger.info("Destroying the IBM Cloud cluster")
            try:
                prefix = get_infra_id(self.cluster_path)
            except FileNotFoundError:
                prefix = f"{self.cluster_name}-"
            logger.info(
                f"Prefix used for destroy resources from odf-qe-vpc VPC: {prefix}"
            )
            super(IBMCloudIPI, self).destroy_cluster(log_level)
            self.destroy_cluster_from_existing_vpc(prefix)
            logger.info("IBM Cloud cluster destroyed successfully")
            ibmcloud.delete_dns_records(prefix)
            logger.info("DNS records deleted successfully")
        else:
            resource_group = self.get_resource_group()
            if resource_group:
                try:
                    self.delete_bucket()
                    scale_down_pods_and_remove_pvcs(self.storage_class)
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
                ibmcloud.delete_dns_records(self.cluster_name)
            except Exception as ex:
                logger.error(f"During IBM Cloud cleanup some exception occurred {ex}")
                raise
        logger.info("Force cleaning up Service IDs and Account Policies leftovers")
        ibmcloud.cleanup_policies_and_service_ids(self.cluster_name)

    def delete_bucket(self):
        """
        Deletes the COS bucket
        """
        api_key = config.AUTH["ibmcloud"]["api_key"]
        service_instance_id = config.AUTH["ibmcloud"]["cos_instance_crn"]
        endpoint_url = constants.IBM_COS_GEO_ENDPOINT_TEMPLATE.format(
            config.ENV_DATA.get("region", "us-east").lower()
        )
        backingstore = get_backingstore()
        bucket_name = backingstore["spec"]["ibmCos"]["targetBucket"]
        logger.debug(f"bucket name from backingstore: {bucket_name}")
        cos = ibmcloud.IBMCloudObjectStorage(
            api_key=api_key,
            service_instance_id=service_instance_id,
            endpoint_url=endpoint_url,
        )
        cos.delete_bucket(bucket_name=bucket_name)

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
            try:
                proc = exec_cmd(cmd)
            except CommandFailed as ex:
                if "No resource group found" in str(ex):
                    logger.info(f"No resource group: {resource_group} found!")
                    return []

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

    def force_cleanup_leftovers(self, resource_group):
        """
        Extra force cleanup for IBM Cloud leftovers
        that installer + normal ocs-ci cleanup misses.
        Mirrors what the bash script does.
        """
        raised_exceptions = []
        # Delete compute instances
        try:
            cmd = f"ibmcloud is instances --resource-group-name {resource_group} --output json"
            proc = exec_cmd(cmd)
            instances = json.loads(proc.stdout)
            for inst in instances:
                logger.info(f"Deleting instance {inst['name']} ({inst['id']})")
                exec_cmd(f"ibmcloud is instance-delete -f {inst['id']}")
        except Exception as ex:
            logger.warning(f"Instance cleanup failed: {ex}")
            raised_exceptions.append(ex)

        # Delete load balancers (with wait loop)
        try:
            cmd = f"ibmcloud is load-balancers --resource-group-name {resource_group} --output json"
            proc = exec_cmd(cmd)
            lbs = json.loads(proc.stdout)
            for lb in lbs:
                lb_id = lb["id"]
                logger.info(f"Deleting load balancer {lb['name']} ({lb_id})")
                exec_cmd(f"ibmcloud is load-balancer-delete -f {lb_id}")
                # wait until gone or max attempts reached
                max_attempts = 30
                attempts = 0
                while attempts < max_attempts:
                    attempts += 1
                    try:
                        exec_cmd(f"ibmcloud is load-balancer {lb_id} --output json")
                        logger.info(f"Waiting for load balancer {lb_id} deletion...")
                        time.sleep(10)
                    except CommandFailed:
                        break
                if attempts == max_attempts:
                    raise LeftoversExistError(
                        f"Load balancer {lb_id} deletion timed out"
                    )
        except Exception as ex:
            logger.warning(f"Load balancer cleanup failed: {ex}")
            raised_exceptions.append(ex)

        # Delete subnets and detach public gateways
        try:
            cmd = f"ibmcloud is subnets --resource-group-name {resource_group} --output json"
            proc = exec_cmd(cmd)
            subnets = json.loads(proc.stdout)
            for subnet in subnets:
                subnet_id = subnet["id"]
                subnet_name = subnet["name"]
                logger.info(f"Handling subnet {subnet_name} ({subnet_id})")

                pgw = subnet.get("public_gateway", {})
                if pgw and pgw.get("id"):
                    pgw_id = pgw["id"]
                    logger.info(
                        f" Detaching public gateway {pgw_id} from subnet {subnet_id}"
                    )
                    try:
                        exec_cmd(
                            f"ibmcloud is subnet-public-gateway-detach -f {subnet_id}"
                        )
                    except CommandFailed as ex:
                        if "subnet_no_public_gateway" in str(ex):
                            logger.info(" No public gateway attached.")
                        else:
                            raise
                    logger.info(f"Deleting public gateway {pgw_id}")
                    exec_cmd(f"ibmcloud is public-gateway-delete -f {pgw_id}")

                logger.info(f"Deleting subnet {subnet_id}")
                exec_cmd(f"ibmcloud is subnet-delete -f {subnet_id}")
        except Exception as ex:
            logger.warning(f"Subnet/PGW cleanup failed: {ex}")
            raised_exceptions.append(ex)

        # Delete VPN Gateways
        try:
            cmd = f"ibmcloud is vpn-gateways --resource-group-name {resource_group} --output json"
            proc = exec_cmd(cmd)
            vpns = json.loads(proc.stdout)
            for vpn in vpns:
                logger.info(f"Deleting VPN Gateway {vpn['name']} ({vpn['id']})")
                exec_cmd(f"ibmcloud is vpn-gateway-delete -f {vpn['id']}")
        except Exception as ex:
            logger.warning(f"VPN cleanup failed: {ex}")
            raised_exceptions.append(ex)

        # VPC deletion (after cleaning subnets/PGWs)
        try:
            cmd = (
                f"ibmcloud is vpcs --resource-group-name {resource_group} --output json"
            )
            proc = exec_cmd(cmd)
            vpcs = json.loads(proc.stdout)
            for vpc in vpcs:
                logger.info(f"Deleting VPC {vpc['name']} ({vpc['id']})")
                exec_cmd(f"ibmcloud is vpc-delete -f {vpc['id']}")
        except Exception as ex:
            logger.warning(f"VPC cleanup failed: {ex}")
            raised_exceptions.append(ex)
        if raised_exceptions:
            ex_msgs = [str(ex) for ex in raised_exceptions]
            raise LeftoversExistError(f"Leftovers cleanup failed: {ex_msgs}")

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

        def _delete_vpc(resource_name_id_map):
            """
            Delete VPC

            Args:
                resource_name_id_map (dict): Dictionary which contains resource name as key and resource type as value
                   e.g: {'vavuthuibmf1-vsdwj-vpc': 'is.vpc', 'ruse-mustang-unarmored-negate': 'is.security-group'}

            """
            logger.info(f"Deleting VPC. Existing resources: {resource_name_id_map}")
            try:
                for name, res_type in resource_name_id_map.items():
                    if res_type == "is.subnet":
                        cmd = f"ibmcloud is subnet {name} --show-attached --output json"
                        result = exec_cmd(cmd)
                        subnet_resources = json.loads(result.stdout)
                        for lb in subnet_resources.get("load_balancers", []):
                            lb_name = lb["name"]
                            logger.info(
                                f"Deleting Load balancer {lb_name} associated with subnet {name}"
                            )
                            delete_lb_cmd = (
                                f"ibmcloud is load-balancer-delete {lb_name} -f"
                            )
                            exec_cmd(delete_lb_cmd)
                            time.sleep(10)

                        delete_subnet_cmd = f"ibmcloud is subnet-delete {name} -f"
                        exec_cmd(delete_subnet_cmd)

                for name, res_type in resource_name_id_map.items():
                    if res_type == "is.public-gateway":
                        logger.info(f"Deleting public gateway {name}")
                        delete_public_gateway_cmd = (
                            f"ibmcloud is public-gateway-delete {name} -f"
                        )
                        exec_cmd(delete_public_gateway_cmd)

                for name, res_type in resource_name_id_map.items():
                    if res_type == "is.floating-ip":
                        logger.info(f"Deleting floating-ip {name}")
                        delete_floating_ip_cmd = (
                            f"ibmcloud is floating-ip-delete {name} -f"
                        )
                        exec_cmd(delete_floating_ip_cmd)

                for name, res_type in resource_name_id_map.items():
                    if res_type == "is.security-group":
                        if config.ENV_DATA["cluster_name"] in res_type:
                            logger.info(f"Deleting security-group {name}")
                            delete_security_group_cmd = (
                                f"ibmcloud is security-group-delete {name} -f"
                            )
                            exec_cmd(delete_security_group_cmd)

                for name, res_type in resource_name_id_map.items():
                    if res_type == "is.network-acl":
                        if config.ENV_DATA["cluster_name"] in res_type:
                            logger.info(f"Deleting network-acl {name}")
                            delete_network_acl_cmd = (
                                f"ibmcloud is network-acl-delete {name} -f"
                            )
                            exec_cmd(delete_network_acl_cmd)

                for name, res_type in resource_name_id_map.items():
                    if res_type == "is.vpc":
                        logger.info(f"Deleting vpc {name}")
                        delete_network_acl_cmd = f"ibmcloud is vpc-delete {name} -f"
                        exec_cmd(delete_network_acl_cmd)
            except CommandFailed as ex:
                logger.error(f"Failed to delete resource: {ex}")

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
                resource_name_id_map = {}
                is_vpc_exists = False
                for r in leftovers:
                    resource_name_id_map[r["name"]] = r["resource_id"]
                    if r["resource_id"] == "is.vpc":
                        is_vpc_exists = True
                if is_vpc_exists:
                    _delete_vpc(resource_name_id_map)
                    leftovers = _get_resources(resource_group)
                    if leftovers:
                        resource_names = set([r["name"] for r in leftovers])
                        logger.info(f"Deleting leftovers {resource_names}")
                        _delete_resources(resource_names, ignore_errors=True)
                else:
                    resource_names = set([r["name"] for r in leftovers])
                    logger.info(f"Deleting leftovers {resource_names}")
                    _delete_resources(resource_names, ignore_errors=True)
                    self.force_cleanup_leftovers(resource_group)
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

    def prepare_existing_vpc_and_network(self):
        """
        Prepare to use existing VPC, resource group, and subnets for IBM Cloud IPI deployment.
        This function allows you to use your own pre-existing VPC infrastructure.

        Required ENV_DATA configuration:
        - existing_vpc: true
        - resource_group_name: name of existing resource group
        - network_resource_group_name: name of existing network resource group (can be same as resource_group_name)
        - vpc_name: name of existing VPC
        - control_plane_subnets: list of existing control plane subnet names
        - compute_subnets: list of existing compute subnet names
        """
        cluster_id = get_random_str(size=5)
        config.ENV_DATA["cluster_id"] = cluster_id

        # Use existing resource group and VPC names from config
        resource_group = config.ENV_DATA.get("resource_group_name")
        network_resource_group = config.ENV_DATA.get(
            "network_resource_group_name", resource_group
        )
        vpc_name = config.ENV_DATA.get("vpc_name")

        # Validate required configuration
        if not resource_group:
            raise ValueError(
                "resource_group_name must be specified in ENV_DATA when using existing VPC"
            )
        if not vpc_name:
            raise ValueError(
                "vpc_name must be specified in ENV_DATA when using existing VPC"
            )

        # Get existing subnet names from config
        control_plane_subnets = config.ENV_DATA.get("control_plane_subnets", [])
        compute_subnets = config.ENV_DATA.get("compute_subnets", [])

        if not control_plane_subnets:
            raise ValueError(
                "control_plane_subnets must be specified in ENV_DATA when using existing VPC"
            )
        if not compute_subnets:
            raise ValueError(
                "compute_subnets must be specified in ENV_DATA when using existing VPC"
            )

        logger.info(f"Using existing VPC: {vpc_name}")
        logger.info(f"Using existing resource group: {resource_group}")
        logger.info(f"Using existing network resource group: {network_resource_group}")
        logger.info(f"Using existing control plane subnets: {control_plane_subnets}")
        logger.info(f"Using existing compute subnets: {compute_subnets}")

    def get_instance_names_by_prefix(self, prefix):
        """
        Get all instance names for instances whose names start with the given prefix.

        Args:
            prefix (str): The prefix to match instance names against

        Returns:
            list: List of instance names that match the prefix, empty list if none found
        """
        try:
            logger.info(f"Fetching instances with prefix: {prefix}")
            cmd = "ibmcloud is instances --output json"
            proc = exec_cmd(cmd)
            instances = json.loads(proc.stdout)

            # Filter instances by name prefix
            matching_instances = [
                inst for inst in instances if inst.get("name", "").startswith(prefix)
            ]

            if not matching_instances:
                logger.info(f"No instances found with prefix '{prefix}'")
                return []

            instance_names = [inst["name"] for inst in matching_instances]

            logger.info(
                f"Found {len(instance_names)} instance(s) with prefix '{prefix}': "
                f"{', '.join(instance_names)}"
            )

            return instance_names

        except Exception as e:
            logger.error(f"Failed to retrieve instances by prefix '{prefix}': {e}")
            return []

    def add_security_group_to_vsi(self, instance_name, security_group_name=None):
        """
        Add a security group to a VSI's network interface using security-group-target-add command.
        This is safer than update command as it doesn't require listing all existing security groups.

        Args:
            instance_name (str): The VSI instance name
            security_group_name (str): Name of the security group to add (if None, it will be fetched from ENV_DATA)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get security group name from config if not provided
            if security_group_name is None:
                security_group_name = config.ENV_DATA.get("security_group_name")
                if not security_group_name:
                    logger.error(
                        "Security group name not provided and not found in ENV_DATA. "
                        "Please provide security_group_name parameter or set it in ENV_DATA."
                    )
                    return False

            # Get instance details to retrieve VPC name
            logger.info(f"Getting instance details for instance: {instance_name}")
            cmd = f"ibmcloud is instance {instance_name} --output json"
            proc = exec_cmd(cmd)
            instance_data = json.loads(proc.stdout)

            # Get VPC name from instance data or config
            vpc_data = instance_data.get("vpc")
            if vpc_data:
                vpc_name = vpc_data.get("name")
            else:
                # Fallback to config if VPC name not in instance data
                vpc_name = config.ENV_DATA.get("vpc_name")
                if not vpc_name:
                    logger.error(
                        f"Could not determine VPC name for instance {instance_name}. "
                        "Please ensure vpc_name is set in ENV_DATA."
                    )
                    return False

            logger.info(
                f"Instance name: {instance_name}, VPC name: {vpc_name}, Security group: {security_group_name}"
            )

            # Get the network interfaces for the instance
            logger.info(f"Getting network interfaces for instance: {instance_name}")
            cmd = (
                f"ibmcloud is instance-network-interfaces {instance_name} --output json"
            )
            proc = exec_cmd(cmd)
            network_interfaces = json.loads(proc.stdout)

            if not network_interfaces:
                logger.error(
                    f"No network interfaces found for instance: {instance_name}"
                )
                return False

            # Get the primary network interface (usually the first one)
            primary_nic = network_interfaces[0]
            nic_name = primary_nic.get("name")
            nic_id = primary_nic.get("id")

            if not nic_name:
                logger.error(
                    f"Could not retrieve network interface name for instance {instance_name}. "
                    f"Network interface ID: {nic_id}"
                )
                return False

            logger.info(f"Using network interface: {nic_name} (ID: {nic_id})")

            # Check if the security group is already attached
            existing_sgs = primary_nic.get("security_groups", [])
            existing_sg_names = [
                sg.get("name") for sg in existing_sgs if sg.get("name")
            ]
            if security_group_name in existing_sg_names:
                logger.info(
                    f"Security group '{security_group_name}' is already attached to "
                    f"instance {instance_name} network interface {nic_name}"
                )
                return True

            # Use the new security-group-target-add command
            logger.info(
                f"Adding security group '{security_group_name}' to instance {instance_name} "
                f"network interface {nic_name} in VPC {vpc_name}"
            )
            cmd = (
                f"ibmcloud is security-group-target-add {security_group_name} {nic_name} "
                f"--vpc {vpc_name} --in {instance_name}"
            )
            exec_cmd(cmd)
            logger.info(
                f"Successfully added security group '{security_group_name}' to instance "
                f"{instance_name} network interface {nic_name}"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to add security group to VSI: {e}")
            return False

    def _wait_for_resource_deletion(
        self, resource_type, resource_id, max_attempts=30, wait_time=5
    ):
        """
        Wait for resource deletion with retry.

        Args:
            resource_type (str): The type of resource to check for deletion
                (e.g., "instance", "load-balancer", "volume", etc.).
            resource_id (str): The unique identifier of the resource to check.
            max_attempts (int, optional): Maximum number of attempts to check for deletion. Default is 30.
            wait_time (int, optional): Time in seconds to wait between checks. Default is 5.

        Returns:
            bool: True if the resource is deleted within the specified attempts, False otherwise.
        """
        for attempt in range(1, max_attempts + 1):
            try:
                if resource_type == "instance":
                    cmd = f"ibmcloud is instance {resource_id} --output json"
                elif resource_type == "load-balancer":
                    cmd = f"ibmcloud is load-balancer {resource_id} --output json"
                elif resource_type == "volume":
                    cmd = f"ibmcloud is volume {resource_id} --output json"
                elif resource_type == "security-group":
                    cmd = f"ibmcloud is security-group {resource_id} --output json"
                elif resource_type == "floating-ip":
                    cmd = f"ibmcloud is floating-ip {resource_id} --output json"
                else:
                    raise ResourceNotSupported(
                        f"Resource type {resource_type} is not supported"
                    )
                exec_cmd(cmd)
            except CommandFailed:
                return True
            if attempt < max_attempts:
                time.sleep(wait_time)
        logger.warning(
            f"Timeout waiting for {resource_type} {resource_id} to be deleted"
        )
        return False

    def delete_vsis(self, prefix):
        """
        Delete Virtual Server Instances matching the provided prefix.

        Args:
            prefix (str): The prefix string to match VSI names.

        Returns:
            tuple: A tuple containing the count of matched VSIs and the number of errors occurred during
            deletion. Errors are raised as CommandFailed exceptions.
        """
        logger.info("Discovering Virtual Server Instances...")
        errors = 0
        count = 0
        try:
            cmd = "ibmcloud is instances --output json"
            proc = exec_cmd(cmd)
            instances = json.loads(proc.stdout)
            vsi_list = [
                inst for inst in instances if inst.get("name", "").startswith(prefix)
            ]
            count = len(vsi_list)

            if count > 0:
                logger.info(f"Found {count} Virtual Server Instance(s)")
                for inst in vsi_list:
                    logger.info(
                        f"Name: {inst.get('name')}, ID: {inst.get('id')}, \
                        VPC: {inst.get('vpc', {}).get('name', 'N/A')}"
                    )

                logger.info("Deleting Virtual Server Instances...")
                vsi_ids = []
                for inst in vsi_list:
                    inst_name = inst.get("name")
                    inst_id = inst.get("id")
                    logger.info(f"Deleting VSI '{inst_name}'...")
                    cmd = f"ibmcloud is instance-delete {inst_id} --force"
                    try:
                        retry(CommandFailed, tries=3, delay=5, backoff=2)(exec_cmd)(cmd)
                        logger.info(f"Successfully deleted VSI '{inst_name}'")
                        vsi_ids.append(inst_id)
                    except CommandFailed:
                        logger.error(f"Failed to delete VSI '{inst_name}'")
                        errors += 1

                # Wait for VSIs to be fully deleted
                if vsi_ids:
                    logger.info("Waiting for VSIs to fully terminate...")
                    for vsi_id in vsi_ids:
                        self._wait_for_resource_deletion("instance", vsi_id, 40, 5)
                    logger.info(
                        "Waiting for network interfaces to be fully released..."
                    )
                    time.sleep(15)
            else:
                logger.info("No Virtual Server Instances found")
        except Exception as e:
            logger.error(f"Error processing VSIs: {e}")
            errors += 1
        return count, errors

    def delete_floating_ips(self, prefix):
        """
        Delete Floating IPs matching the provided prefix.

        Args:
            prefix (str): The prefix string to match floating IP names.

        Returns:
            tuple: A tuple containing the count of matched floating IPs and the number of errors occurred during
            deletion. Errors are raised as CommandFailed exceptions.
        """
        logger.info("Discovering Floating IPs...")
        errors = 0
        count = 0
        try:
            cmd = "ibmcloud is floating-ips --output json"
            proc = exec_cmd(cmd)
            floating_ips = json.loads(proc.stdout)
            fip_list = [
                fip for fip in floating_ips if fip.get("name", "").startswith(prefix)
            ]
            count = len(fip_list)

            if count > 0:
                logger.info(f"Found {count} Floating IP(s)")
                for fip in fip_list:
                    target_name = (
                        fip.get("target", {}).get("name", "unbound")
                        if fip.get("target")
                        else "unbound"
                    )
                    logger.info(
                        f"Name: {fip.get('name')}, ID: {fip.get('id')}, \
                        Address: {fip.get('address')}, Target: {target_name}"
                    )

                logger.info("Deleting Floating IPs...")
                for fip in fip_list:
                    fip_name = fip.get("name")
                    fip_id = fip.get("id")
                    logger.info(f"Deleting Floating IP '{fip_name}'...")

                    # Special handling for Floating IPs - they often need multiple attempts
                    deleted = False
                    for attempt in range(1, 9):
                        try:
                            cmd = f"ibmcloud is floating-ip-release {fip_id} --force"
                            exec_cmd(cmd)
                            deleted = True
                            break
                        except CommandFailed:
                            # Check if it still exists
                            try:
                                cmd = f"ibmcloud is floating-ip {fip_id} --output json"
                                exec_cmd(cmd)
                            except CommandFailed:
                                deleted = True
                                break
                            if attempt < 8:
                                wait_time = 5 * attempt
                                time.sleep(wait_time)

                    if deleted:
                        logger.info(f"Successfully deleted Floating IP '{fip_name}'")
                    else:
                        logger.warning(
                            f"Floating IP '{fip_name}' may still be attached or in transition state"
                        )
                        errors += 1
                time.sleep(5)
            else:
                logger.info("No Floating IPs found")
        except Exception as e:
            logger.error(f"Error processing Floating IPs: {e}")
            errors += 1
        return count, errors

    def delete_load_balancers(self, prefix):
        """
        Delete Load Balancers matching the provided prefix.

        Args:
            prefix (str): The prefix string to match load balancer names.

        Returns:
            tuple: A tuple containing the count of matched load balancers and the number of errors occurred during
            deletion. Errors are raised as CommandFailed exceptions.
        """
        logger.info("Discovering Load Balancers...")
        errors = 0
        count = 0
        try:
            cmd = "ibmcloud is load-balancers --output json"
            proc = exec_cmd(cmd)
            load_balancers = json.loads(proc.stdout)
            lb_list = [
                lb for lb in load_balancers if lb.get("name", "").startswith(prefix)
            ]
            count = len(lb_list)

            if count > 0:
                logger.info(f"Found {count} Load Balancer(s)")
                for lb in lb_list:
                    logger.info(
                        f"Name: {lb.get('name')}, ID: {lb.get('id')}, \
                        Hostname: {lb.get('hostname')}, Status: {lb.get('provisioning_status')}"
                    )

                logger.info("Deleting Load Balancers...")
                lb_ids = []
                for lb in lb_list:
                    lb_name = lb.get("name")
                    lb_id = lb.get("id")
                    logger.info(f"Deleting Load Balancer '{lb_name}'...")
                    cmd = f"ibmcloud is load-balancer-delete {lb_id} --force"
                    try:
                        retry(CommandFailed, tries=3, delay=5, backoff=2)(exec_cmd)(cmd)
                        logger.info(f"Successfully deleted Load Balancer '{lb_name}'")
                        lb_ids.append(lb_id)
                    except CommandFailed:
                        logger.error(f"Failed to delete Load Balancer '{lb_name}'")
                        errors += 1

                # Wait for Load Balancers to be fully deleted
                if lb_ids:
                    logger.info(
                        "Waiting for Load Balancers to fully delete (this may take a while)..."
                    )
                    for lb_id in lb_ids:
                        self._wait_for_resource_deletion("load-balancer", lb_id, 60, 10)
            else:
                logger.info("No Load Balancers found")
        except Exception as e:
            logger.error(f"Error processing Load Balancers: {e}")
            errors += 1
        return count, errors

    def delete_security_groups(self, prefix):
        """
        Delete Security Groups matching the provided prefix.

        Args:
            prefix (str): The prefix string to match security group names.

        Returns:
            tuple: A tuple containing the count of matched security groups and the number of errors occurred during
            deletion. Errors are raised as CommandFailed exceptions.
        """
        logger.info("Discovering Security Groups...")
        errors = 0
        count = 0
        try:
            cmd = "ibmcloud is security-groups --output json"
            proc = exec_cmd(cmd)
            security_groups = json.loads(proc.stdout)
            sg_list = [
                sg for sg in security_groups if sg.get("name", "").startswith(prefix)
            ]
            count = len(sg_list)

            if count > 0:
                logger.info(f"Found {count} Security Group(s)")
                for sg in sg_list:
                    logger.info(
                        f"Name: {sg.get('name')}, ID: {sg.get('id')}, \
                        VPC: {sg.get('vpc', {}).get('name', 'N/A')}"
                    )

                logger.info("Deleting Security Groups...")
                # Get all security groups for reference checking
                all_sgs = security_groups

                for sg in sg_list:
                    sg_name = sg.get("name")
                    sg_id = sg.get("id")
                    logger.info(f"Processing Security Group '{sg_name}'...")

                    # Find and remove rules that reference this SG from other SGs
                    for ref_sg in all_sgs:
                        rules = ref_sg.get("rules", [])
                        for rule in rules:
                            remote = rule.get("remote", {})
                            if remote.get("id") == sg_id:
                                rule_id = rule.get("id")
                                if rule_id:
                                    try:
                                        cmd = (
                                            f"ibmcloud is security-group-rule-delete "
                                            f"{ref_sg.get('id')} {rule_id} --force"
                                        )
                                        exec_cmd(cmd)
                                    except CommandFailed:
                                        pass
                    time.sleep(2)

                    # Now delete the security group
                    cmd = f"ibmcloud is security-group-delete {sg_id} --force"
                    try:
                        retry(CommandFailed, tries=3, delay=5, backoff=2)(exec_cmd)(cmd)
                        logger.info(f"Successfully deleted Security Group '{sg_name}'")
                    except CommandFailed:
                        logger.error(f"Failed to delete Security Group '{sg_name}'")
                        errors += 1
                time.sleep(5)
            else:
                logger.info("No Security Groups found")
        except Exception as e:
            logger.error(f"Error processing Security Groups: {e}")
            errors += 1
        return count, errors

    def delete_cos_instances(self, prefix):
        """
        Delete COS Instances matching the provided prefix.

        Args:
            prefix (str): The prefix string to match instance names.

        Returns:
            tuple: A tuple containing the count of matched COS instances and the number of errors occurred during
            deletion. Errors are raised as CommandFailed exceptions.
        """
        logger.info("Discovering Cloud Object Storage Instances...")
        errors = 0
        count = 0
        try:
            cmd = "ibmcloud resource service-instances --service-name cloud-object-storage --output json"
            proc = exec_cmd(cmd)
            cos_instances = json.loads(proc.stdout)
            cos_list = [
                cos_inst
                for cos_inst in cos_instances
                if cos_inst.get("name", "").startswith(prefix)
            ]
            count = len(cos_list)

            if count > 0:
                logger.info(f"Found {count} COS Instance(s)")
                for cos_inst in cos_list:
                    logger.info(
                        f"Name: {cos_inst.get('name')}, ID: {cos_inst.get('id')}, \
                        GUID: {cos_inst.get('guid')}"
                    )

                logger.info("Deleting COS Instances...")
                for cos_inst in cos_list:
                    cos_name = cos_inst.get("name")
                    cos_guid = cos_inst.get("guid")
                    logger.info(f"Deleting COS Instance '{cos_name}'...")
                    cmd = f"ibmcloud resource service-instance-delete {cos_guid} --force --recursive"
                    try:
                        retry(CommandFailed, tries=3, delay=5, backoff=2)(exec_cmd)(cmd)
                        logger.info(f"Successfully deleted COS Instance '{cos_name}'")
                    except CommandFailed:
                        logger.error(f"Failed to delete COS Instance '{cos_name}'")
                        errors += 1
                time.sleep(5)
            else:
                logger.info("No COS Instances found")
        except Exception as e:
            logger.error(f"Error processing COS Instances: {e}")
            errors += 1
        return count, errors

    def delete_custom_images(self, prefix):
        """
        Delete Custom Images matching the provided prefix.

        Args:
            prefix (str): The prefix string to match image names.

        Returns:
            tuple: A tuple containing the count of matched custom images and the number of errors occurred during
            deletion. Errors are raised as CommandFailed exceptions.
        """
        logger.info("Discovering Custom Images...")
        errors = 0
        count = 0
        try:
            cmd = "ibmcloud is images --output json"
            proc = exec_cmd(cmd)
            images = json.loads(proc.stdout)
            image_list = [
                img
                for img in images
                if img.get("name", "").startswith(prefix)
                and img.get("visibility") == "private"
            ]
            count = len(image_list)

            if count > 0:
                logger.info(f"Found {count} Custom Image(s)")
                for img in image_list:
                    logger.info(
                        f"Name: {img.get('name')}, ID: {img.get('id')}, \
                        Status: {img.get('status')}"
                    )

                logger.info("Deleting Custom Images...")
                for img in image_list:
                    img_name = img.get("name")
                    img_id = img.get("id")
                    logger.info(f"Deleting Image '{img_name}'...")
                    cmd = f"ibmcloud is image-delete {img_id} --force"
                    try:
                        retry(CommandFailed, tries=3, delay=5, backoff=2)(exec_cmd)(cmd)
                        logger.info(f"Successfully deleted Image '{img_name}'")
                    except CommandFailed:
                        logger.error(f"Failed to delete Image '{img_name}'")
                        errors += 1
            else:
                logger.info("No Custom Images found")
        except Exception as e:
            logger.error(f"Error processing Custom Images: {e}")
            errors += 1
        return count, errors

    def destroy_cluster_from_existing_vpc(self, prefix):
        """
        Destroy the OCP cluster from existing VPC infrastructure on IBM Cloud.
        This function will destroy the cluster and the following resources:
        - Virtual Server Instances (VSIs)
        - Floating IPs
        - Load Balancers
        - Security Groups
        - Custom Images
        - Volumes (Block Storage)
        - Cloud Object Storage

        prefix: the prefix of the cluster (can be obtained from metadata infraID or cluster_name-)

        raises LeftoversExistError if any errors occur during deletion.
        """
        if not prefix:
            logger.error("cluster_name not found in ENV_DATA")
            return

        # Delete each resource type (VSIs first as they may have dependencies)
        try:
            vsi_count, vsi_errors = self.delete_vsis(prefix)
        except Exception as e:
            logger.error(f"Error deleting Virtual Server Instances: {e}")

        try:
            fip_count, fip_errors = self.delete_floating_ips(prefix)
        except Exception as e:
            logger.error(f"Error deleting Floating IPs: {e}")

        try:
            lb_count, lb_errors = self.delete_load_balancers(prefix)
        except Exception as e:
            logger.error(f"Error deleting Load Balancers: {e}")

        try:
            sg_count, sg_errors = self.delete_security_groups(prefix)
        except Exception as e:
            logger.error(f"Error deleting Security Groups: {e}")

        try:
            cos_count, cos_errors = self.delete_cos_instances(prefix)
        except Exception as e:
            logger.error(f"Error deleting COS Instances: {e}")

        try:
            image_count, image_errors = self.delete_custom_images(prefix)
        except Exception as e:
            logger.error(f"Error deleting Custom Images: {e}")

        total_errors = (
            vsi_errors + fip_errors + lb_errors + sg_errors + cos_errors + image_errors
        )
        total_resources = (
            vsi_count + fip_count + lb_count + sg_count + cos_count + image_count
        )

        if total_errors > 0:
            raise LeftoversExistError(f"Total errors: {total_errors}")

        logger.info(f"Successfully deleted {total_resources} resources")
