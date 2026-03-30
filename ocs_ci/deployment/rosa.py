# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Openshfit Dedicated Platform.
"""

import logging
import os

from botocore.exceptions import ClientError

from ocs_ci.ocs.machinepool import MachinePool
from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.helpers.rosa_cluster_helpers import (
    ROSAProdEnvCluster,
    ROSAStageEnvCluster,
)
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.ocs.resources.pod import get_operator_pods
from ocs_ci.utility import openshift_dedicated as ocm, rosa
from ocs_ci.utility.aws import AWS as AWSUtil, delete_sts_iam_roles, delete_subnet_tags
from ocs_ci.utility.deployment import create_openshift_install_log_file
from ocs_ci.utility.rosa import (
    get_associated_oidc_config_id,
    delete_account_roles,
    wait_console_url,
    destroy_rosa_cluster,
)
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

logger = logging.getLogger(__name__)


class ROSAOCP(BaseOCPDeployment):
    """
    ROSA deployment class.
    """

    def __init__(self):
        super(ROSAOCP, self).__init__()
        self.ocp_version = get_ocp_version()
        self.region = config.ENV_DATA["region"]

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
            "CLUSTER_NAME": config.ENV_DATA["cluster_name"],
            "OCM_TOKEN": openshiftdedicated["token"],
        }
        for key, value in env_vars.items():
            if value:
                os.environ[key] = str(value)

    def deploy(self, log_level=""):
        """
        Deployment specific to OCP cluster on a ROSA Managed Service platform.

        Args:
            log_level (str): openshift installer's log level

        """
        # AWS does not guarantee machines provisioning time,
        # we need to wait up to 3 hours (observed time)
        wait_replicas_ready_timeout = 60 * 60 * 3
        if (
            config.ENV_DATA.get("appliance_mode", False)
            and config.ENV_DATA.get("cluster_type", "") == "provider"
        ):
            rosa.appliance_mode_cluster(self.cluster_name)
        else:
            rosa.login()
            rosa.create_cluster(self.cluster_name, self.ocp_version, self.region)
            machinepool_details = MachinePool.get_machinepool_details(
                self.cluster_name, config.ENV_DATA["machine_pool"]
            )
            machinepool_details.wait_replicas_ready(
                target_replicas=config.ENV_DATA["worker_replicas"],
                timeout=wait_replicas_ready_timeout,
            )
            if node_labels := config.ENV_DATA.get("node_labels"):
                if machinepool_id := config.ENV_DATA.get("machine_pool"):
                    rosa.label_nodes(
                        self.cluster_name, machinepool_id, node_labels, rewrite=False
                    )

        logger.info("generate kubeconfig and kubeadmin-password files")
        if config.ENV_DATA["ms_env_type"] == "staging":
            cluster_path = config.ENV_DATA["cluster_path"]
            kubeconfig_path = os.path.join(
                cluster_path, config.RUN["kubeconfig_location"]
            )
            ocm.get_kubeconfig(self.cluster_name, kubeconfig_path)
            # this default admin password from secret doesn't work for ROSA HCP staging in the management-console
            # but kubeconfig works for CLI operations, creating kubeadmin-password file for CLI operations via rosa cli
            rosa_stage_cluster = ROSAStageEnvCluster(self.cluster_name)
            rosa_stage_cluster.create_admin_and_login()
            rosa_stage_cluster.generate_kubeadmin_password_file()
            console_url = wait_console_url(self.cluster_name)
            create_openshift_install_log_file(cluster_path, console_url)
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

    def destroy(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        try:
            aws = AWSUtil()
            rosa_hcp = config.ENV_DATA.get("platform") == constants.ROSA_HCP_PLATFORM
            oidc_config_id = (
                get_associated_oidc_config_id(self.cluster_name) if rosa_hcp else None
            )
            try:
                cluster_id = ocm.get_cluster_details(self.cluster_name).get("id")
            except CommandFailed as e:
                if "Cluster was Deprovisioned" in str(e):
                    logger.warning(
                        f"Cluster {self.cluster_name} is already deprovisioned, {e}"
                    )
                    return
                raise
            subnet_ids = aws.get_cluster_subnet_ids(cluster_name=self.cluster_name)
            log_step(f"Destroying ROSA cluster. Hosted CP: {rosa_hcp}")
            delete_status = rosa.destroy_appliance_mode_cluster(self.cluster_name)
            if not delete_status:
                if rosa_hcp:
                    destroy_rosa_cluster(self.cluster_name)
                else:
                    ocm.destroy_cluster(self.cluster_name)
            log_step("Waiting for ROSA cluster to be uninstalled")
            sample = TimeoutSampler(
                timeout=14400,
                sleep=300,
                func=self.cluster_present,
            )
            if not sample.wait_for_func_status(result=False):
                err_msg = f"Failed to delete {self.cluster_name}"
                logger.error(err_msg)
                raise TimeoutExpiredError(err_msg)
            log_step("Deleting ROSA/aws associated resources")
            oproles_prefix = (
                f"{constants.OPERATOR_ROLE_PREFIX_ROSA_HCP}-{self.cluster_name}"
            )
            rosa.delete_operator_roles(prefix=oproles_prefix)
            if rosa_hcp:
                if oidc_config_id:
                    rosa.delete_oidc_config(oidc_config_id)
                # use sts IAM roles for ROSA HCP is mandatory
                delete_sts_iam_roles()
                delete_subnet_tags(f"kubernetes.io/cluster/{cluster_id}", subnet_ids)
            rosa.delete_oidc_provider(self.cluster_name)
            account_roles_prefix = (
                f"{constants.ACCOUNT_ROLE_PREFIX_ROSA_HCP}-{self.cluster_name}"
            )
            delete_account_roles(account_roles_prefix)
            logger.info(
                f"Cluster {self.cluster_name} and associated resources deleted successfully"
            )
        except CommandFailed as err:
            if "There are no subscriptions or clusters with identifier or name" in str(
                err
            ):
                logger.info(
                    f"Cluster {self.cluster_name} doesn't exists, no other action is required."
                )
            else:
                raise

    def cluster_present(self):
        """
        Check if the cluster is present in the cluster list, regardless of its state.

        Returns:
            bool: True if a cluster with the given name exists, False otherwise

        """
        cluster_list = ocm.list_cluster()
        for cluster in cluster_list:
            if cluster[0] == self.cluster_name:
                logger.info(f"Cluster found: {cluster[0]}")
                return True
        return False


class ROSA(CloudDeploymentBase):
    """
    Deployment class for ROSA and ROSA HCP.
    """

    OCPDeployment = ROSAOCP

    def __init__(self):
        self.name = self.__class__.__name__
        super(ROSA, self).__init__()
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
        super(ROSA, self).deploy_ocp(log_cli_level)
        if config.DEPLOYMENT.get("host_network"):
            self.host_network_update()

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence based on a cluster name. Cluster in Uninstalling
        phase is not considered to be existing

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
        Deployment of ODF Managed Service addon on ROSA or ODF operator on ROSA HCP.
        """
        rosa_hcp = config.ENV_DATA.get("platform") == constants.ROSA_HCP_PLATFORM
        ceph_cluster = ocp.OCP(kind="CephCluster", namespace=self.namespace)
        try:
            ceph_cluster.get().get("items")[0]
            logger.warning("OCS cluster already exists")
            return
        except (IndexError, CommandFailed):
            msg = "Running OCS basic installation"
            msg += " with ODF addon" if not rosa_hcp else " with ODF operator"
            logger.info(msg)

        # rosa hcp is self-managed and doesn't support ODF addon
        if rosa_hcp:
            super(ROSA, self).deploy_ocs()
        else:
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
                namespace=config.ENV_DATA["cluster_namespace"],
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
                        namespace=config.ENV_DATA["cluster_namespace"],
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
        rosa.delete_odf_addon(self.cluster_name)

    def host_network_update(self):
        """
        Update security group rules for HostNetwork
        """
        infrastructure_id = ocp.OCP().exec_oc_cmd(
            "get -o jsonpath='{.status.infrastructureName}{\"\\n\"}' infrastructure cluster"
        )
        worker_pattern = f"{infrastructure_id}-worker*"
        worker_instances = self.aws.get_instances_by_name_pattern(worker_pattern)
        security_groups = worker_instances[0]["security_groups"]
        sg_id = None
        for security_group in security_groups:
            if "terraform" in security_group["GroupName"]:
                sg_id = security_group["GroupId"]
                break
        if not sg_id:
            raise ManagedServiceSecurityGroupNotFound

        security_group = self.aws.ec2_resource.SecurityGroup(sg_id)
        # The ports are not 100 % clear yet. Taken from doc:
        # https://docs.google.com/document/d/1RM8tmMbvnJcOZFdsqbCl9RvHXBv5K2ZI6ziQ-YTloGk/edit#
        machine_cidr = config.ENV_DATA.get("machine_cidr", "10.0.0.0/16")
        rules = [
            {
                "FromPort": 6800,
                "ToPort": 7300,
                "IpProtocol": "tcp",
                "IpRanges": [
                    {"CidrIp": machine_cidr, "Description": "Ceph OSDs"},
                ],
            },
            {
                "FromPort": 3300,
                "ToPort": 3300,
                "IpProtocol": "tcp",
                "IpRanges": [
                    {"CidrIp": machine_cidr, "Description": "Ceph MONs rule1"}
                ],
            },
            {
                "FromPort": 6789,
                "ToPort": 6789,
                "IpProtocol": "tcp",
                "IpRanges": [
                    {"CidrIp": machine_cidr, "Description": "Ceph MONs rule2"},
                ],
            },
            {
                "FromPort": 9283,
                "ToPort": 9283,
                "IpProtocol": "tcp",
                "IpRanges": [
                    {"CidrIp": machine_cidr, "Description": "Ceph Manager"},
                ],
            },
            {
                "FromPort": 31659,
                "ToPort": 31659,
                "IpProtocol": "tcp",
                "IpRanges": [
                    {"CidrIp": machine_cidr, "Description": "API Server"},
                ],
            },
        ]
        for rule in rules:
            try:
                security_group.authorize_ingress(
                    DryRun=False,
                    IpPermissions=[rule],
                )
            except ClientError as err:
                if (
                    err.response.get("Error", {}).get("Code")
                    == "InvalidPermission.Duplicate"
                ):
                    logger.debug(
                        f"Security group '{sg_id}' already contains the required rule "
                        f"({err.response.get('Error', {}).get('Message')})."
                    )
                else:
                    raise
