# -*- coding: utf8 -*-
"""
This module contains platform specific methods and classes for deployment
on Openshfit Dedicated Platform.
"""


import logging
import os

from ocs_ci.deployment.cloud import CloudDeploymentBase
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.utility import openshift_dedicated as ocm, rosa
from ocs_ci.utility.aws import AWS as AWSUtil
from ocs_ci.utility.utils import ceph_health_check, get_ocp_version
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.managedservice import update_pull_secret, patch_consumer_toolbox
from ocs_ci.ocs.resources import pvc

logger = logging.getLogger(name=__file__)


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
            log_cli_level (str): openshift installer's log level

        """
        rosa.create_cluster(self.cluster_name, self.ocp_version, self.region)
        kubeconfig_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["kubeconfig_location"]
        )
        ocm.get_kubeconfig(self.cluster_name, kubeconfig_path)
        password_path = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN["password_location"]
        )
        ocm.get_kubeadmin_password(self.cluster_name, password_path)
        self.test_cluster()

    def destroy(self, log_level="DEBUG"):
        """
        Destroy OCP cluster specific

        Args:
            log_level (str): log level openshift-installer (default: DEBUG)

        """
        ocm.destroy_cluster(self.cluster_name)
        # TODO: investigate why steps below fail despite being in accordance with
        # https://docs.openshift.com/rosa/rosa_getting_started_sts/rosa-sts-deleting-cluster.html
        # cluster_details = ocm.get_cluster_details(self.cluster_name)
        # cluster_id = cluster_details.get("id")
        # rosa.delete_operator_roles(cluster_id)
        # rosa.delete_oidc_provider(cluster_id)


class ROSA(CloudDeploymentBase):
    """
    Deployment class for ROSA.
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
        Check cluster existence based on a cluster name.

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
                selector="app=rook-ceph-mon",
                resource_count=3,
                timeout=600,
            )
            assert pod.wait_for_resource(
                condition="Running", selector="app=rook-ceph-mgr", timeout=600
            )
            assert pod.wait_for_resource(
                condition="Running",
                selector="app=rook-ceph-osd",
                resource_count=3,
                timeout=600,
            )

        if config.DEPLOYMENT.get("pullsecret_workaround"):
            update_pull_secret()
        if config.ENV_DATA.get("cluster_type") == "consumer":
            patch_consumer_toolbox()

        # Verify health of ceph cluster
        ceph_health_check(namespace=self.namespace, tries=60, delay=10)

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
        sg_id = security_groups[0]["GroupId"]
        security_group = self.aws.ec2_resource.SecurityGroup(sg_id)
        # The ports are not 100 % clear yet. Taken from doc:
        # https://docs.google.com/document/d/1RM8tmMbvnJcOZFdsqbCl9RvHXBv5K2ZI6ziQ-YTloGk/edit#
        security_group.authorize_ingress(
            DryRun=False,
            IpPermissions=[
                {
                    "FromPort": 6800,
                    "ToPort": 7300,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {"CidrIp": "10.0.0.0/16", "Description": "Ceph OSDs"},
                    ],
                },
                {
                    "FromPort": 3300,
                    "ToPort": 3300,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {"CidrIp": "10.0.0.0/16", "Description": "Ceph MONs rule1"}
                    ],
                },
                {
                    "FromPort": 6789,
                    "ToPort": 6789,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {"CidrIp": "10.0.0.0/16", "Description": "Ceph MONs rule2"},
                    ],
                },
                {
                    "FromPort": 9283,
                    "ToPort": 9283,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {"CidrIp": "10.0.0.0/16", "Description": "Ceph Manager"},
                    ],
                },
                {
                    "FromPort": 31659,
                    "ToPort": 31659,
                    "IpProtocol": "tcp",
                    "IpRanges": [
                        {"CidrIp": "10.0.0.0/16", "Description": "API Server"},
                    ],
                },
            ],
        )
