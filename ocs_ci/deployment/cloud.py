# -*- coding: utf8 -*-
"""
This module contains common code and a base class for any cloud platform
deployment.
"""

import json
import logging
import os
import subprocess

from ocs_ci.ocs import ocp
from ocs_ci.deployment.deployment import Deployment
from ocs_ci.deployment.ocp import OCPDeployment as BaseOCPDeployment
from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions
from ocs_ci.utility.bootstrap import gather_bootstrap
from ocs_ci.utility.deployment import get_cluster_prefix
from ocs_ci.utility.ibmcloud import (
    run_ibmcloud_cmd,
    set_target_region,
    configure_ingress_load_balancer_security_group,
)
from ocs_ci.utility.utils import (
    get_cluster_name,
    get_infra_id,
    get_infra_id_from_openshift_install_state,
    run_cmd,
    TimeoutSampler,
)

logger = logging.getLogger(__name__)


class CloudDeploymentBase(Deployment):
    """
    Base class for deployment on a cloud platform (such as AWS, Azure, ...).
    """

    def __init__(self):
        """
        Any cloud platform deployment requires region and cluster name.
        """
        super(CloudDeploymentBase, self).__init__()
        self.region = config.ENV_DATA["region"]
        if config.ENV_DATA.get("cluster_name"):
            self.cluster_name = config.ENV_DATA["cluster_name"]
        else:
            self.cluster_name = get_cluster_name(self.cluster_path)
        # dict of cluster prefixes with special handling rules (for existence
        # check or during a cluster cleanup)
        self.cluster_prefixes_special_rules = {}

    def check_cluster_existence(self, cluster_name_prefix):
        """
        Check cluster existence according to cluster name prefix

        Returns:
            bool: True if a cluster with the same name prefix already exists,
                False otherwise

        """
        raise NotImplementedError()

    def deploy_ocp(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")

        """
        if not config.DEPLOYMENT.get("force_deploy_multiple_clusters"):
            prefix = get_cluster_prefix(
                self.cluster_name, self.cluster_prefixes_special_rules
            )
            if self.check_cluster_existence(prefix):
                raise exceptions.SameNamePrefixClusterAlreadyExistsException(
                    f"Cluster with name prefix {prefix} already exists. "
                    f"Please destroy the existing cluster for a new cluster "
                    f"deployment"
                )
        super(CloudDeploymentBase, self).deploy_ocp(log_cli_level)


class IPIOCPDeployment(BaseOCPDeployment):
    """
    Common implementation of IPI OCP deployments for cloud platforms.
    """

    def __init__(self):
        super(IPIOCPDeployment, self).__init__()

    def deploy_prereq(self):
        """
        Overriding deploy_prereq from parent. Perform all necessary
        prerequisites for cloud IPI here.
        """
        super(IPIOCPDeployment, self).deploy_prereq()
        if config.DEPLOYMENT["preserve_bootstrap_node"]:
            logger.info("Setting ENV VAR to preserve bootstrap node")
            os.environ["OPENSHIFT_INSTALL_PRESERVE_BOOTSTRAP"] = "True"
            assert os.getenv("OPENSHIFT_INSTALL_PRESERVE_BOOTSTRAP") == "True"

    def deploy(self, log_cli_level="DEBUG"):
        """
        Deployment specific to OCP cluster on a cloud platform.

        Args:
            log_cli_level (str): openshift installer's log level
                (default: "DEBUG")
        """
        logger.info("Deploying OCP cluster")
        install_timeout = config.DEPLOYMENT.get("openshift_install_timeout")
        logger.info(
            f"running openshift-install with '{log_cli_level}' log level "
            f"and {install_timeout} second timeout"
        )
        try:
            run_cmd(
                f"{self.installer} create cluster "
                f"--dir {self.cluster_path} "
                f"--log-level {log_cli_level}",
                timeout=install_timeout,
            )
        except (exceptions.CommandFailed, subprocess.TimeoutExpired) as e:
            if constants.GATHER_BOOTSTRAP_PATTERN in str(e):
                try:
                    gather_bootstrap()
                except Exception as ex:
                    logger.error(ex)
            # W/A for bugs:
            # - https://issues.redhat.com/browse/OCPBUGS-63723
            # - https://redhat.atlassian.net/browse/OCPBUGS-125799
            # Issue to track W/A: https://github.com/red-hat-storage/ocs-ci/issues/13519
            if (
                "failed retrieving cos instance for destroy bootstrap: COS Resource Not Found"
                in str(e)
            ):
                logger.warning(
                    "COS instance not found for destroy bootstrap, related to bugs: "
                    "OCPBUGS-63723 and OCPBUGS-125799, continuing..."
                )
                logger.warning("Deleting bootstrap leftovers")
                set_target_region()

                # Try to get infra_id from multiple sources (in order of reliability)
                # 1. openshift_install_state.json (created early, always available)
                # 2. Cluster API (if cluster is accessible)
                # 3. metadata.json (only after successful install)
                # 4. cluster_name as prefix (last resort)
                infra_id = None

                try:
                    infra_id = get_infra_id_from_openshift_install_state(
                        self.cluster_path
                    )
                    logger.info(
                        f"Got infra_id from openshift_install_state.json: {infra_id}"
                    )
                except Exception as ex:
                    logger.warning(
                        f"Could not get infra_id from openshift_install_state.json: {ex}"
                    )

                if not infra_id:
                    try:
                        # Try to get infra_id from cluster API (cluster must be accessible)
                        from ocs_ci.ocs.ocp import OCP

                        infra_obj = OCP(kind="infrastructure", resource_name="cluster")
                        infra_id = (
                            infra_obj.get().get("status", {}).get("infrastructureName")
                        )
                        if infra_id:
                            logger.info(f"Got infra_id from cluster API: {infra_id}")
                        else:
                            raise ValueError(
                                "infrastructureName not found in cluster object"
                            )
                    except Exception as ex:
                        logger.warning(f"Could not get infra_id from cluster API: {ex}")

                if not infra_id:
                    try:
                        infra_id = get_infra_id(config.ENV_DATA["cluster_name"])
                        logger.info(f"Got infra_id from metadata.json: {infra_id}")
                    except Exception as ex:
                        logger.warning(
                            f"Could not get infra_id from metadata.json: {ex}"
                        )

                if not infra_id:
                    logger.error(
                        "Could not get infra_id from any source. "
                        "Attempting cleanup with cluster_name prefix instead."
                    )
                    # Fall back to using cluster_name as prefix
                    infra_id = f"{config.ENV_DATA['cluster_name']}-"

                # Try to delete bootstrap VSI
                try:
                    logger.info(f"Checking for bootstrap VSI: {infra_id}-bootstrap")
                    run_ibmcloud_cmd(f"ibmcloud is instance {infra_id}-bootstrap")
                    logger.warning(f"Deleting bootstrap VSI: {infra_id}-bootstrap")
                    run_ibmcloud_cmd(
                        f"ibmcloud is instance-delete --force {infra_id}-bootstrap"
                    )
                    logger.info(
                        f"Successfully deleted bootstrap VSI: {infra_id}-bootstrap"
                    )
                except Exception as e:
                    logger.warning(
                        f"Bootstrap VSI cleanup: {e} (may already be deleted)"
                    )

                # Try to delete COS bootstrap resources
                try:
                    logger.info(f"Checking for COS instance: {infra_id}-cos")
                    cos_instances_output = run_ibmcloud_cmd(
                        f"ibmcloud resource service-instance --output json {infra_id}-cos"
                    )
                    cos_instances = json.loads(cos_instances_output)

                    # Handle both single instance (dict) and multiple instances (list)
                    if isinstance(cos_instances, dict):
                        cos_instances = [cos_instances]

                    # Sort by creation time and get the latest one (bootstrap COS)
                    # The first COS instance is for VSI images, the second is for bootstrap
                    cos_instances_sorted = sorted(
                        cos_instances, key=lambda x: x.get("created_at", "")
                    )

                    for idx, cos_instance in enumerate(cos_instances_sorted):
                        cos_guid = cos_instance["guid"]
                        cos_name = cos_instance.get("name", "unknown")
                        created_at = cos_instance.get("created_at", "unknown")

                        logger.info(
                            f"Found COS instance {idx+1}/{len(cos_instances_sorted)}: "
                            f"{cos_name} (GUID: {cos_guid}, created: {created_at})"
                        )

                        try:
                            buckets_output = run_ibmcloud_cmd(
                                f"ibmcloud cos buckets --output json --ibm-service-instance-id {cos_guid}"
                            )
                            buckets_data = json.loads(buckets_output)
                            buckets = buckets_data.get("Buckets", [])

                            # Check if this is the bootstrap COS instance (has bootstrap bucket)
                            bootstrap_buckets = [
                                b for b in buckets if "bootstrap" in b.get("Name", "")
                            ]

                            if bootstrap_buckets:
                                logger.warning(
                                    f"Found bootstrap COS instance with {len(bootstrap_buckets)} "
                                    f"bootstrap bucket(s): {[b['Name'] for b in bootstrap_buckets]}"
                                )
                                logger.warning(
                                    f"Deleting bootstrap COS instance: {cos_name} (GUID: {cos_guid})"
                                )
                                run_ibmcloud_cmd(
                                    f"ibmcloud resource service-instance-delete -f {cos_guid}"
                                )
                                logger.info(
                                    f"Successfully deleted bootstrap COS instance: {cos_name} "
                                    f"with bucket(s): {[b['Name'] for b in bootstrap_buckets]}"
                                )
                            else:
                                logger.info(
                                    f"COS instance {cos_name} has no bootstrap buckets "
                                    f"(has {len(buckets)} bucket(s)), skipping deletion"
                                )
                        except Exception as bucket_err:
                            logger.warning(
                                f"Could not check/delete buckets for COS {cos_name}: {bucket_err}"
                            )

                except Exception as e:
                    logger.warning(
                        f"COS cleanup failed or no COS instance found: {e}. "
                        "This is expected if COS resources were already cleaned up or never created."
                    )

                # Verify the cluster is actually healthy before declaring success
                logger.info(
                    "Verifying cluster health after bootstrap cleanup workaround..."
                )
                try:
                    cluster_operators = ocp.get_all_cluster_operators()
                    logger.info(
                        f"Found {len(cluster_operators)} cluster operators to verify"
                    )

                    for ocp_operator in cluster_operators:
                        logger.info(f"Checking cluster operator: {ocp_operator}")
                        for sampler in TimeoutSampler(
                            timeout=1600,
                            sleep=60,
                            func=ocp.verify_cluster_operator_status,
                            cluster_operator=ocp_operator,
                        ):
                            if sampler:
                                logger.info(
                                    f"Cluster operator {ocp_operator} is healthy"
                                )
                                break
                            else:
                                logger.info(
                                    f"Waiting for {ocp_operator} to become healthy..."
                                )

                    logger.info("Checking clusterversion status")
                    cluster_version_timeout = 1800
                    for sampler in TimeoutSampler(
                        timeout=cluster_version_timeout,
                        sleep=15,
                        func=ocp.validate_cluster_version_status,
                    ):
                        if sampler:
                            logger.info(
                                "Installation Completed Successfully despite bootstrap cleanup issue! "
                                "W/A applied for OCPBUGS-63723 / OCPBUGS-125799"
                            )
                            break
                except Exception as health_check_err:
                    logger.error(
                        f"Cluster health verification failed: {health_check_err}. "
                        "The cluster may not have deployed successfully."
                    )
                    raise
            elif "Waiting up to" in str(e):
                if (
                    config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                    and config.ENV_DATA["deployment_type"] == constants.IPI_DEPL_TYPE
                ):
                    configure_ingress_load_balancer_security_group()
                run_cmd(
                    f"{self.installer} wait-for install-complete "
                    f"--dir {self.cluster_path} "
                    f"--log-level {log_cli_level}",
                    timeout=3600,
                )
            else:
                raise e
        self.test_cluster()
