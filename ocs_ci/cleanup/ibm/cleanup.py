import argparse
import logging
import re
import yaml

from ocs_ci.framework import config
from ocs_ci import framework
from ocs_ci.deployment.ibmcloud import IBMCloudIPI
from ocs_ci.cleanup.ibm.defaults import CLUSTER_PREFIXES_SPECIAL_RULES, DEFAULT_TIME


logger = logging.getLogger(__name__)


def ibm_cleanup():
    parser = argparse.ArgumentParser(
        description="ibmcloud cluster cleanup",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--cluster-name",
        action="store",
        required=False,
        help="The name of the cluster to delete from ibmcloud",
    )
    parser.add_argument(
        "--ocsci-conf",
        action="store",
        required=False,
        type=argparse.FileType("r", encoding="UTF-8"),
        help="""IBM configuration file in yaml format.
            Example file:
                ---
                ENV_DATA:
                  platform: 'ibm_cloud'
                  deployment_type: 'ipi'
                  region: 'us-south'

            """,
    )
    parser.add_argument(
        "--region",
        action="store",
        required=False,
        help="The name of the IBM region to delete the resources from",
    )

    args = parser.parse_args()

    # load ibm_conf data to config
    if args.ocsci_conf:
        ibm_conf = args.ocsci_conf
        vsphere_config_data = yaml.safe_load(ibm_conf)
        framework.config.update(vsphere_config_data)
        ibm_conf.close()

    if args.cluster_name:
        cluster_name = args.cluster_name
        config.ENV_DATA["cluster_name"] = cluster_name
        config.ENV_DATA["cluster_path"] = "/"
        ibm_cloud_ipi_obj = IBMCloudIPI()
        resource_group_name = ibm_cloud_ipi_obj.get_resource_group()
        resource_group_id = ibm_cloud_ipi_obj.get_resource_group(return_id=True)

        if resource_group_name is None:
            logger.info(
                "Resource group not found. Please check via command: "
                "ibmcloud resource groups , what resource group you have."
            )
            return
        else:
            logger.info(f"Resource group found: {resource_group_name}")
            logger.info(f"Resource group ID: {resource_group_id}")

        ibm_cloud_ipi_obj.delete_leftover_resources(resource_group_name)
    else:
        config.ENV_DATA["cluster_name"] = "cluster"

    # region = IBM_REGION if not args.region else args.region

    ibm_cloud_ipi_obj = IBMCloudIPI()
    resource_group_name = ibm_cloud_ipi_obj.get_resource_group()
    resource_group_id = ibm_cloud_ipi_obj.get_resource_group(return_id=True)
    if resource_group_name is None:
        logger.info(
            "Resource group not found. Please check via command: "
            "ibmcloud resource groups , what resource group you have."
        )
        return
    else:
        logger.info(f"Resource group found: {resource_group_name}")
        logger.info(f"Resource group ID: {resource_group_id}")

    if resource_group_name:
        pass
    clusters_deletion = determine_cluster_deletion()
    for cluster_deletion in clusters_deletion:
        ibm_cloud_ipi_obj.delete_leftover_resources(cluster_deletion)


def determine_cluster_deletion():
    cluster_deletion = list()
    ibm_cloud_ipi_obj = IBMCloudIPI()
    resource_group_names = ibm_cloud_ipi_obj.get_resource_groups()
    for resource_group_name in resource_group_names:
        for prefix, hours in CLUSTER_PREFIXES_SPECIAL_RULES.items():
            pattern = re.compile(f"r'{prefix}'")
            created_time = ibm_cloud_ipi_obj.get_created_time(resource_group_name)
            if pattern.search(resource_group_name):
                if hours == "never":
                    continue
                if created_time > hours:
                    cluster_deletion.append(resource_group_name)
            elif created_time > DEFAULT_TIME:
                cluster_deletion.append(resource_group_name)
    return resource_group_names
