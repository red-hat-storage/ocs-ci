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

    config.ENV_DATA["cluster_path"] = "/"
    if args.cluster_name:
        cluster_name = args.cluster_name
        config.ENV_DATA["cluster_name"] = cluster_name
        ibm_cloud_ipi_obj = IBMCloudIPI()
        resource_group_name = ibm_cloud_ipi_obj.get_resource_group(return_id=False)
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
        return
    else:
        config.ENV_DATA["cluster_name"] = "cluster"

    IbmClusterDeleteion()

    # region = IBM_REGION if not args.region else args.region


class IbmClusterDeleteion(object):
    def __init__(self):
        self.clusters_deletion = list()
        self.ibm_cloud_ipi_obj = IBMCloudIPI()
        self.determine_cluster_deletion()
        self.delete_clusters()

    def determine_cluster_deletion(self):
        resource_group_names = self.ibm_cloud_ipi_obj.get_resource_groups()

        for resource_group_name in resource_group_names:
            created_time = self.ibm_cloud_ipi_obj.get_created_time(resource_group_name)
            for prefix, hours in CLUSTER_PREFIXES_SPECIAL_RULES.items():
                pattern = re.compile(f"r'{prefix}'")
                if pattern.search(resource_group_name):
                    delete_hours = hours
                elif prefix == "never":
                    continue
                else:
                    delete_hours = DEFAULT_TIME
            if created_time > delete_hours:
                self.clusters_deletion.append(resource_group_name)

    def delete_clusters(self):
        for cluster_deletion in self.clusters_deletion:
            self.ibm_cloud_ipi_obj.delete_leftover_resources(cluster_deletion)
