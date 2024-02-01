import argparse
import logging
import re
import yaml
import sys

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
        help="""
            IBM configuration file in yaml format.
            Example file:
                ---
                ENV_DATA:
                  platform: 'ibm_cloud'
                  deployment_type: 'ipi'
                  region: 'us-south'
            """,
    )
    parser.add_argument(
        "--hours",
        action="store",
        required=False,
        help="The name of the IBM region to delete the resources from",
    )

    args = parser.parse_args()

    # load ibm_conf data to config
    if args.ocsci_conf:
        ibm_conf = args.ocsci_conf
        ibm_config_data = yaml.safe_load(ibm_conf)
        framework.config.update(ibm_config_data)
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

    time_to_delete = int(args.hours) if args.hours else None
    IbmClusterDeleteion(time_to_delete)


class IbmClusterDeleteion(object):
    def __init__(self, time_to_delete):
        self.time_to_delete = (
            time_to_delete if time_to_delete is not None else DEFAULT_TIME
        )
        self.clusters_deletion = list()
        self.clusters_deletion_failed = dict()
        self.ibm_cloud_ipi_obj = IBMCloudIPI()
        self.determine_cluster_deletion()
        self.delete_clusters()

    def determine_cluster_deletion(self):
        resource_group_names = self.ibm_cloud_ipi_obj.get_resource_groups()
        for resource_group_name in resource_group_names:
            created_time = self.ibm_cloud_ipi_obj.get_created_time(resource_group_name)
            for prefix, hours in CLUSTER_PREFIXES_SPECIAL_RULES.items():
                prefix_pattern = re.compile(rf"^{prefix}")
                if prefix_pattern.match(resource_group_name):
                    if hours == "never":
                        delete_hours = sys.maxsize
                        break
                    else:
                        delete_hours = hours
                        break
                else:
                    delete_hours = self.time_to_delete

            if created_time > delete_hours:
                self.clusters_deletion.append(resource_group_name)

    def delete_clusters(self):
        for cluster_deletion in self.clusters_deletion:
            try:
                self.ibm_cloud_ipi_obj.delete_leftover_resources(cluster_deletion)
            except Exception as e:
                self.clusters_deletion_failed[cluster_deletion] = e
                logger.info(f"{cluster_deletion} cluster deletion failed:\n{e}")

        if len(self.clusters_deletion_failed) > 0:
            err_string = ""
            for cluster_deletion_failed, error in self.clusters_deletion_failed.items():
                err_string += (
                    f"{cluster_deletion_failed} cluster deletion failed:\n{error}"
                )
            raise Exception(err_string)
