"""
This module will cleanup the clusters in vSphere Environment

Use-case for this module:
    1. Jenkins slave lost or deleted accidentally
    2. Lost of terraform data/files

"""

import argparse
import logging
import os
import requests
import yaml

from ocs_ci import framework
from ocs_ci.framework import config
from ocs_ci.utility.aws import AWS
from ocs_ci.utility.vsphere import VSPHERE as VSPHEREUtil

FORMAT = "%(asctime)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def delete_cluster(vsphere, cluster_name):
    """
    Deletes the cluster

    Args:
        vsphere (instance): vSphere instance
        cluster_name (str): Cluster name to delete from Data center

    """
    datacenter = config.ENV_DATA["vsphere_datacenter"]
    cluster = config.ENV_DATA["vsphere_cluster"]

    # check for Resource pool
    if not vsphere.is_resource_pool_exist(cluster_name, datacenter, cluster):
        logger.info(f"Resource pool {cluster_name} does not exists")
        return

    # Get all VM's in resource pool
    vms = vsphere.get_all_vms_in_pool(cluster_name, datacenter, cluster)
    if not vms:
        logger.info(f"There is no VM's in resource pool {cluster_name}")
        # delete the resource pool even though it empty
        vsphere.destroy_pool(cluster_name, datacenter, cluster)
        return

    # Delete the disks
    logger.info("Deleting disks from VM's")
    vm_names = []
    for vm in vms:
        vm_names.append(vm.name)
        vsphere.remove_disks(vm)
    config.ENV_DATA["vm_names"] = vm_names

    # delete the resource pool
    vsphere.destroy_pool(cluster_name, datacenter, cluster)


def get_vsphere_connection(server, user, password):
    """
    Establish connection to vSphere

    Args:
        server (str): vCenter server to connect
        user (str): vCenter username to login
        password ( str): password to login

    Returns:
        Instance: vSphere Instance

    """
    server = server or config.ENV_DATA["vsphere_server"]
    user = user or config.ENV_DATA["vsphere_user"]
    password = password or config.ENV_DATA["vsphere_password"]
    vsphere = VSPHEREUtil(server, user, password)
    return vsphere


class IPAM(object):
    """
    IPAM class
    """

    def __init__(self):
        """
        Initialize required variables
        """
        self.ipam = config.ENV_DATA["ipam"]
        self.token = config.ENV_DATA["ipam_token"]
        self.base_domain = config.ENV_DATA["base_domain"]
        self.cluster_name = config.ENV_DATA["vsphere_cluster"]
        self.apiapp = "address"

    def delete_ips(self, cluster_name):
        """
        Delete IP's from IPAM server

        Args:
            cluster_name (str): Name of the cluster to release IP's
                from IPAM server

        """
        # Form the FQDN for the nodes
        all_nodes = []
        nodes = config.ENV_DATA.get("vm_names", [])
        # sometime cluster deployment fails without creating VM's
        # ( eg: space issue ) but VM's has reserved the IP address in
        # IPAM server. In that we have to delete the IP's in IPAM server
        # even though resource pool is not created in Datacenter
        if not nodes:
            node_type = ["compute", "control-plane", "lb"]
            for each_type in node_type:
                if each_type == "lb":
                    nodes.append(f"{each_type}-0")
                    continue
                for i in range(0, 3):
                    nodes.append(f"{each_type}-{i}")
        for node in nodes:
            node_fqdn = f"{node}.{cluster_name}.{config.ENV_DATA['base_domain']}"
            all_nodes.append(node_fqdn)

        logger.info(f"Removing IP's for nodes {all_nodes} from IPAM server")
        # release the IPs
        endpoint = os.path.join("http://", self.ipam, "api/removeHost.php?")
        for node in all_nodes:
            payload = {"apiapp": self.apiapp, "apitoken": self.token, "host": node}
            res = requests.post(endpoint, data=payload)
            if res.status_code == "200":
                logger.info(f"Successfully deleted {node} IP from IPAM server")


def vsphere_cleanup():
    """
    Deletes the cluster and all the associated resources
    on vSphere environment.

    Resources that are deleting:
        1. Delete disks
        2. Delete VM's
        3. Delete Resource Pool
        4. Remove IP's from IPAM server
        5. Removes Resource records from Hosted Zone
        6. Removes Hosted Zone from AWS
        7. Removes records from Base Domain

    """
    parser = argparse.ArgumentParser(
        description="vSphere cluster cleanup",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--cluster-name",
        action="store",
        required=True,
        help="The name of the cluster to delete from vSphere",
    )
    parser.add_argument(
        "--ocsci-conf",
        action="store",
        required=True,
        type=argparse.FileType("r", encoding="UTF-8"),
        help="""vSphere configuration file in yaml format.
            Example file:
                ---
                ENV_DATA:
                  # aws region
                  region: 'us-east-2'
                  base_domain: 'qe.rh-ocs.com'
                  # vsphere details
                  vsphere_server: '<your_vcenter.lab.com>'
                  vsphere_user: '<user>'
                  vsphere_password: '<password>'
                  vsphere_cluster: '<cluster name>'
                  vsphere_datacenter: '<datacenter name>'
                  ipam: '<IP>'
                  ipam_token: '<IPAM token>'
            """,
    )

    args = parser.parse_args()
    cluster_name = args.cluster_name
    vsphere_conf = args.ocsci_conf

    # load vsphere_conf data to config
    vsphere_config_data = yaml.safe_load(vsphere_conf)
    framework.config.update(vsphere_config_data)
    vsphere_conf.close()

    # get connection to vSphere
    server = config.ENV_DATA["vsphere_server"]
    user = config.ENV_DATA["vsphere_user"]
    password = config.ENV_DATA["vsphere_password"]
    vsphere = get_vsphere_connection(server, user, password)

    # delete the cluster
    delete_cluster(vsphere, cluster_name)

    # release IP's from IPAM server
    ipam = IPAM()
    ipam.delete_ips(cluster_name=cluster_name)

    # Delete AWS route
    aws = AWS()
    aws.delete_hosted_zone(cluster_name=cluster_name)

    # Delete records in base domain
    base_domain = config.ENV_DATA["base_domain"]
    aws.delete_record_from_base_domain(
        cluster_name=cluster_name, base_domain=base_domain
    )
