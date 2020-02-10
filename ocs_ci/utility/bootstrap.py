import logging
import os

import boto3
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import NodeNotFoundError, UnsupportedPlatformError
from ocs_ci.utility import vsphere
from ocs_ci.utility.utils import get_infra_id, run_cmd

logger = logging.getLogger(__name__)


def gather_bootstrap():
    """
    Gather debugging data for a failing-to-bootstrap control plane.
    Data is placed in the `gather_bootstrap` directory under the log directory.

    Raises:
        NodeNotFoundError: If we are unable to retrieve the IP of any master
            nodes

    """
    logger.info("Running gather bootstrap")
    gather_bootstrap_dir = os.path.expanduser(os.path.join(
        config.RUN['log_dir'], 'gather_bootstrap'
    ))
    openshift_install = os.path.join(
        config.RUN.get('bin_dir'),
        'openshift-install'
    )
    ssh_key = os.path.expanduser(config.DEPLOYMENT.get('ssh_key_private'))
    data = get_gather_bootstrap_node_data()
    bootstrap_ip = data['bootstrap_ip']
    logger.debug('Bootstrap IP: %s', bootstrap_ip)
    master_ips = data['master_ips']
    logger.debug('Master IPs: %s', master_ips)
    cmd = (
        f"{openshift_install} gather bootstrap --bootstrap {bootstrap_ip} "
        f"--dir {gather_bootstrap_dir} --log-level debug --key {ssh_key} "
    )
    if len(master_ips) == 0:
        raise NodeNotFoundError(
            "No master nodes found for cluster, "
            "unable to gather bootstrap data"
        )
    for master in master_ips:
        cmd += f"--master {master} "
    run_cmd(cmd)


def get_gather_bootstrap_node_data():
    """
    Retrieve node IPs required by the gather bootstrap command

    Raises:
        UnsupportedPlatformError: If we do not support gathering bootstrap
            data for the configured provider

    Returns:
        dict: Public IP of the bootstrap node and Private IPs of master nodes

    """
    logger.info("Retrieving bootstrap node data")
    platform = config.ENV_DATA['platform'].lower()
    if platform == constants.AWS_PLATFORM:
        return get_node_data_aws()
    elif platform == constants.VSPHERE_PLATFORM:
        return get_node_data_vsphere()
    else:
        raise UnsupportedPlatformError(
            "Platform '%s' is not supported, "
            "unable to retrieve gather bootstrap node data",
            platform
        )


def get_node_data_aws():
    """
    Retrieve bootstrap public IP and master node private IPs running in aws

    Raises:
        NodeNotFoundError: If we are unable to find the bootstrap node or IP

    Returns:
        dict: bootstrap and master node IP data

    """
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()
    ec2_driver = get_driver(Provider.EC2)
    driver = ec2_driver(
        credentials.access_key, credentials.secret_key,
        region=config.ENV_DATA['region']
    )
    cluster_path = config.ENV_DATA['cluster_path']
    infra_id = get_infra_id(cluster_path)
    bootstrap_name = f"{infra_id}-bootstrap"
    master_pattern = f"{infra_id}-master"
    data = dict()
    try:
        bootstrap_node = [
            node for node in driver.list_nodes()
            if bootstrap_name == node.name
        ][0]
        bootstrap_ip = bootstrap_node.public_ips[0]
        logger.info(
            "Found bootstrap node %s with IP %s", bootstrap_name, bootstrap_ip
        )
        data['bootstrap_ip'] = bootstrap_ip

    except IndexError:
        raise NodeNotFoundError(
            f"Unable to find bootstrap node with name {bootstrap_name}"
        )
    master_nodes = [
        node for node in driver.list_nodes()
        if master_pattern in node.name
    ]
    master_ips = [master.private_ips[0] for master in master_nodes]
    data['master_ips'] = master_ips
    logger.debug(data)
    return data


def get_node_data_vsphere():
    """
    Retrieve bootstrap public IP and master node private IPs running in vsphere

    Raises:
        NodeNotFoundError: If we are unable to find the bootstrap node or IP

    Returns:
        dict: bootstrap and master node IP data

    """
    server = config.ENV_DATA['vsphere_server']
    user = config.ENV_DATA['vsphere_user']
    password = config.ENV_DATA['vsphere_password']
    cluster = config.ENV_DATA['vsphere_cluster']
    datacenter = config.ENV_DATA['vsphere_datacenter']
    pool = config.ENV_DATA['cluster_name']
    _vsphere = vsphere.VSPHERE(server, user, password)
    bootstrap_name = "bootstrap-0"
    master_pattern = "control-plane"
    data = dict()
    try:
        bootstrap_vm = _vsphere.get_vm_in_pool_by_name(
            bootstrap_name, datacenter, cluster, pool
        )
        bootstrap_ip = _vsphere.get_vms_ips([bootstrap_vm])[0]
        logger.info(
            "Found bootstrap node %s with IP %s", bootstrap_name, bootstrap_ip
        )
        data['bootstrap_ip'] = bootstrap_ip
    except IndexError:
        raise NodeNotFoundError(
            f"Unable to find bootstrap node with name {bootstrap_name}"
        )
    master_ips = list()
    for i in range(config.ENV_DATA['master_replicas']):
        master_name = f"{master_pattern}-{i}"
        master_node = _vsphere.get_vm_in_pool_by_name(
            master_name, datacenter, cluster, pool
        )
        master_ip = _vsphere.get_vms_ips([master_node])[0]
        logger.info("Found master node %s with IP %s", master_name, master_ip)
        master_ips.append(master_ip)
    data['master_ips'] = master_ips
    logger.debug(data)
    return data
