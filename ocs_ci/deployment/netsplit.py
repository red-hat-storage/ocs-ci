# -*- coding: utf8 -*-


import logging

import ocpnetsplit.main

from ocs_ci.deployment.zones import are_zone_labels_present
from ocs_ci.ocs import exceptions
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile


logger = logging.getLogger(__name__)


def setup_netsplit(
    tmp_path, master_zones, worker_zones, x_addr_list=None, arbiter_zone=None
):
    """
    Deploy machineconfig with network split scripts and configuration, tailored
    for the current cluster state.

    Args:
        tmp_path(pathlib.Path): Directory where a temporary yaml file will
                be created. In test context, use pytest fixture ``tmp_path``.
        master_zones(list[str]): zones where master nodes are placed
        worker_zones(list[str]): zones where worker nodes are placed
        x_addr_list(list[str]): IP addressess of external services (zone x)
        arbiter_zone(str): name of arbiter zone if arbiter deployment is used

    Raises:
        UnexpectedDeploymentConfiguration: in case of invalid cluster
            configuration, which prevents deployment of network split scripts
        ValueError: in case given zone configuration doesn't make any sense
    """
    logger.info("going to deploy ocpnetsplit scripts")
    # checking assumptions: each node has a zone label
    if not are_zone_labels_present():
        msg = "to use network_split_setup, all nodes needs a zone label"
        logger.error(msg)
        raise exceptions.UnexpectedDeploymentConfiguration(msg)
    # check zone assummtions: all worker zones are master zones as well
    worker_zones_without_master = set(worker_zones).difference(set(master_zones))
    if len(worker_zones_without_master) != 0:
        msg = (
            "there are zones which contains worker nodes, "
            f"but no master nodes: {worker_zones_without_master}"
        )
        logger.error(msg)
        raise exceptions.UnexpectedDeploymentConfiguration(msg)
    if (arbiter_zone is not None) and (arbiter_zone not in master_zones):
        msg = "given arbiter zone not found among master zones"
        logger.error(msg)
        raise ValueError(msg)
    if len(master_zones) == 3:
        zone_a, zone_b, zone_c = master_zones
        # handle arbiter (so that zone a is always arbiter) if specified
        if arbiter_zone is not None:
            zone_a = arbiter_zone
            other_zones = master_zones.copy()
            other_zones.remove(arbiter_zone)
            zone_b, zone_c = other_zones
    else:
        msg = "ocpnetsplit can handle only 3 zones, setup can't continue"
        logger.error(msg)
        raise exceptions.UnexpectedDeploymentConfiguration(msg)
    # we assume that there are just 2 machine config pools: master and worker
    mcp_h = OCP(kind="MachineConfigPool", namespace="openshift-config")
    mcp_objects = mcp_h.get()
    mcp_names = [i["metadata"]["name"] for i in mcp_objects["items"]]
    if len(mcp_names) != 2:
        msg = (
            "ocpnetsplit can handle only 2 machine config pools, "
            f"but there are {mcp_names}"
        )
        logger.error(msg)
        raise exceptions.UnexpectedDeploymentConfiguration(msg)
    for exp_pool in ("master", "worker"):
        if exp_pool not in mcp_names:
            msg = f"MachineConfigPool/{exp_pool} not found"
            logger.error(msg)
            raise exceptions.UnexpectedDeploymentConfiguration(msg)
    # generate zone config (list of node ip addressess for each zone)
    zone_config = ocpnetsplit.main.get_zone_config(zone_a, zone_b, zone_c, x_addr_list)
    zone_env = zone_config.get_env_file()
    # get machinecofnig for network split firewall scripts
    mc = ocpnetsplit.main.get_networksplit_mc_spec(zone_env)
    # deploy it within openshift-config namespace
    mc_file = ObjectConfFile("network-split", mc, None, tmp_path)
    mc_file.create(namespace="openshift-config")
    # now let's make sure the MCO (machine config operator) noticed just
    # deployed network-split machine config and started to process it
    logger.info(
        "waiting for both machineconfigpools to be updating "
        "as a result of deployment of network-split machineconfig"
    )
    mcp_h.wait_for_resource(
        resource_count=2,
        condition="True",
        column="UPDATING",
        sleep=5,
        timeout=120,
    )
    # and now wait for MachineConfigPools to be updated and ready
    logger.info("waiting for both machineconfigpools to be updated and ready")
    mcp_h.wait_for_resource(
        resource_count=2,
        condition="True",
        column="UPDATED",
        sleep=60,
        timeout=1800,
    )
    # also check that no pools are degraded
    mcp_h.wait_for_resource(
        resource_count=2,
        condition="False",
        column="DEGRADED",
        sleep=10,
        timeout=120,
    )
