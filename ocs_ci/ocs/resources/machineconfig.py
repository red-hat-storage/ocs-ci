# -*- coding: utf8 -*-


import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile


logger = logging.getLogger(__name__)


def deploy_machineconfig(tmp_path, mc_name, mc_dict, mcp_num=2):
    """
    Deploy given ``MachineConfig`` dict and wait for the configuration to be
    deployed on all MachineConfigPools. By default we assume there are
    just two pools.

    Args:
        tmp_path (pathlib.Path): Directory where a temporary yaml file will
            be created. In test context, use pytest fixture `tmp_path`_.
        mc_name (str): name prefix for object config yaml file which will be
            created for the machineconfig before it's deployment
        mc_dict (list): list of dictionaries with MachineConfig resource(s)
            to deploy
        mcp_num (int): number of MachineConfigPool resources in the cluster

    .. _`tmp_path`: https://docs.pytest.org/en/latest/tmpdir.html#the-tmp-path-fixture
    """
    # deploy the machine config within openshift-config namespace
    mc_file = ObjectConfFile(mc_name, mc_dict, None, tmp_path)
    mc_file.create(namespace=constants.OPENSHIFT_CONFIG_NAMESPACE)
    # now let's make sure the MCO (machine config operator) noticed just
    # deployed givne machine config and started to process it
    logger.info(
        "waiting for both machineconfigpools to be updating "
        "as a result of deployment of given machineconfig"
    )
    mcp_h = OCP(
        kind=constants.MACHINECONFIGPOOL, namespace=constants.OPENSHIFT_CONFIG_NAMESPACE
    )
    mcp_h.wait_for_resource(
        resource_count=mcp_num,
        condition="True",
        column="UPDATING",
        sleep=5,
        timeout=120,
    )
    # and now wait for MachineConfigPools to be updated and ready
    logger.info("waiting for %d machineconfigpools to be updated and ready", mcp_num)
    mcp_h.wait_for_resource(
        resource_count=mcp_num,
        condition="True",
        column="UPDATED",
        sleep=60,
        timeout=1800,
    )
    # also check that no pools are degraded
    mcp_h.wait_for_resource(
        resource_count=mcp_num,
        condition="False",
        column="DEGRADED",
        sleep=10,
        timeout=120,
    )
    logger.info("MachineConfig %s has been deployed", mc_name)
