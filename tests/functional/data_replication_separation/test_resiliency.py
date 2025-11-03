import logging
import pytest
import random
import time

from ocs_ci.framework.pytest_customization.marks import (
    data_replication_separation_required,
    runs_on_provider,
    yellow_squad,
)
from ocs_ci.framework.testlib import tier4a, tier4c
from ocs_ci.helpers.helpers import modify_deployment_replica_count
from ocs_ci.ocs.resources.deployment import get_mon_deployments
from ocs_ci.ocs.cluster import (
    ceph_health_check,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs import data_replication_separation
from ocs_ci.ocs.node import (
    drain_nodes,
    get_nodes,
    schedule_nodes,
    wait_for_nodes_status,
)
from ocs_ci.utility.utils import wait_for_machineconfigpool_status

logger = logging.getLogger(__name__)


@tier4a
@runs_on_provider
@data_replication_separation_required
@pytest.mark.polarion_id("OCS-7363")
@yellow_squad
def test_worker_node_drain():
    """
    Test that node configuration is correct after a worker node drain.
    """
    ocp_nodes = get_nodes(node_type=constants.WORKER_MACHINE)
    ocp_node = random.choice(ocp_nodes)
    drain_nodes([ocp_node.name])
    wait_for_machineconfigpool_status(
        node_type=constants.WORKER_MACHINE, force_delete_pods=True
    )
    # Wait for the node to be unschedule
    wait_for_nodes_status(
        node_names=[ocp_node.name],
        status=constants.NODE_READY_SCHEDULING_DISABLED,
        timeout=120,
        sleep=5,
    )

    wait_time_before_reschedule = 30
    logger.info(
        f"Wait {wait_time_before_reschedule} seconds before rescheduling the node"
    )
    time.sleep(wait_time_before_reschedule)

    schedule_nodes([ocp_node.name])
    wait_for_nodes_status(
        node_names=[ocp_node.name],
        status=constants.NODE_READY,
        timeout=120,
        sleep=5,
    )
    logger.info("Checking that the Ceph health is OK")
    ceph_health_check()
    logger.info("Checking that all nodes are correctly annotated")
    data_replication_separation.validate_mon_ip_annotation_on_workers()


@tier4c
@runs_on_provider
@pytest.mark.polarion_id("OCS-7364")
@data_replication_separation_required
@yellow_squad
def test_mon_respin():
    """
    Test that a new monitor has set hostnetwork correctly when monitor is respinned.
    """
    mon_obj = random.choice(get_mon_deployments())
    assert modify_deployment_replica_count(
        mon_obj.name, 0
    ), f"Fail to scale {mon_obj.name} to replica count: 0"
    logger.info("Wait for new monitor to be provisioned")
    time.sleep(60)
    logger.info("Checking that the Ceph health is OK")
    ceph_health_check()
    logger.info("Checking that all mons have set host network")
    data_replication_separation.validate_monitor_pods_have_host_network()
