import uuid
import time
import threading
import random
import logging
from datetime import datetime

from ocs_ci.ocs.platform_nodes import PlatformNodesFactory
from ocs_ci.ocs.node import get_nodes
import tests.OMADOps.conftest as omadops_config
from ocs_ci.ocs.longevity import Longevity
from ocs_ci.ocs import constants, node
from ocs_ci.ocs.ocp import switch_to_default_rook_cluster_project
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


def dummy_func(op_enum):
    log.info(
        f"Running Op = '{op_enum.value}' from thread '{threading.currentThread().getName()}'"
    )
    return True


def is_in_timeframe():
    """
    Function for determine if test should stop

    Returns:
        bool: True if END_TIME arrived, False otherwise
    """
    return datetime.now() < omadops_config.END_TIME


def run_user_ops(project_factory):
    """
    Run Longevity Stage3 as user ops in the background
    """

    # bg User Ops
    long = Longevity()
    log.info("Starting Longevity Stage3 execution")
    long.stage3(
        project_factory,
        num_of_pvc=30,
        num_of_obc=30,
        run_time=omadops_config.EXECUTION_TIME_HOURS * 60,
    )


def run_admin_ops(self):
    """
    Admin Ops function:
      - Runs while in timeframe (configured in config.EXECUTION_TIME_HOURS)
      - Steps:
        1. Loop through the list and check if elements are None or thread.is_alive() == False
        2. If (1) yes, pick randomly a admin Op and start it

      Note, if  omadops_config.NUM_OF_ADMIN_OPS_ASYNC == 1, no need to use threads as it's sequential

    TBD: replace dummy_func()
    """
    if not omadops_config.ADMIN_OPS:
        log.warning("Admin Ops list is empty!")
        return
    random.shuffle(omadops_config.TEMP_ADMIN_OPS_MATRIX)
    while is_in_timeframe():
        flow_to_be_tested = random.choice(omadops_config.TEMP_ADMIN_OPS_MATRIX)
        log.info(f"flow_to_be_tested: {flow_to_be_tested}")
        if (
            omadops_config.ADMIN_OPS_ASYNC
        ):  # Running multiple admin ops in parallel is allowed?
            for curr_idx, curr_op in enumerate(flow_to_be_tested):
                log.info(f"Running op {curr_op.value} multi threaded")
                omadops_config.CURRENT_ADMIN_OPS_LIST[curr_idx] = None
                omadops_config.CURRENT_ADMIN_OPS_LIST[
                    curr_idx
                ] = create_admin_op_thread(self, curr_op)
                omadops_config.CURRENT_ADMIN_OPS_LIST[curr_idx].start()
                time.sleep(omadops_config.SLEEP_TIMEOUT)

            for curr_idx, curr_op in enumerate(flow_to_be_tested):
                omadops_config.CURRENT_ADMIN_OPS_LIST[curr_idx].join()
        else:
            for curr_op in flow_to_be_tested:
                log.info(f"Running op {curr_op.value} with single thread")
                omadops_config.CURRENT_ADMIN_OPS_LIST[0] = None
                omadops_config.CURRENT_ADMIN_OPS_LIST[0] = create_admin_op_thread(
                    self, curr_op
                )
                omadops_config.CURRENT_ADMIN_OPS_LIST[0].start()
                omadops_config.CURRENT_ADMIN_OPS_LIST[0].join()
                time.sleep(omadops_config.SLEEP_TIMEOUT)

            time.sleep(omadops_config.SLEEP_TIMEOUT)
        omadops_config.FLOWSTESTED.append(flow_to_be_tested)
        if len(omadops_config.FLOWSTESTED) == omadops_config.TEMP_ADMIN_OPS_MATRIX:
            random.shuffle(omadops_config.TEMP_ADMIN_OPS_MATRIX)


def create_admin_op_thread(self, op_enum):
    name = str(uuid.uuid1()) + "_" + op_enum.value
    if op_enum == omadops_config.AdminOpsEnum.NODE_DRAIN:
        target = node_drain
        args = ()
    elif op_enum == omadops_config.AdminOpsEnum.NODE_REBOOT:
        target = node_reboot
        args = ()
    elif op_enum == omadops_config.AdminOpsEnum.SNAPSHOT_RESTORE:
        target = snapshot_restore
        args = (
            omadops_config.CONFIG_VARS["pvc_factory"],
            omadops_config.CONFIG_VARS["pvc_clone_factory"],
            omadops_config.CONFIG_VARS["snapshot_factory"],
            omadops_config.CONFIG_VARS["snapshot_restore_factory"],
        )
    else:
        target = dummy_func
        name = "Dummy function"
        args = (op_enum,)

    return threading.Thread(target=target, name=name, args=args)


def node_drain():
    node_to_drain = random.choice(node.get_osd_running_nodes())
    node.drain_nodes([node_to_drain])
    node.schedule_nodes([node_to_drain])


def node_reboot():
    nodes = PlatformNodesFactory().get_nodes_platform()
    cluster_nodes = get_nodes()
    node_to_reboot = random.choice(cluster_nodes)
    nodes.restart_nodes([node_to_reboot])


def snapshot_restore(
    pvc_factory, pvc_clone_factory, snapshot_factory, snapshot_restore_factory
):
    # Create a project
    # namespace = "test-snapshot-restore-admin-op-project"
    proj_obj = helpers.create_project()

    # from pdb import set_trace
    # set_trace()

    pvc_obj = pvc_factory(size=3, status=constants.STATUS_BOUND, project=proj_obj)
    pvc_clone_factory(pvc_obj)
    snap_obj = snapshot_factory(pvc_obj)
    #
    snapshot_restore_factory(
        snapshot_obj=snap_obj,
        volume_mode=snap_obj.parent_volume_mode,
    )

    # Delete the project
    switch_to_default_rook_cluster_project()
    try:
        proj_obj.delete(resource_name=proj_obj.namespace)
        proj_obj.wait_for_delete(resource_name=proj_obj.namespace, timeout=60, sleep=10)
    except CommandFailed:
        log.error(f"Cannot delete project {proj_obj.namespace}")
