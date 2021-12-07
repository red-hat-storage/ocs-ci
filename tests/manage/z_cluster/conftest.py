# -*- coding: utf8 -*-

import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.ocs import constants

from ocs_ci.ocs.fiojob import workload_fio_storageutilization


logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def workload_storageutilization_rbd(
    request,
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    supported_configuration,
    threading_lock,
):
    """
    In order to use this fixture you need to pass 3 indirect parameters:
    target_percentage (float): the percentage storage utilization(from 0.01 to 0.99).
    keep_fio_data (bool): indicate if you want to keep the fio data after the test is finished.
    minimal_time (int): Minimal number of seconds to monitor a system
    (See more details in the function 'measure_operation').

    For example: Let's say I want to use workload_storageutilization_rbd fixture with
    'target_percentage'=0.25, 'keep_fio_data'=True, 'minimal_time'=120
    then In my test I will specify these parameters:
    @pytest.mark.parametrize("workload_storageutilization_rbd",
    [(0.25, True, 120)], indirect=["workload_storageutilization_rbd"])
    """

    target_percentage, keep_fio_data, minimal_time = request.param
    percent_to_fill = int(target_percentage * 100)
    fixture_name = f"workload_storageutilization_{percent_to_fill}p_rbd"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=target_percentage,
        keep_fio_data=keep_fio_data,
        minimal_time=minimal_time,
        threading_lock=threading_lock,
    )
    return measured_op


@pytest.fixture(scope="function")
def workload_storageutilization_cephfs(
    request,
    project,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
    measurement_dir,
    tmp_path,
    supported_configuration,
    threading_lock,
):
    """
    In order to use this fixture you need to pass 3 indirect parameters:
    target_percentage (float): the percentage storage utilization(from 0.01 to 0.99).
    keep_fio_data (bool): indicate if you want to keep the fio data after the test is finished.
    minimal_time (int): Minimal number of seconds to monitor a system
    (See more details in the function 'measure_operation').

    For example: Let's say I want to use workload_storageutilization_cephfs fixture with
    'target_percentage'=0.25, 'keep_fio_data'=True, 'minimal_time'=120
    then In my test I will specify these parameters:
    @pytest.mark.parametrize("workload_storageutilization_cephfs",
    [(0.25, True, 120)], indirect=["workload_storageutilization_cephfs"])
    """

    target_percentage, keep_fio_data, minimal_time = request.param
    percent_to_fill = int(target_percentage * 100)
    fixture_name = f"workload_storageutilization_{percent_to_fill}p_cephfs"
    measured_op = workload_fio_storageutilization(
        fixture_name,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        target_percentage=target_percentage,
        keep_fio_data=keep_fio_data,
        minimal_time=minimal_time,
        threading_lock=threading_lock,
    )
    return measured_op


def pytest_collection_modifyitems(items):
    """
    A pytest hook to skip certain tests when running on
    openshift dedicated platform
    Args:
        items: list of collected tests
    """
    # Skip the below test till node implementaion completed for ODF-MS platform
    skip_till_node_implement = [
        "test_nodereplacement_proactive",
        "test_pv_provisioning_under_degraded_state_stop_rook_operator_pod_node",
        "test_pv_after_reboot_node",
        "test_add_capacity_node_restart",
        "test_nodes_restart",
        "test_rolling_nodes_restart",
        "test_pv_provisioning_under_degraded_state_stop_provisioner_pod_node",
        "test_pv_provisioning_under_degraded_state_stop_rook_operator_pod_node",
        "test_toleration",
        "test_node_maintenance_restart_activate",
        "test_simultaneous_drain_of_two_ocs_nodes",
        "test_all_worker_nodes_short_network_failure",
        "test_check_pod_status_after_two_nodes_shutdown_recovery",
    ]
    if config.ENV_DATA["platform"].lower() in constants.MANAGED_SERVICE_PLATFORMS:
        for item in items.copy():
            for testname in skip_till_node_implement:
                if testname in str(item.fspath):
                    logger.info(
                        f"Test {item} is removed from the collected items"
                        f" till node implentation is in place"
                    )
                    items.remove(item)
                    break
