# -*- coding: utf8 -*-

import logging
import pytest
from tests.manage.monitoring.conftest import measurement_dir


from ocs_ci.ocs.fiojob import workload_fio_storageutilization


logger = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def workload_storageutilization_rbd(
        request,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    """
    In order to use this fixture you need to pass 3 indirect parameters:
    target_percentage(float): the percentage storage utilization(from 0.01 to 0.99).
    delete_fio_data_after_test(bool): indicate if you want to delete the fio data after the test is finished.
    minimal_time(int): Minimal number of seconds to monitor a system
    (See more details in the function 'measure_operation').

    For example: Let's say I want to use workload_storageutilization_rbd fixture with
    'target_percentage'=0.25, 'delete_fio_job_file'=True, 'minimal_time'=120
    then In my test I will specify these parameters:
    @pytest.mark.parametrize("workload_storageutilization_rbd",
    [(0.25, False, 120)], indirect=["workload_storageutilization_rbd"])
    """

    target_percentage, delete_fio_data_after_test, minimal_time = request.param
    percent_to_fill = int(target_percentage*100)
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
        delete_fio_data_after_test=delete_fio_data_after_test,
        request=request,
        minimal_time=minimal_time,
    )
    return measured_op


@pytest.fixture(scope='function')
def workload_storageutilization_cephfs(
        request,
        project,
        fio_pvc_dict,
        fio_job_dict,
        fio_configmap_dict,
        measurement_dir,
        tmp_path,
        supported_configuration):
    """
    In order to use this fixture you need to pass 3 indirect parameters:
    target_percentage(float): the percentage storage utilization(from 0.01 to 0.99).
    delete_fio_data_after_test(bool): indicate if you want to delete the fio data after the test is finished.
    minimal_time(int): Minimal number of seconds to monitor a system
    (See more details in the function 'measure_operation').

    For example: Let's say I want to use workload_storageutilization_cephfs fixture with
    'target_percentage'=0.25, 'delete_fio_job_file'=True, 'minimal_time'=120
    then In my test I will specify these parameters:
    @pytest.mark.parametrize("workload_storageutilization_cephfs",
    [(0.25, False, 120)], indirect=["workload_storageutilization_cephfs"])
    """

    target_percentage, delete_fio_data_after_test, minimal_time = request.param
    percent_to_fill = int(target_percentage*100)
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
        delete_fio_data_after_test=delete_fio_data_after_test,
        request=request,
        minimal_time=minimal_time,
    )
    return measured_op
