from ocs_ci.framework.pytest_customization.marks import polarion_id
from ocs_ci.framework.testlib import ignore_leftovers, ManageTest, tier2
from ocs_ci.ocs.resources.pod import get_pod_obj, delete_pods
from ocs_ci.ocs.resources.pvc import get_pvc_obj, delete_pvcs
import logging
import pytest
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import get_osd_utilization
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.ocs.resources.storage_cluster import check_rebalance_occur_after_expand
from ocs_ci.ocs.resources.storage_cluster import check_until_osd_ratio_start_decrease_or_equal
from time import sleep
from ocs_ci.utility.retry import retry
from ocs_ci.ocs.exceptions import ClusterUtilizationNotBalanced


logger = logging.getLogger(__name__)

@ignore_leftovers
@tier2
@polarion_id('OCS-xxx')




@pytest.mark.parametrize("workload_storageutilization_rbd",
                         [(0.11, True, 5)], indirect=["workload_storageutilization_rbd"])
class TestRebalaceAddCapacity(ManageTest):
    """
    Run io till 9% of all cluster, add capacity to the cluster and check the rebalance is initializing
    """

    def test_rebalance_add_capacity(self, workload_storageutilization_rbd):

        osds_before_expand=[]
        osd_util_dict = get_osd_utilization()
        logger.info(f"Checking osds utilization")
        for osd_name, osd_util in osd_util_dict.items():
            osds_before_expand.append(osd_name)
            logger.info(f"osd {osd_name} has {osd_util} util")
        num_of_osd = len(osds_before_expand)
        # wait for data to stop increasing
        check_until_osd_ratio_start_decrease_or_equal(osds_before_expand, digit_point=2, ratio_stable=1)
        logger.info(f"Adding capacity to the cluster")
        osd_size = storage_cluster.get_osd_size()
        result = storage_cluster.add_capacity(osd_size)
        pod = OCP(
            kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
        )
        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_RUNNING,
            selector='app=rook-ceph-osd',
            resource_count=result * 3
        )

        pod.wait_for_resource(
            timeout=300,
            condition=constants.STATUS_COMPLETED,
            selector=constants.OSD_PREPARE_APP_LABEL,
            resource_count=result * 3
        )
        num_of_osd_factor = num_of_osd / 3
        #Waiting for old osd sum of utilization to go down
        check_until_osd_ratio_start_decrease_or_equal(osds_before_expand)
        check_rebalance_occur_after_expand_with_retry = retry(ClusterUtilizationNotBalanced,
                                                              tries=(10*num_of_osd_factor),
                                                              delay=30, backoff=1)(check_rebalance_occur_after_expand)
        check_rebalance_occur_after_expand_with_retry(osds_before_expand)

        #check_rebalance_occur_after_expand(osds_before_expand)
        logger.info(f"Deleting pod and pvc")
        ocp = OCP()
        get_all_pvc_output = ocp.exec_oc_cmd('get pvc -A -o json')
        for pvc_dict in get_all_pvc_output['items']:
            logger.info(f"{pvc_dict['metadata']['name']}")
            if pvc_dict['metadata']['name'] == 'fio-target':
                logger.info(f"The project name is {pvc_dict['metadata']['namespace']}")
                test_namespace = pvc_dict['metadata']['namespace']

        from ocs_ci.ocs.utils import get_pod_name_by_pattern
        fio_pod_name = get_pod_name_by_pattern(pattern='fio', namespace=test_namespace)
        fio_pod_obj = get_pod_obj(fio_pod_name[0], test_namespace)
        delete_pods([fio_pod_obj], True)
        pvc_obj = get_pvc_obj('fio-target', test_namespace)
        delete_pvcs([pvc_obj])
        #ocp.delete_project(test_namespace)
        ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace'], tries=80
        )
        logger.info(f"Waiting for one minute to check ceph health again")
        sleep(60)
        ceph_health_check(
            namespace=config.ENV_DATA['cluster_namespace'], tries=80
        )

