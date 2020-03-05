"""
Testcase to Create 200 PVC+POD scale in one namespace with and without IO
This will be Scale upto 1500 PVC+POD
Scale PVC and POD Configuration
---------------------------------------------------------
Number of PVC+POD Creation: 1500
PVC Type: All 4
POD Type: Nginx basic configuration, no DC POD
Cluster Capacity to be Utilized: 80%
Network or IO saturation: 80%
"""
import logging
import pytest
import random

from tests import helpers
from ocs_ci.ocs import constants, cluster, machine, node
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.exceptions import UnexpectedBehaviour, CephHealthException
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers

log = logging.getLogger(__name__)


@scale
@ignore_leftovers
@pytest.mark.parametrize(
    argnames="start_io",
    argvalues=[
        pytest.param(
            *[False], marks=pytest.mark.polarion_id("OCS-1357")
        ),
        pytest.param(
            *[True], marks=pytest.mark.polarion_id("OCS-1357")
        )
    ]
)
class TestMultiProjectScalePVCPOD(E2ETest):
    """
    Scale the OCS cluster to reach 1500 PVC+POD
    """
    @pytest.fixture()
    def setup_fixture(self, request):
        def finalizer():
            self.cleanup()

        request.addfinalizer(finalizer)

    def test_multi_project_scale_pvcs_pods(self, setup_fixture, start_io):
        """
        Test case to scale PVC+POD with multi projects and reach expected PVC count
        :param setup_fixture:
        :param start_io:
        :return:
        """

        # Set PVC count based on number of OCS workers
        ocs_nodes = node.get_typed_nodes(node_type='worker')
        pvc_count = {3: 1500, 6: 3000, 9: 4500}
        scale_count = pvc_count[len(ocs_nodes)]

        # Other variables
        rbd_sc_obj = helpers.default_storage_class(constants.CEPHBLOCKPOOL)
        cephfs_sc_obj = helpers.default_storage_class(constants.CEPHFILESYSTEM)
        pvc_count_each_itr = 5
        size = f"{random.randrange(5, 105, 5)}Gi"
        fio_size_param = '2G'
        fio_rate_param = '16k'
        self.namespace_list, all_pod_obj = ([] for i in range(2))

        # Pre-requisite check for number of OSD's and app worker nodes.
        helpers.add_required_osd_count(total_osd_nos=3)

        # Create machineset for app worker nodes, which will create one app worker node
        self.ms_name = machine.create_custom_machineset(instance_type='m5.4xlarge', zone='a')
        machine.wait_for_new_node_to_be_ready(self.ms_name)
        self.app_worker_nodes = machine.get_machine_from_machineset(self.ms_name)

        # Create namespace
        self.namespace_list.append(helpers.create_project())

        # Continue to iterate till the scale pvc limit is reached
        while True:
            if scale_count <= len(all_pod_obj):
                log.info(f"Scaled {scale_count} pvc and pods")
                break
            else:
                log.info(f"Create {pvc_count_each_itr} pods & pvc")
                pod_obj, pvc_obj = helpers.create_multi_pvc_pod(
                    self.namespace_list[-1].name, rbd_sc_obj, cephfs_sc_obj, pvc_count_each_itr,
                    size, fio_rate=fio_rate_param, start_io=start_io, fio_size=fio_size_param,
                    fio_runtime=3600, node_selector=constants.SCALE_NODE_SELECTOR
                )
                all_pod_obj.extend(pod_obj)
                try:
                    # Check enough resources available in the dedicated app workers
                    if helpers.add_worker_based_on_cpu_utilization(
                        machineset_name=self.ms_name, node_count=1, expected_percent=75,
                        role_type='app,worker'
                    ):
                        logging.info(f"Nodes added for app pod creation")
                    else:
                        logging.info(f"Existing resource are enough to create more pods")

                    # Check for ceph cluster OSD utilization
                    if not cluster.validate_osd_utilization(osd_used=80):
                        logging.info("Cluster OSD utilization is below 80")
                    else:
                        raise CephHealthException("Cluster OSDs are near full")

                    # Change fio_size and fio_rate param according to env
                    fio_size_param = helpers.suggest_io_size_based_on_cls_usage()
                    fio_rate_param = helpers.suggest_fio_rate_based_on_cls_iops()

                    # Check for pg_balancer
                    if cluster.validate_pg_balancer():
                        logging.info("OSD consumption and PG distribution is good to continue")
                    else:
                        raise UnexpectedBehaviour("Unequal PG distribution to OSDs")

                    # Check for 200 pods per namespace
                    pod_objs = pod.get_all_pods(namespace=self.namespace_list[-1].name)
                    if len(pod_objs) >= 200:
                        self.namespace_list.append(helpers.create_project())

                except UnexpectedBehaviour:
                    logging.error(
                        f"Scaling of cluster failed after {len(all_pod_obj)} pod creation"
                    )
                    raise UnexpectedBehaviour(
                        f"Scaling PVC+POD failed analyze setup and log for more details"
                    )

    def cleanup(self):
        """
        Function to tear down
        """
        # Delete all pods, pvcs and namespaces
        # TODO: Add checks for resource delete success.
        for namespace in self.namespace_list:
            helpers.delete_objs_parallel(pod.get_all_pods(namespace=namespace.name))
            helpers.delete_objs_parallel(pvc.get_all_pvc_objs(namespace=namespace.name))
            namespace.delete()
        # Delete machineset which will delete respective nodes too
        machine.delete_custom_machineset(self.ms_name)
