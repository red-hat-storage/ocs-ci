"""
Test to verify PVC creation performance
"""
import logging
import pytest
import math
import ocs_ci.ocs.exceptions as ex
import ocs_ci.ocs.resources.pvc as pvc
import urllib.request
import time
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.testlib import (
    performance, E2ETest, polarion_id, bugzilla
)
from tests import helpers
from ocs_ci.ocs import defaults, constants
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@performance
class TestPVCCreationPerformance(E2ETest):
    """
    Test to verify PVC creation performance
    """
    pvc_size = '1Gi'

    @pytest.fixture()
    def base_setup(
        self, request, interface_iterate, storageclass_factory
    ):
        """
        A setup phase for the test

        Args:
            interface_iterate: A fixture to iterate over ceph interfaces
            storageclass_factory: A fixture to create everything needed for a
                storageclass
        """
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id('OCS-1225')
    @bugzilla('1740139')
    def test_pvc_creation_measurement_performance(self, teardown_factory):
        """
        Measuring PVC creation time
        """
        log.info('Start creating new PVC')

        pvc_obj = helpers.create_pvc(
            sc_name=self.sc_obj.name, size=self.pvc_size
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        pvc_obj.reload()
        teardown_factory(pvc_obj)
        create_time = helpers.measure_pvc_creation_time(
            self.interface, pvc_obj.name
        )
        if create_time > 1:
            raise ex.PerformanceException(
                f"PVC creation time is {create_time} and greater than 1 second"
            )
        logging.info("PVC creation took less than a 1 second")

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id('OCS-1620')
    @bugzilla('1741612')
    def test_multiple_pvc_creation_measurement_performance(
        self, teardown_factory
    ):
        """
        Measuring PVC creation time of 120 PVCs in 60 seconds
        """
        number_of_pvcs = 120
        log.info('Start creating new 120 PVCs')

        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=self.pvc_size,
        )
        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj,
                    constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)
        start_time = helpers.get_start_creation_time(
            self.interface, pvc_objs[0].name
        )
        end_time = helpers.get_end_creation_time(
            self.interface, pvc_objs[number_of_pvcs - 1].name,
        )
        total = end_time - start_time
        total_time = total.total_seconds()
        if total_time > 60:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation time is {total_time} and "
                f"greater than 60 seconds"
            )
        logging.info(
            f"{number_of_pvcs} PVCs creation time took less than a 60 seconds"
        )

    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id('OCS-1270')
    @bugzilla('1741612')
    def test_multiple_pvc_creation_after_deletion_performance(
        self, teardown_factory
    ):
        """
        Measuring PVC creation time of 75% of initial PVCs (120) in the same
        rate after deleting 75% of the initial PVCs
        """
        initial_number_of_pvcs = 120
        number_of_pvcs = math.ceil(initial_number_of_pvcs * 0.75)

        log.info('Start creating new 120 PVCs')
        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=initial_number_of_pvcs,
            size=self.pvc_size,
        )
        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor() as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj,
                    constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)
        log.info('Deleting 75% of the PVCs - 90 PVCs')
        assert pvc.delete_pvcs(pvc_objs[:number_of_pvcs], True), (
            "Deletion of 75% of PVCs failed"
        )
        log.info('Re-creating the 90 PVCs')
        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=self.pvc_size,
        )
        start_time = helpers.get_start_creation_time(
            self.interface, pvc_objs[0].name
        )
        end_time = helpers.get_end_creation_time(
            self.interface, pvc_objs[number_of_pvcs - 1].name,
        )
        total = end_time - start_time
        total_time = total.total_seconds()
        if total_time > 45:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation (after initial deletion of "
                f"75%) time is {total_time} and greater than 45 seconds"
            )
        logging.info(
            f"{number_of_pvcs} PVCs creation time took less than a 45 seconds"
        )


    def test_pvc_reattach_time_performance(self, pvc_factory, teardown_factory):
        """
        Test assign nodeName to a pod using RWX pvc
        Performance in test_multiple_pvc_creation_measurement_performance
        Each kernel (unzipped) is 892M and 61694 files
        """
        interface = constants.CEPHFILESYSTEM
        kernel_url = 'https://cdn.kernel.org/pub/linux/kernel/v4.x/linux-4.19.5.tar.gz'
        download_path = 'tmp'
        # Number of times we copy the kernel
        copies = 3

        # Download a linux Kernel
        import os
        dir_path = os.path.join(os.getcwd(), download_path)
        file_path = os.path.join(dir_path, 'file.gz')
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        urllib.request.urlretrieve(kernel_url, file_path)

        worker_nodes_list = helpers.get_worker_nodes()
        assert (len(worker_nodes_list) > 1)
        node_one = worker_nodes_list[0]
        node_two = worker_nodes_list[1]

        # Create a RWX PVC
        pvc_obj = pvc_factory(
            interface=interface, access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
            size='15',
        )

        # Create a pod on one node
        logging.info(
            f"Creating Pod with pvc {pvc_obj.name} on node {node_one}"
        )
        pod_obj1 = helpers.create_pod(
            interface_type=interface, pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace, node_name=node_one,
            pod_dict_path=constants.NGINX_POD_YAML
        )

        # Confirm that pod is running on the selected_nodes
        logging.info('Checking whether pods are running on the selected nodes')
        helpers.wait_for_resource_state(
            resource=pod_obj1, state=constants.STATUS_RUNNING,
            timeout=120
        )

        pod_name = pod_obj1.name
        pod_path = '/var/lib/www/html'

        _ocp = OCP(namespace=pvc_obj.namespace)
        rsh_cmd = f"rsync {dir_path} {pod_name}:{pod_path}"
        _ocp.exec_oc_cmd(rsh_cmd)
        # doesn't work!
        # ocp.rsync(src='/tmp/tocopy', dst=pod_path, node=pod_name)

        rsh_cmd = f"exec {pod_name} -- tar xvf {pod_path}/tmp/file.gz -C /var/lib/www/html/tmp"
        _ocp.exec_oc_cmd(rsh_cmd)

        for x in range(copies):
            rsh_cmd = f"exec {pod_name} -- mkdir -p {pod_path}/folder{x}"
            _ocp.exec_oc_cmd(rsh_cmd)
            rsh_cmd = f"exec {pod_name} -- cp -r {pod_path}/tmp {pod_path}/folder{x}"
            _ocp.exec_oc_cmd(rsh_cmd)

        rsh_cmd = f"delete pod {pod_name}"
        _ocp.exec_oc_cmd(rsh_cmd)

        start_time = time.time()

        logging.info(
            f"Creating Pod with pvc {pvc_obj.name} on node {node_two}"
        )

        pod_obj2 = helpers.create_pod(
            interface_type=interface, pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace, node_name=node_two,
            pod_dict_path=constants.NGINX_POD_YAML
        )
        pod_name = pod_obj2.name
        helpers.wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_RUNNING,
            timeout=120
        )
        end_time = time.time()
        total_time = end_time - start_time
        if total_time > 60:
            raise ex.PerformanceException(
                f"Pod creation time is {total_time} and "
                f"greater than 60 seconds"
            )
        logging.info(
            f"Pod {pod_name} creation time took {total_time} seconds"
        )

        teardown_factory(pod_obj2)
        os.remove(file_path)
        os.rmdir(dir_path)
