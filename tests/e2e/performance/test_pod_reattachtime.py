import logging
import pytest
import ocs_ci.ocs.exceptions as ex
import urllib.request
import time
from ocs_ci.framework.testlib import (
    performance, E2ETest
)
from tests import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


@pytest.mark.polarion_id("OCS-2208")
@performance
class TestPVCCreationPerformance(E2ETest):
    """
    Test to verify PVC creation performance
    """

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

        if self.interface.lower() == 'cephfs':
            self.interface = constants.CEPHFILESYSTEM
        if self.interface.lower() == 'rbd':
            self.interface = constants.CEPHBLOCKPOOL
        self.sc_obj = storageclass_factory(self.interface)

    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_reattach_time_performance(self, pvc_factory, teardown_factory):
        """
        Test assign nodeName to a pod using RWX pvc
        Performance in test_multiple_pvc_creation_measurement_performance
        Each kernel (unzipped) is 892M and 61694 files
        """

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

        # Create a PVC
        accessmode = constants.ACCESS_MODE_RWX
        if self.interface == constants.CEPHBLOCKPOOL:
            accessmode = constants.ACCESS_MODE_RWO
        pvc_obj = pvc_factory(
            interface=self.interface, access_mode=accessmode,
            status=constants.STATUS_BOUND,
            size='15',
        )

        # Create a pod on one node
        logging.info(
            f"Creating Pod with pvc {pvc_obj.name} on node {node_one}"
        )

        helpers.pull_images('nginx')
        pod_obj1 = helpers.create_pod(
            interface_type=self.interface, pvc_name=pvc_obj.name,
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

        rsh_cmd = f"exec {pod_name} -- apt-get update"
        _ocp.exec_oc_cmd(rsh_cmd)
        rsh_cmd = f"exec {pod_name} -- apt-get install -y rsync"
        _ocp.exec_oc_cmd(rsh_cmd, ignore_error=True, out_yaml_format=False)

        rsh_cmd = f"rsync {dir_path} {pod_name}:{pod_path}"
        _ocp.exec_oc_cmd(rsh_cmd)

        rsh_cmd = f"exec {pod_name} -- tar xvf {pod_path}/tmp/file.gz -C /var/lib/www/html/tmp"
        _ocp.exec_oc_cmd(rsh_cmd)

        for x in range(copies):
            rsh_cmd = f"exec {pod_name} -- mkdir -p {pod_path}/folder{x}"
            _ocp.exec_oc_cmd(rsh_cmd)
            rsh_cmd = f"exec {pod_name} -- cp -r {pod_path}/tmp {pod_path}/folder{x}"
            _ocp.exec_oc_cmd(rsh_cmd)

        rsh_cmd = f"delete pod {pod_name}"
        _ocp.exec_oc_cmd(rsh_cmd)

        logging.info(
            f"Creating Pod with pvc {pvc_obj.name} on node {node_two}"
        )

        pod_obj2 = helpers.create_pod(
            interface_type=self.interface, pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace, node_name=node_two,
            pod_dict_path=constants.NGINX_POD_YAML
        )

        start_time = time.time()

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
