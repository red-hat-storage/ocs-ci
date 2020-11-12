import os
import logging
import subprocess
from ocs_ci.framework import config
from ocs_ci.ocs.resources import  pod
from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.framework.testlib import (
    skipif_ocs_version, skipif_ocp_version, E2ETest, performance
)

log = logging.getLogger(__name__)


@performance
@skipif_ocp_version('<4.6')
@skipif_ocs_version('<4.6')
class TestPvcMultiClonePerformance(E2ETest):
    """
    Tests to measure PVC clones creation performance
    The test is supposed to create the maximum number of snapshot for one PVC
    """

    def test_pvc_multiple_clone_performance(
        self, interface_iterate, teardown_factory, storageclass_factory,
        pvc_factory, pod_factory
    ):
        """
        1. Creating PVC
           PVS size is calculated in the test and depends on the storage capacity, but not less then 1 GiB
           it will use ~75% capacity of the Storage, Min storage capacity 1 TiB
        2. Fill the PVC with 70% of data
        3. Take a clone of the PVC and measure time and speed of creation.
        4. repeat the previous step number of times (maximal num_of_clones is 512)
           this will be run by outside script for low memory consumption
        5. print all measured statistics for all the clones.

        Raises:
            StorageNotSufficientException: in case of not enough capacity

        """
        num_of_clones = 512
        if interface_iterate == constants.CEPHBLOCKPOOL:
            num_of_clones = 450 # bz_1896831

        # Getting the total Storage capacity
        ceph_cluster = CephCluster()
        ceph_capacity = int(ceph_cluster.get_ceph_capacity())

        # Use 70% of the storage capacity in the test
        capacity_to_use = int(ceph_capacity * 0.7)

        # since we do not want to use more then 65%, we add 35% to the needed
        # capacity, and minimum PVC size is 1 GiB
        need_capacity = int((num_of_clones + 2) * 1.35)
        # Test will run only on system with enough capacity
        if capacity_to_use < need_capacity:
            err_msg = (
                f'The system have only {ceph_capacity} GiB, '
                f'we want to use only {capacity_to_use} GiB, '
                f'and we need {need_capacity} GiB to run the test'
            )
            log.error(err_msg)
            raise exceptions.StorageNotSufficientException(err_msg)

        # Calculating the PVC size in GiB
        pvc_size = int(capacity_to_use / (num_of_clones + 2))

        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

        self.pvc_obj = pvc_factory(
            interface=self.interface,
            size=pvc_size,
            status=constants.STATUS_BOUND
        )

        self.pod_obj = pod_factory(
            interface=self.interface,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING
        )

        # Calculating the file size as 70% of the PVC size
        filesize = self.pvc_obj.size * 0.70
        # Change the file size to MB for the FIO function
        file_size = f'{int(filesize * constants.GB2MB)}M'
        file_name = self.pod_obj.name

        log.info(
            f'Total capacity size is : {ceph_capacity} GiB, '
            f'Going to use {need_capacity} GiB, '
            f'With {num_of_clones} clones to {pvc_size} GiB PVC. '
            f'File size to be written is : {file_size} '
            f'with the name of {file_name}'
        )

        os.environ["CLONENUM"] = f'{num_of_clones}'
        os.environ["LOGPATH"] = f'{ocsci_log_path()}'
        os.environ["FILESIZE"] = file_size
        os.environ["NSPACE"] = self.pvc_obj.namespace
        os.environ["PODNAME"] = self.pod_obj.name
        os.environ["PVCNAME"] = self.pvc_obj.name
        os.environ["PVCSIZE"] = str(self.pvc_obj.size)
        os.environ["SCNAME"] = self.pvc_obj.backed_sc
        os.environ["INTERFACE"] = self.interface
        os.environ["CLUSTERPATH"] = config.ENV_DATA['cluster_path']

        self.run_fio_on_pod(file_size)

        main_script = "tests/e2e/performance/test_multi_clones.py"
        result = subprocess.run([main_script], stdout=subprocess.PIPE)
        log.info(f"Results from main script : {result.stdout.decode('utf-8')}")

        if 'All results are' not in result.stdout.decode('utf-8'):
            log.error('Test was not completed')
            raise Exception('Test was not completed')

        # TODO: push all results to elasticsearch server

    def run_fio_on_pod(self, file_size):
        """
        Args:
         file_size (str): Size of file to write in MB, e.g. 200M or 50000M

        """
        file_name = self.pod_obj.name
        log.info(f'Starting IO on the POD {self.pod_obj.name}')
        print(f'Starting IO on the POD {self.pod_obj.name}')
        # Going to run only write IO to fill the PVC for the before creating a clone
        self.pod_obj.fillup_fs(size=file_size, fio_filename=file_name)

        # Wait for fio to finish
        fio_result = self.pod_obj.get_fio_results(timeout=18000)
        err_count = fio_result.get('jobs')[0].get('error')
        assert err_count == 0, (
            f"IO error on pod {self.pod_obj.name}. "
            f"FIO result: {fio_result}."
        )
        log.info('IO on the PVC Finished')
        print('IO on the PVC Finished')

        # Verify presence of the file on pvc
        file_path = pod.get_file_path(self.pod_obj, file_name)
        log.info(f"Actual file path on the pod is {file_path}.")
        assert pod.check_file_existence(self.pod_obj, file_path), (
            f"File {file_name} does not exist"
        )
        log.info(f"File {file_name} exists in {self.pod_obj.name}.")
