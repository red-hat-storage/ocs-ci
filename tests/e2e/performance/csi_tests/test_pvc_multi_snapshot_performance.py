import os
import logging
import subprocess

import pytest

from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    performance,
)

log = logging.getLogger(__name__)

SKIP_REASON = "Test is re-written to fix teardown issues, Hence skipping this test"


@performance
@skipif_ocp_version("<4.6")
@skipif_ocs_version("<4.6")
class TestPvcMultiSnapshotPerformance(E2ETest):
    """
    Tests to measure PVC snapshots creation performance & scale
    The test is trying to to take the maximum number of snapshot for one PVC
    """

    @pytest.mark.skip(SKIP_REASON)
    @pytest.mark.polarion_id("OCS-2623")
    def test_pvc_multiple_snapshot_performance(
        self,
        interface_iterate,
        teardown_factory,
        storageclass_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        1. Creating PVC
           size is depend on storage capacity, but not less then 1 GiB
           it will use ~75% capacity of the Storage, Min storage capacity 1 TiB
        2. Fill the PVC with 80% of data
        3. Take a snapshot of the PVC and measure the time of creation.
        4. re-write the data on the PVC
        5. Take a snapshot of the PVC and measure the time of creation.
        6. repeat steps 4-5 the numbers of snapshot we want to take : 512
           this will be run by outside script for low memory consumption
        7. print all information.

        Raises:
            StorageNotSufficientException: in case of not enough capacity

        """
        # Number od snapshot for CephFS is 100 and for RBD is 512
        num_of_snaps = 100
        if interface_iterate == constants.CEPHBLOCKPOOL:
            num_of_snaps = 512

        # Getting the total Storage capacity
        ceph_cluster = CephCluster()
        ceph_capacity = int(ceph_cluster.get_ceph_capacity())

        # Use 70% of the storage capacity in the test
        capacity_to_use = int(ceph_capacity * 0.7)

        # since we do not want to use more then 65%, we add 35% to the needed
        # capacity, and minimum PVC size is 1 GiB
        need_capacity = int((num_of_snaps + 2) * 1.35)
        # Test will run only on system with enough capacity
        if capacity_to_use < need_capacity:
            err_msg = (
                f"The system have only {ceph_capacity} GiB, "
                f"we want to use only {capacity_to_use} GiB, "
                f"and we need {need_capacity} GiB to run the test"
            )
            log.error(err_msg)
            raise exceptions.StorageNotSufficientException(err_msg)

        # Calculating the PVC size in GiB
        pvc_size = int(capacity_to_use / (num_of_snaps + 2))

        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

        self.pvc_obj = pvc_factory(
            interface=self.interface, size=pvc_size, status=constants.STATUS_BOUND
        )

        self.pod_obj = pod_factory(
            interface=self.interface, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

        # Calculating the file size as 80% of the PVC size
        filesize = self.pvc_obj.size * 0.80
        # Change the file size to MB for the FIO function
        file_size = f"{int(filesize * constants.GB2MB)}M"
        file_name = self.pod_obj.name

        log.info(
            f"Total capacity size is : {ceph_capacity} GiB, "
            f"Going to use {need_capacity} GiB, "
            f"With {num_of_snaps} Snapshots to {pvc_size} GiB PVC. "
            f"File size to be written is : {file_size} "
            f"with the name of {file_name}"
        )

        os.environ["SNAPNUM"] = f"{num_of_snaps}"
        os.environ["LOGPATH"] = f"{ocsci_log_path()}"
        os.environ["FILESIZE"] = file_size
        os.environ["NSPACE"] = self.pvc_obj.namespace
        os.environ["PODNAME"] = self.pod_obj.name
        os.environ["PVCNAME"] = self.pvc_obj.name
        os.environ["INTERFACE"] = self.interface

        main_script = "tests/e2e/performance/csi_tests/test_multi_snapshots.py"
        result = subprocess.run([main_script], stdout=subprocess.PIPE)
        log.info(f"Results from main script : {result.stdout.decode('utf-8')}")

        if "All results are" not in result.stdout.decode("utf-8"):
            log.error("Test did not completed")
            raise Exception("Test did not completed")

        # TODO: push all results to elasticsearch server
