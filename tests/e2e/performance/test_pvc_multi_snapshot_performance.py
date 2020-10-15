import logging

from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.framework.testlib import (
    skipif_ocs_version, skipif_ocp_version, E2ETest, performance
)
from ocs_ci.ocs.resources import pvc
from tests import helpers

log = logging.getLogger(__name__)


@performance
@skipif_ocp_version('<4.6')
@skipif_ocs_version('<4.6')
class TestPvcMultiSnapshotPerformance(E2ETest):
    """
    Tests to measure PVC snapshots creation performance & scale
    The test is trying to to take the maximum number of snapshot for one PVC
    """

    def test_pvc_multiple_snapshot_performance(self, interface_iterate,
                                               teardown_factory,
                                               storageclass_factory,
                                               pvc_factory,
                                               pod_factory
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
        7. print all information.

        Raise:
            StorageNotSufficientException: in case of not enough capacity

        """
        create_times = []  # list for all snapshots results
        num_of_snaps = 512  # number of snapshots to take

        # Getting the total Storage capacity
        ceph_cluster = CephCluster()
        ceph_capacity = ceph_cluster.get_ceph_capacity()

        # Use 70% of the storage capacity in the test
        capacity_2_use = int(ceph_capacity * 0.7)

        # since we do not want to use more then 75%, we add 50% to the needed
        # capacity, and minimum PVC size is 1 GiB
        need_capacity = int((num_of_snaps + 2) * 1.5)
        # Test will run only on system with enough capacity
        if capacity_2_use < need_capacity:
            log.error(
                f'The system have only {ceph_capacity} GiB, '
                f'we want to use only {capacity_2_use} GiB, '
                f'and we need {need_capacity} GiB to run the test'
            )
            raise exceptions.StorageNotSufficientException

        # Calculating the PVC size in GiB
        pvc_size = int(capacity_2_use / (num_of_snaps + 2))

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

        # Calculating the file size as 80% of the PVC size
        filesize = self.pvc_obj.size * 0.80
        # Change the file size to MB for the FIO function
        file_size = f'{int(filesize * constants.GB2MB)}M'
        file_name = self.pod_obj.name

        log.info(
            f'Total capacity size is : {ceph_capacity} GiB, '
            f'Going to use {need_capacity} GiB, '
            f'With {num_of_snaps} Snapshots to {pvc_size} GiB PVC. '
            f'File size to be written is : {file_size} '
            f'with the name of {file_name}'
        )

        snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
        if self.interface == constants.CEPHFILESYSTEM:
            snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        for test_num in range(1, (num_of_snaps + 1)):

            test_results = {
                'test_num': test_num,
                'create': {'time': None, 'speed': None},
            }

            # Going to run only write IO to fill the PVC for the snapshot - will run 1M BS for 3 Min. 1M
            log.info(f"Running IO on pod {self.pod_obj.name} - Test number {test_num}")
            self.pod_obj.fillup_fs(size=file_size, fio_filename=file_name)

            # Wait for fio to finish
            fio_result = self.pod_obj.get_fio_results()
            err_count = fio_result.get('jobs')[0].get('error')
            assert err_count == 0, (
                f"IO error on pod {self.pod_obj.name}. "
                f"FIO result: {fio_result}"
            )
            log.info('IO on the PVC Finished')

            # Take a snapshot of the PVC and measure the time of creation.
            snap_name = self.pvc_obj.name.replace(
                'pvc-test', f'snapshot-test{test_num}'
            )
            log.info(f'Taking snapshot of the PVC {snap_name}')
            snap_obj = pvc.create_pvc_snapshot(
                self.pvc_obj.name,
                snap_yaml,
                snap_name,
                helpers.default_volumesnapshotclass(self.interface).name,
            )
            snap_obj.ocp.wait_for_resource(
                condition='true', resource_name=snap_obj.name,
                column=constants.STATUS_READYTOUSE, timeout=60
            )

            snap_con_name = snap_obj.ocp.get(
                resource_name=snap_name,
                out_yaml_format=True
            )["status"]["boundVolumeSnapshotContentName"]
            log.info(f'Snap content name is {snap_con_name}')

            test_results['create']['time'] = helpers.measure_snapshot_creation_time(
                self.interface, snap_obj.name, snap_con_name
            )
            test_results['create']['speed'] = int(
                filesize * constants.GB2MB / test_results['create']['time']
            )
            log.info(f' Test {test_num} results:')
            log.info(f'Snapshot creation time is : {test_results["create"]["time"]} sec.')
            log.info(f'Snapshot speed is : {test_results["create"]["speed"]} MB/sec')
            create_times.append(test_results)

        logging.info(f"Snapshot created results are:  {create_times}")

        # TODO: push all results to elasticsearch server
