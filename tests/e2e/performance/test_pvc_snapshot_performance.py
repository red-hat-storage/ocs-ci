# Builtin modules
import logging
import time

# 3ed party modules
import pytest
import statistics

# Local modules
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import constants
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.ripsaw import RipSaw
from ocs_ci.ocs.version import get_environment_info
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.helpers import helpers
from ocs_ci.framework.testlib import (
    skipif_ocp_version,
    skipif_ocs_version,
    E2ETest,
    performance,
)
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def ripsaw(request, storageclass_factory):
    def teardown():
        ripsaw.cleanup()
        time.sleep(10)

    request.addfinalizer(teardown)

    ripsaw = RipSaw()

    return ripsaw


@performance
@skipif_ocp_version("<4.6")
@skipif_ocs_version("<4.6")
class TestPvcSnapshotPerformance(E2ETest):
    """
    Tests to verify PVC snapshot creation and deletion performance
    """

    tests_numbers = 3  # number of tests to run

    @pytest.fixture()
    def base_setup(
        self,
        request,
        interface_iterate,
        storageclass_factory,
        pvc_factory,
        pod_factory,
        pvc_size,
    ):
        """
        A setup phase for the test - creating resources

        Args:
            interface_iterate: A fixture to iterate over ceph interfaces
            storageclass_factory: A fixture to create everything needed for a
                storageclass
            pvc_factory: A fixture to create new pvc
            pod_factory: A fixture to create new pod
            pvc_size: The size of the PVC in Gi

        """
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

        self.pvc_obj = pvc_factory(
            interface=self.interface, size=pvc_size, status=constants.STATUS_BOUND
        )

        self.pod_obj = pod_factory(
            interface=self.interface, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

    def measure_create_snapshot_time(self, pvc_name, snap_name, interface):
        """
        Creation volume snapshot, and measure the creation time

        Args:
            pvc_name (str): the PVC name to create a snapshot of
            snap_name (str): the name of the snapshot to be created
            interface (str): the interface (rbd / cephfs) to used

        Returns:
            int : the snapshot creation time in seconds

        """

        # Find the snapshot yaml according to the interface
        snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
        if interface == constants.CEPHFILESYSTEM:
            snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML

        # Create the Snapshot of the PVC
        self.snap_obj = pvc.create_pvc_snapshot(
            pvc_name=pvc_name,
            snap_yaml=snap_yaml,
            snap_name=snap_name,
            sc_name=helpers.default_volumesnapshotclass(interface).name,
        )

        # Wait until the snapshot is bound and ready to use
        self.snap_obj.ocp.wait_for_resource(
            condition="true",
            resource_name=self.snap_obj.name,
            column=constants.STATUS_READYTOUSE,
            timeout=600,
        )

        # Getting the snapshot content name
        self.snap_content = helpers.get_snapshot_content_obj(self.snap_obj)
        self.snap_uid = (
            self.snap_content.data.get("spec").get("volumeSnapshotRef").get("uid")
        )
        log.info(f"The snapshot UID is :{self.snap_uid}")

        # Measure the snapshot creation time
        c_time = helpers.measure_snapshot_creation_time(
            interface, snap_name, self.snap_content.name, self.snap_uid
        )
        return c_time

    @pytest.mark.parametrize(
        argnames=["pvc_size"],
        argvalues=[pytest.param(*["1"]), pytest.param(*["10"]), pytest.param(*["100"])],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_snapshot_performance(self, teardown_factory, pvc_size):
        """
        1. Run I/O on a pod file.
        2. Calculate md5sum of the file.
        3. Take a snapshot of the PVC and measure the time of creation.
        4. Restore From the snapshot and measure the time
        5. Attach a new pod to it.
        6. Verify that the file is present on the new pod also.
        7. Verify that the md5sum of the file on the new pod matches
           with the md5sum of the file on the original pod.

        This scenario run 3 times and report all results
        Args:
            teardown_factory: A fixture to destroy objects
            pvc_size: the size of the PVC to be tested - parametrize

        """

        # Getting the total Storage capacity
        ceph_cluster = CephCluster()
        ceph_capacity = ceph_cluster.get_ceph_capacity()

        log.info(f"Total capacity size is : {ceph_capacity}")
        log.info(f"PVC Size is : {pvc_size}")
        log.info(f"Needed capacity is {int(int(pvc_size) * 5)}")
        if int(ceph_capacity) < int(pvc_size) * 5:
            log.error(
                f"PVC size is {pvc_size}GiB and it is too large for this system"
                f" which have only {ceph_capacity}GiB"
            )
            return
        # Calculating the file size as 25% of the PVC size
        # in the end the PVC will be 75% full
        filesize = self.pvc_obj.size * 0.25
        # Change the file size to MB and from int to str
        file_size = f"{int(filesize * 1024)}M"

        all_results = []

        for test_num in range(self.tests_numbers):
            test_results = {
                "test_num": test_num + 1,
                "dataset": (test_num + 1) * filesize * 1024,  # size in MiB
                "create": {"time": None, "speed": None},
                "restore": {"time": None, "speed": None},
            }
            log.info(f"Starting test phase number {test_num}")
            # Step 1. Run I/O on a pod file.
            file_name = f"{self.pod_obj.name}-{test_num}"
            log.info(f"Starting IO on the POD {self.pod_obj.name}")
            # Going to run only write IO to fill the PVC for the snapshot
            self.pod_obj.fillup_fs(size=file_size, fio_filename=file_name)

            # Wait for fio to finish
            fio_result = self.pod_obj.get_fio_results()
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"IO error on pod {self.pod_obj.name}. FIO result: {fio_result}"
            log.info("IO on the PVC Finished")

            # Verify presence of the file
            file_path = pod.get_file_path(self.pod_obj, file_name)
            log.info(f"Actual file path on the pod {file_path}")
            assert pod.check_file_existence(
                self.pod_obj, file_path
            ), f"File {file_name} doesn't exist"
            log.info(f"File {file_name} exists in {self.pod_obj.name}")

            # Step 2. Calculate md5sum of the file.
            orig_md5_sum = pod.cal_md5sum(self.pod_obj, file_name)

            # Step 3. Take a snapshot of the PVC and measure the time of creation.
            snap_name = self.pvc_obj.name.replace(
                "pvc-test", f"snapshot-test{test_num}"
            )
            log.info(f"Taking snapshot of the PVC {snap_name}")

            test_results["create"]["time"] = self.measure_create_snapshot_time(
                pvc_name=self.pvc_obj.name,
                snap_name=snap_name,
                interface=self.interface,
            )
            test_results["create"]["speed"] = int(
                test_results["dataset"] / test_results["create"]["time"]
            )
            log.info(f' Test {test_num} dataset is {test_results["dataset"]} MiB')
            log.info(
                f'Snapshot creation time is : {test_results["create"]["time"]} sec.'
            )
            log.info(f'Snapshot speed is : {test_results["create"]["speed"]} MB/sec')

            # Step 4. Restore the PVC from the snapshot and measure the time
            # Same Storage class of the original PVC
            sc_name = self.pvc_obj.backed_sc

            # Size should be same as of the original PVC
            pvc_size = str(self.pvc_obj.size) + "Gi"

            # Create pvc out of the snapshot
            # Both, the snapshot and the restore PVC should be in same namespace

            log.info("Restoring from the Snapshot")
            restore_pvc_name = self.pvc_obj.name.replace(
                "pvc-test", f"restore-pvc{test_num}"
            )
            restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
            if self.interface == constants.CEPHFILESYSTEM:
                restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

            log.info("Resorting the PVC from Snapshot")
            restore_pvc_obj = pvc.create_restore_pvc(
                sc_name=sc_name,
                snap_name=self.snap_obj.name,
                namespace=self.snap_obj.namespace,
                size=pvc_size,
                pvc_name=restore_pvc_name,
                restore_pvc_yaml=restore_pvc_yaml,
            )
            helpers.wait_for_resource_state(
                restore_pvc_obj,
                constants.STATUS_BOUND,
                timeout=3600  # setting this to 60 Min.
                # since it can be take long time to restore, and we want it to finished.
            )
            teardown_factory(restore_pvc_obj)
            restore_pvc_obj.reload()
            log.info("PVC was restored from the snapshot")
            test_results["restore"]["time"] = helpers.measure_pvc_creation_time(
                self.interface, restore_pvc_obj.name
            )
            test_results["restore"]["speed"] = int(
                test_results["dataset"] / test_results["restore"]["time"]
            )
            log.info(f'Snapshot restore time is : {test_results["restore"]["time"]}')
            log.info(f'restore sped is : {test_results["restore"]["speed"]} MB/sec')

            # Step 5. Attach a new pod to the restored PVC
            restore_pod_obj = helpers.create_pod(
                interface_type=self.interface,
                pvc_name=restore_pvc_obj.name,
                namespace=self.snap_obj.namespace,
                pod_dict_path=constants.NGINX_POD_YAML,
            )

            # Confirm that the pod is running
            helpers.wait_for_resource_state(
                resource=restore_pod_obj, state=constants.STATUS_RUNNING
            )
            teardown_factory(restore_pod_obj)
            restore_pod_obj.reload()

            # Step 6. Verify that the file is present on the new pod also.
            log.info(
                f"Checking the existence of {file_name} "
                f"on restore pod {restore_pod_obj.name}"
            )
            assert pod.check_file_existence(
                restore_pod_obj, file_path
            ), f"File {file_name} doesn't exist"
            log.info(f"File {file_name} exists in {restore_pod_obj.name}")

            # Step 7. Verify that the md5sum matches
            log.info(
                f"Verifying that md5sum of {file_name} "
                f"on pod {self.pod_obj.name} matches with md5sum "
                f"of the same file on restore pod {restore_pod_obj.name}"
            )
            assert pod.verify_data_integrity(
                restore_pod_obj, file_name, orig_md5_sum
            ), "Data integrity check failed"
            log.info("Data integrity check passed, md5sum are same")

            all_results.append(test_results)

        # logging the test summery, all info in one place for easy log reading
        c_speed, c_runtime, r_speed, r_runtime = (0 for i in range(4))
        log.info("Test summery :")
        for tst in all_results:
            c_speed += tst["create"]["speed"]
            c_runtime += tst["create"]["time"]
            r_speed += tst["restore"]["speed"]
            r_runtime += tst["restore"]["time"]
            log.info(
                f"Test {tst['test_num']} results : dataset is {tst['dataset']} MiB. "
                f"Take snapshot time is {tst['create']['time']} "
                f"at {tst['create']['speed']} MiB/Sec "
                f"Restore from snapshot time is {tst['restore']['time']} "
                f"at {tst['restore']['speed']} MiB/Sec "
            )
        log.info(
            f" Average snapshot creation time is {c_runtime / self.tests_numbers} sec."
        )
        log.info(
            f" Average snapshot creation speed is {c_speed / self.tests_numbers} MiB/sec"
        )
        log.info(
            f" Average snapshot restore time is {r_runtime / self.tests_numbers} sec."
        )
        log.info(
            f" Average snapshot restore speed is {r_speed / self.tests_numbers} MiB/sec"
        )

    @pytest.mark.parametrize(
        argnames=["file_size", "files", "threads", "interface"],
        argvalues=[
            pytest.param(
                *[32, 125000, 8, constants.CEPHBLOCKPOOL],
            ),
            pytest.param(
                *[16, 250000, 8, constants.CEPHBLOCKPOOL],
            ),
            pytest.param(
                *[8, 500000, 8, constants.CEPHBLOCKPOOL],
            ),
            pytest.param(
                *[32, 125000, 8, constants.CEPHFILESYSTEM],
            ),
            pytest.param(
                *[16, 250000, 8, constants.CEPHFILESYSTEM],
            ),
            pytest.param(
                *[8, 500000, 8, constants.CEPHFILESYSTEM],
            ),
        ],
    )
    def test_pvc_snapshot_performance_multiple_files(
        self, ripsaw, file_size, files, threads, interface
    ):
        """
        Run SmallFile Workload and the take snapshot.
        test will run with 1M, 2M and 4M of file on the volume - total data set
        is the same for all tests, ~30GiB, and then take snapshot and measure
        the time it takes.
        the test will run 3 time to check consistency.

        Args:
            ripsaw : benchmark operator fixture which will run the workload
            file_size (int): the size of the file to be create - in KiB
            files (int): number of files each thread will create
            threads (int): number of threads will be used in the workload
            interface (str): the volume interface that will be used
                             CephBlockPool / CephFileSystem

        Raises:
            TimeoutError : in case of creation files take too long time
                           more then 2 Hours

        """

        # Loading the main template yaml file for the benchmark and update some
        # fields with new values
        sf_data = templating.load_yaml(constants.SMALLFILE_BENCHMARK_YAML)

        if interface == constants.CEPHBLOCKPOOL:
            storageclass = constants.DEFAULT_STORAGECLASS_RBD
        else:
            storageclass = constants.DEFAULT_STORAGECLASS_CEPHFS
        log.info(f"Using {storageclass} Storageclass")

        # Setting up the parameters for this test
        sf_data["spec"]["workload"]["args"]["samples"] = 1
        sf_data["spec"]["workload"]["args"]["operation"] = ["create"]
        sf_data["spec"]["workload"]["args"]["file_size"] = file_size
        sf_data["spec"]["workload"]["args"]["files"] = files
        sf_data["spec"]["workload"]["args"]["threads"] = threads
        sf_data["spec"]["workload"]["args"]["storageclass"] = storageclass
        del sf_data["spec"]["elasticsearch"]

        """
        Calculating the size of the volume that need to be test, it should
        be at least twice in the size then the size of the files, and at
        least 100Gi.

        Since the file_size is in Kb and the vol_size need to be in Gb, more
        calculation is needed.
        """
        total_files = int(files * threads)
        total_data = int(files * threads * file_size / constants.GB2KB)
        data_set = int(total_data * 3)  # calculate data with replica
        vol_size = data_set if data_set >= 100 else 100
        sf_data["spec"]["workload"]["args"]["storagesize"] = f"{vol_size}Gi"

        environment = get_environment_info()
        if not environment["user"] == "":
            sf_data["spec"]["test_user"] = environment["user"]
        else:
            # since full results object need this parameter, initialize it from CR file
            environment["user"] = sf_data["spec"]["test_user"]

        sf_data["spec"]["clustername"] = environment["clustername"]
        log.debug(f"The smallfile yaml file is {sf_data}")

        # Deploy the ripsaw operator
        log.info("Apply Operator CRD")
        ripsaw.apply_crd("resources/crds/ripsaw_v1alpha1_ripsaw_crd.yaml")

        all_results = []

        for test_num in range(self.tests_numbers):

            # deploy the smallfile workload
            log.info("Running SmallFile bench")
            sf_obj = OCS(**sf_data)
            sf_obj.create()

            # wait for benchmark pods to get created - takes a while
            for bench_pod in TimeoutSampler(
                240,
                10,
                get_pod_name_by_pattern,
                "smallfile-client",
                constants.RIPSAW_NAMESPACE,
            ):
                try:
                    if bench_pod[0] is not None:
                        small_file_client_pod = bench_pod[0]
                        break
                except IndexError:
                    log.info("Bench pod not ready yet")

            bench_pod = OCP(kind="pod", namespace=constants.RIPSAW_NAMESPACE)
            log.info("Waiting for SmallFile benchmark to Run")
            assert bench_pod.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=small_file_client_pod,
                sleep=30,
                timeout=600,
            )
            for item in bench_pod.get()["items"][1]["spec"]["volumes"]:
                if "persistentVolumeClaim" in item:
                    pvc_name = item["persistentVolumeClaim"]["claimName"]
                    break
            log.info(f"Benchmark PVC name is : {pvc_name}")
            # Creation of 4M files on CephFS can take a lot of time
            timeout = 7200
            while timeout >= 0:
                logs = bench_pod.get_logs(name=small_file_client_pod)
                if "RUN STATUS DONE" in logs:
                    break
                timeout -= 30
                if timeout == 0:
                    raise TimeoutError("Timed out waiting for benchmark to complete")
                time.sleep(30)
            log.info(f"Smallfile test ({test_num + 1}) finished.")
            snap_name = pvc_name.replace("claim", "snapshot-")
            log.info(f"Taking snapshot of the PVC {pvc_name}")
            log.info(f"Snapshot name : {snap_name}")
            creation_time = self.measure_create_snapshot_time(
                pvc_name=pvc_name, snap_name=snap_name, interface=interface
            )
            log.info(f"Snapshot creation time is {creation_time} seconds")
            all_results.append(creation_time)

            # Delete the smallfile workload
            log.info("Deleting the smallfile workload")
            if sf_obj.delete(wait=True):
                log.info("The smallfile workload was deleted successfully")

            # Delete VolumeSnapshots
            log.info("Deleting the snapshots")
            if self.snap_obj.delete(wait=True):
                log.info("The snapshot deleted successfully")
            log.info("Verify (and wait if needed) that ceph health is OK")
            ceph_health_check(tries=45, delay=60)

        log.info(f"Full test report for {interface}:")
        log.info(
            f"Test ran {self.tests_numbers} times, " f"All results are {all_results}"
        )
        log.info(f"The average creation time is : {statistics.mean(all_results)}")
        log.info(
            f"Number of Files on the volume : {total_files:,}, "
            f"Total dataset : {int(data_set / 3)} GiB"
        )
