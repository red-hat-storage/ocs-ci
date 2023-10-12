# Builtin modules
import logging
import time

# 3ed party modules
import pytest
import statistics

# Local modules
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.ocs.ocp import OCP, switch_to_project
from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.elasticsearch import ElasticSearch
from ocs_ci.ocs.version import get_environment_info
from ocs_ci.ocs.benchmark_operator import BMO_NAME
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.resources import pod, pvc
import ocs_ci.ocs.exceptions as ex
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.framework.testlib import (
    skipif_ocp_version,
    skipif_ocs_version,
    performance,
    performance_b,
)
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@grey_squad
@performance
@performance_b
@skipif_ocp_version("<4.6")
@skipif_ocs_version("<4.6")
class TestPvcSnapshotPerformance(PASTest):
    """
    Tests to verify PVC snapshot creation and deletion performance
    """

    @pytest.fixture()
    def base_setup(
        self,
        interface_iterate,
        storageclass_factory,
        pvc_size,
    ):
        """
        A setup phase for the test - creating resources

        Args:
            interface_iterate: A fixture to iterate over ceph interfaces
            storageclass_factory: A fixture to create everything needed for a
                storageclass
            pvc_size: The size of the PVC in Gi

        """
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc = "RBD"
        elif self.interface == constants.CEPHFILESYSTEM:
            self.sc = "CephFS"
        elif self.interface == constants.CEPHBLOCKPOOL_THICK:
            self.sc = "RBD-Thick"

        self.create_test_project()

        self.pvc_obj = helpers.create_pvc(
            sc_name=self.sc_obj.name, size=pvc_size + "Gi", namespace=self.namespace
        )
        helpers.wait_for_resource_state(self.pvc_obj, constants.STATUS_BOUND)
        self.pvc_obj.reload()

        # Create a POD and attach it the the PVC
        try:
            self.pod_object = helpers.create_pod(
                interface_type=self.interface,
                pvc_name=self.pvc_obj.name,
                namespace=self.namespace,
                pod_dict_path=constants.PERF_POD_YAML,
            )
            helpers.wait_for_resource_state(self.pod_object, constants.STATUS_RUNNING)
            self.pod_object.reload()
            self.pod_object.workload_setup("fs", jobs=1, fio_installed=True)
        except Exception as e:
            log.error(
                f"Pod on PVC {self.pvc_obj.name} was not created, exception {str(e)}"
            )
            raise ex.PodNotCreated("Pod on PVC was not created.")

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestPvcSnapshotPerformance, self).setup()
        self.benchmark_name = "pvc_snaspshot_performance"
        self.tests_numbers = 3  # number of tests to run

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an empty ResultsAnalyse object

        Returns:
            ResultsAnalyse (obj): the input object filled with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])
        return full_results

    def measure_create_snapshot_time(
        self, pvc_name, snap_name, namespace, interface, start_time=None
    ):
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
            namespace=namespace,
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
        c_time = performance_lib.measure_total_snapshot_creation_time(
            snap_name, start_time
        )

        return c_time

    @pytest.mark.parametrize(
        argnames=["pvc_size"],
        argvalues=[pytest.param(*["1"]), pytest.param(*["10"]), pytest.param(*["100"])],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_snapshot_performance(self, pvc_size):
        """
        1. Run I/O on a pod file
        2. Calculate md5sum of the file
        3. Take a snapshot of the PVC
        4. Measure the total snapshot creation time and the CSI snapshot creation time
        4. Restore From the snapshot and measure the time
        5. Attach a new pod to it
        6. Verify that the file is present on the new pod also
        7. Verify that the md5sum of the file on the new pod matches
           with the md5sum of the file on the original pod

        This scenario run 3 times and report all the average results of the 3 runs
        and will send them to the ES
        Args:
            pvc_size: the size of the PVC to be tested - parametrize

        """

        # Getting the total Storage capacity
        ceph_capacity = self.ceph_cluster.get_ceph_capacity()

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

        # Produce ES report
        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        self.full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "pvc_snapshot_perf",
            )
        )
        self.full_results.add_key("pvc_size", pvc_size + " GiB")
        self.full_results.add_key("interface", self.sc)
        self.full_results.all_results["creation_time"] = []
        self.full_results.all_results["csi_creation_time"] = []
        self.full_results.all_results["creation_speed"] = []
        self.full_results.all_results["restore_time"] = []
        self.full_results.all_results["restore_speed"] = []
        self.full_results.all_results["restore_csi_time"] = []
        self.full_results.all_results["dataset_inMiB"] = []
        for test_num in range(self.tests_numbers):
            test_results = {
                "test_num": test_num + 1,
                "dataset": (test_num + 1) * filesize * 1024,  # size in MiB
                "create": {"time": None, "csi_time": None, "speed": None},
                "restore": {"time": None, "speed": None},
            }
            log.info(f"Starting test phase number {test_num}")
            # Step 1. Run I/O on a pod file.
            file_name = f"{self.pod_object.name}-{test_num}"
            log.info(f"Starting IO on the POD {self.pod_object.name}")
            # Going to run only write IO to fill the PVC for the snapshot
            self.pod_object.fillup_fs(size=file_size, fio_filename=file_name)

            # Wait for fio to finish
            fio_result = self.pod_object.get_fio_results(timeout=1800)
            err_count = fio_result.get("jobs")[0].get("error")
            assert (
                err_count == 0
            ), f"IO error on pod {self.pod_object.name}. FIO result: {fio_result}"
            log.info("IO on the PVC Finished")

            # Verify presence of the file
            file_path = pod.get_file_path(self.pod_object, file_name)
            log.info(f"Actual file path on the pod {file_path}")
            assert pod.check_file_existence(
                self.pod_object, file_path
            ), f"File {file_name} doesn't exist"
            log.info(f"File {file_name} exists in {self.pod_object.name}")

            # Step 2. Calculate md5sum of the file.
            orig_md5_sum = pod.cal_md5sum(self.pod_object, file_name)

            # Step 3. Take a snapshot of the PVC and measure the time of creation.
            snap_name = self.pvc_obj.name.replace(
                "pvc-test", f"snapshot-test{test_num}"
            )
            log.info(f"Taking snapshot of the PVC {snap_name}")

            start_time = self.get_time("CSI")

            test_results["create"]["time"] = self.measure_create_snapshot_time(
                pvc_name=self.pvc_obj.name,
                snap_name=snap_name,
                namespace=self.pod_object.namespace,
                interface=self.interface,
                start_time=start_time,
            )

            test_results["create"][
                "csi_time"
            ] = performance_lib.measure_csi_snapshot_creation_time(
                interface=self.interface,
                snapshot_id=self.snap_uid,
                start_time=start_time,
            )

            test_results["create"]["speed"] = int(
                test_results["dataset"] / test_results["create"]["time"]
            )
            log.info(f' Test {test_num} dataset is {test_results["dataset"]} MiB')
            log.info(
                f"Snapshot name {snap_name} and id {self.snap_uid} creation time is"
                f' : {test_results["create"]["time"]} sec.'
            )
            log.info(
                f"Snapshot name {snap_name} and id {self.snap_uid} csi creation time is"
                f' : {test_results["create"]["csi_time"]} sec.'
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

            csi_start_time = self.get_time("csi")
            log.info("Restoring the PVC from Snapshot")
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
            restore_pvc_obj.reload()
            log.info("PVC was restored from the snapshot")
            test_results["restore"]["time"] = performance_lib.measure_pvc_creation_time(
                self.interface, restore_pvc_obj.name, csi_start_time
            )

            test_results["restore"]["speed"] = int(
                test_results["dataset"] / test_results["restore"]["time"]
            )
            log.info(f'Snapshot restore time is : {test_results["restore"]["time"]}')
            log.info(f'restore speed is : {test_results["restore"]["speed"]} MB/sec')

            test_results["restore"]["csi_time"] = performance_lib.csi_pvc_time_measure(
                self.interface, restore_pvc_obj, "create", csi_start_time
            )
            log.info(
                f'Snapshot csi restore time is : {test_results["restore"]["csi_time"]}'
            )

            # Step 5. Attach a new pod to the restored PVC
            restore_pod_object = helpers.create_pod(
                interface_type=self.interface,
                pvc_name=restore_pvc_obj.name,
                namespace=self.snap_obj.namespace,
                pod_dict_path=constants.PERF_POD_YAML,
            )

            # Confirm that the pod is running
            helpers.wait_for_resource_state(
                resource=restore_pod_object, state=constants.STATUS_RUNNING
            )
            restore_pod_object.reload()

            # Step 6. Verify that the file is present on the new pod also.
            log.info(
                f"Checking the existence of {file_name} "
                f"on restore pod {restore_pod_object.name}"
            )
            assert pod.check_file_existence(
                restore_pod_object, file_path
            ), f"File {file_name} doesn't exist"
            log.info(f"File {file_name} exists in {restore_pod_object.name}")

            # Step 7. Verify that the md5sum matches
            log.info(
                f"Verifying that md5sum of {file_name} "
                f"on pod {self.pod_object.name} matches with md5sum "
                f"of the same file on restore pod {restore_pod_object.name}"
            )
            assert pod.verify_data_integrity(
                restore_pod_object, file_name, orig_md5_sum
            ), "Data integrity check failed"
            log.info("Data integrity check passed, md5sum are same")

            restore_pod_object.delete()
            restore_pvc_obj.delete()

            all_results.append(test_results)

        # clean the enviroment
        self.pod_object.delete()
        self.pvc_obj.delete()
        self.delete_test_project()

        # logging the test summary, all info in one place for easy log reading
        c_speed, c_runtime, c_csi_runtime, r_speed, r_runtime, r_csi_runtime = (
            0 for i in range(6)
        )

        log.info("Test summary :")
        for tst in all_results:
            c_speed += tst["create"]["speed"]
            c_runtime += tst["create"]["time"]
            c_csi_runtime += tst["create"]["csi_time"]
            r_speed += tst["restore"]["speed"]
            r_runtime += tst["restore"]["time"]
            r_csi_runtime += tst["restore"]["csi_time"]

            self.full_results.all_results["creation_time"].append(tst["create"]["time"])
            self.full_results.all_results["csi_creation_time"].append(
                tst["create"]["csi_time"]
            )
            self.full_results.all_results["creation_speed"].append(
                tst["create"]["speed"]
            )
            self.full_results.all_results["restore_time"].append(tst["restore"]["time"])
            self.full_results.all_results["restore_speed"].append(
                tst["restore"]["speed"]
            )
            self.full_results.all_results["restore_csi_time"].append(
                tst["restore"]["csi_time"]
            )
            self.full_results.all_results["dataset_inMiB"].append(tst["dataset"])
            log.info(
                f"Test {tst['test_num']} results : dataset is {tst['dataset']} MiB. "
                f"Take snapshot time is {tst['create']['time']} "
                f"at {tst['create']['speed']} MiB/Sec "
                f"Restore from snapshot time is {tst['restore']['time']} "
                f"at {tst['restore']['speed']} MiB/Sec "
            )

        avg_snap_c_time = c_runtime / self.tests_numbers
        avg_snap_csi_c_time = c_csi_runtime / self.tests_numbers
        avg_snap_c_speed = c_speed / self.tests_numbers
        avg_snap_r_time = r_runtime / self.tests_numbers
        avg_snap_r_speed = r_speed / self.tests_numbers
        avg_snap_r_csi_time = r_csi_runtime / self.tests_numbers
        log.info(f" Average snapshot creation time is {avg_snap_c_time} sec.")
        log.info(f" Average csi snapshot creation time is {avg_snap_csi_c_time} sec.")
        log.info(f" Average snapshot creation speed is {avg_snap_c_speed} MiB/sec")
        log.info(f" Average snapshot restore time is {avg_snap_r_time} sec.")
        log.info(f" Average snapshot restore speed is {avg_snap_r_speed} MiB/sec")
        log.info(f" Average snapshot restore csi time is {avg_snap_r_csi_time} sec.")

        self.full_results.add_key("avg_snap_creation_time_insecs", avg_snap_c_time)
        self.full_results.add_key(
            "avg_snap_csi_creation_time_insecs", avg_snap_csi_c_time
        )
        self.full_results.add_key("avg_snap_creation_speed", avg_snap_c_speed)
        self.full_results.add_key("avg_snap_restore_time_insecs", avg_snap_r_time)
        self.full_results.add_key("avg_snap_restore_speed", avg_snap_r_speed)
        self.full_results.add_key(
            "avg_snap_restore_csi_time_insecs", avg_snap_r_csi_time
        )

        # Write the test results into the ES server
        self.results_path = helpers.get_full_test_logs_path(cname=self)
        log.info(f"Logs file path name is : {self.full_log_path}")
        log.info("writing results to elastic search server")
        if self.full_results.es_write():
            res_link = self.full_results.results_link()

            # write the ES link to the test results in the test log.
            log.info(f"The result can be found at : {res_link}")

            self.write_result_to_file(res_link)

    @pytest.mark.parametrize(
        argnames=["file_size", "files", "threads", "interface"],
        argvalues=[
            pytest.param(
                *[32, 125000, 8, constants.CEPHBLOCKPOOL],
                marks=[pytest.mark.polarion_id("OCS-2624")],
            ),
            pytest.param(
                *[32, 125000, 8, constants.CEPHFILESYSTEM],
                marks=[pytest.mark.polarion_id("OCS-2625")],
            ),
        ],
    )
    def test_pvc_snapshot_performance_multiple_files(
        self, file_size, files, threads, interface
    ):
        """
        Run SmallFile Workload and the take snapshot.
        test will run with 1M of file on the volume - total data set
        is the same for all tests, ~30GiB, and then take snapshot and measure
        the time it takes.
        the test will run 3 time to check consistency.

        Args:
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

        # Deploying elastic-search server in the cluster for use by the
        # SmallFiles workload, since it is mandatory for the workload.
        # This is deployed once for all test iterations and will be deleted
        # in the end of the test.
        if config.PERF.get("deploy_internal_es"):
            self.es = ElasticSearch()
            sf_data["spec"]["elasticsearch"] = {
                "url": f"http://{self.es.get_ip()}:{self.es.get_port()}"
            }
        else:
            if config.PERF.get("internal_es_server") == "":
                self.es = None
                return
            else:
                self.es = {
                    "server": config.PERF.get("internal_es_server"),
                    "port": config.PERF.get("internal_es_port"),
                    "url": f"http://{config.PERF.get('internal_es_server')}:{config.PERF.get('internal_es_port')}",
                }
                # verify that the connection to the elasticsearch server is OK
                if not super(TestPvcSnapshotPerformance, self).es_connect():
                    self.es = None
                    log.error("ElasticSearch doesn't exist ! The test cannot run")
                    return
                sf_data["spec"]["elasticsearch"] = {"url": self.es["url"]}

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

        # Deploy the benchmark-operator, so we can use the SmallFiles workload
        # to fill up the volume with files, and switch to the benchmark-operator namespace.
        log.info("Deploy the benchmark-operator")
        self.deploy_benchmark_operator()
        switch_to_project(BMO_NAME)

        all_results = []

        # Produce ES report
        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        self.full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "pvc_snapshot_perf_multiple_files",
            )
        )
        self.full_results.add_key("file_size_inKB", file_size)
        self.full_results.add_key("threads", threads)
        self.full_results.add_key("interface", interface)
        for test_num in range(self.tests_numbers):
            test_results = {"creation_time": None, "csi_creation_time": None}

            # deploy the smallfile workload
            self.crd_data = sf_data
            self.client_pod_name = "smallfile-client"
            self.deploy_and_wait_for_wl_to_start(timeout=240)
            # Initialize the pvc_name variable so it will not be in loop scope only.
            pvc_name = (
                OCP(kind="pvc", namespace=BMO_NAME)
                .get()
                .get("items")[0]
                .get("metadata")
                .get("name")
            )
            log.info(f"Benchmark PVC name is : {pvc_name}")
            self.wait_for_wl_to_finish(sleep=30)

            # Taking snapshot of the PVC (which contain files)
            snap_name = pvc_name.replace("claim", "snapshot-")
            log.info(f"Taking snapshot of the PVC {pvc_name}")
            log.info(f"Snapshot name : {snap_name}")

            start_time = self.get_time("csi")

            test_results["creation_time"] = self.measure_create_snapshot_time(
                pvc_name=pvc_name,
                snap_name=snap_name,
                namespace=BMO_NAME,
                interface=interface,
                start_time=start_time,
            )
            log.info(
                f"Snapshot with name {snap_name} and id {self.snap_uid} creation time is"
                f' {test_results["creation_time"]} seconds'
            )

            test_results[
                "csi_creation_time"
            ] = performance_lib.measure_csi_snapshot_creation_time(
                interface=interface, snapshot_id=self.snap_uid, start_time=start_time
            )
            log.info(
                f"Snapshot with name {snap_name} and id {self.snap_uid} csi creation time is"
                f' {test_results["csi_creation_time"]} seconds'
            )

            all_results.append(test_results)

            # Delete the smallfile workload - which will delete also the PVC
            log.info("Deleting the smallfile workload")
            if self.benchmark_obj.delete(wait=True):
                log.info("The smallfile workload was deleted successfully")

            # Delete VolumeSnapshots
            log.info("Deleting the snapshots")
            if self.snap_obj.delete(wait=True):
                log.info("The snapshot deleted successfully")
            log.info("Verify (and wait if needed) that ceph health is OK")
            ceph_health_check(tries=45, delay=60)

            # Sleep for 1 Min. between test samples
            time.sleep(60)

        # Cleanup the elasticsearch instance, if needed.
        if isinstance(self.es, ElasticSearch):
            log.info("Deleting the elastic-search instance")
            self.es.cleanup()

        creation_times = [t["creation_time"] for t in all_results]
        avg_c_time = statistics.mean(creation_times)
        csi_creation_times = [t["csi_creation_time"] for t in all_results]
        avg_csi_c_time = statistics.mean(csi_creation_times)

        t_dateset = int(data_set / 3)

        log.info(f"Full test report for {interface}:")
        log.info(
            f"Test ran {self.tests_numbers} times, "
            f"All snapshot creation results are {creation_times} seconds"
        )
        log.info(f"The average snapshot creation time is : {avg_c_time} seconds")
        log.info(
            f"Test ran {self.tests_numbers} times, "
            f"All snapshot csi creation results are {csi_creation_times}"
        )
        log.info(f"The average csi snapshot creation time is : {avg_csi_c_time}")

        log.info(
            f"Number of Files on the volume : {total_files:,}, "
            f"Total dataset : {t_dateset} GiB"
        )

        self.full_results.add_key("avg_snapshot_creation_time_insecs", avg_c_time)
        self.full_results.all_results["total_files"] = total_files
        self.full_results.all_results["total_dataset"] = t_dateset
        self.full_results.all_results["creation_time"] = creation_times
        self.full_results.all_results["csi_creation_time"] = csi_creation_times

        # Write the test results into the ES server
        log.info("writing results to elastic search server")
        self.results_path = helpers.get_full_test_logs_path(cname=self)
        if self.full_results.es_write():
            res_link = self.full_results.results_link()
            # write the ES link to the test results in the test log.
            log.info(f"The result can be found at : {res_link}")

            # Create text file with results of all subtest
            self.write_result_to_file(res_link)

    def test_pvc_snapshot_performance_results(self):
        """
        This is not a test - it is only check that previous tests ran and finished as expected
        and reporting the full results (links in the ES) of previous tests (6 + 2)
        """
        self.add_test_to_results_check(
            test="test_pvc_snapshot_performance",
            test_count=6,
            test_name="PVC Snapshot",
        )
        self.add_test_to_results_check(
            test="test_pvc_snapshot_performance_multiple_files",
            test_count=2,
            test_name="PVC Snapshot - Multiple Files",
        )
        self.check_results_and_push_to_dashboard()
