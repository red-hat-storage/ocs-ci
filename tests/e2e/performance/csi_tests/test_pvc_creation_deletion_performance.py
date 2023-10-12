"""
Test to verify performance of PVC creation and deletion
for RBD and CephFS interfaces
"""

import logging
import pytest
import ocs_ci.ocs.exceptions as ex
import statistics
import tempfile
import yaml

from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import performance, performance_a
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.utility import templating


log = logging.getLogger(__name__)

Interface_Info = {
    constants.CEPHFILESYSTEM: {
        "type": "CephFS",
        "sc": constants.CEPHFILESYSTEM_SC,
        "delete_time": 2,
    },
    constants.CEPHBLOCKPOOL: {
        "type": "RBD",
        "sc": constants.CEPHBLOCKPOOL_SC,
        "delete_time": 1,
    },
}
Operations_Mesurment = ["create", "delete", "csi_create", "csi_delete"]


@grey_squad
@performance
@performance_a
class TestPVCCreationDeletionPerformance(PASTest):
    """
    Test(s) to verify performance of PVC creation and deletion
    """

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestPVCCreationDeletionPerformance, self).setup()
        self.benchmark_name = "PVC_Creation-Deletion"
        self.create_test_project()

    def teardown(self):
        """
        Cleanup the test environment
        """

        log.info("Starting the test environment celanup")
        # Delete the test project (namespace)
        self.delete_test_project()
        super(TestPVCCreationDeletionPerformance, self).teardown()

    def create_fio_pod_yaml(self, pvc_size=1):
        """
        This function create a new performance pod yaml file, which will trigger
        the FIO command on starting and getting into Compleat state when finish

        The FIO will fillup 70% of the PVC which will attached to the pod.

        Args:
            pvc_size (int/float): the size of the pvc_which will attach to the pod (in GiB)

        """
        file_size = f"{int(pvc_size * 1024 * 0.7)}M"
        self.full_results.add_key("dataset_written", file_size)

        # Creating the FIO command line parameters string
        command = (
            "--name=fio-fillup --filename=/mnt/test_file --rw=write --bs=1m"
            f" --direct=1 --numjobs=1 --time_based=0 --runtime=36000 --size={file_size}"
            " --ioengine=libaio --end_fsync=1 --output-format=json"
        )
        # Load the default POD yaml file and update it to run the FIO immediately
        pod_data = templating.load_yaml(constants.PERF_POD_YAML)
        pod_data["spec"]["containers"][0]["command"] = ["/usr/bin/fio"]
        pod_data["spec"]["containers"][0]["args"] = command.split(" ")
        pod_data["spec"]["containers"][0]["stdin"] = False
        pod_data["spec"]["containers"][0]["tty"] = False
        # FIO need to run only once
        pod_data["spec"]["restartPolicy"] = "Never"

        # Generate new POD yaml file
        self.pod_yaml_file = tempfile.NamedTemporaryFile(prefix="PerfPod")
        with open(self.pod_yaml_file.name, "w") as temp:
            yaml.dump(pod_data, temp)

    def create_pvcs_and_wait_for_bound(self, msg_prefix, pvcs, pvc_size, burst=True):
        """
        Creating  PVC(s) - one or more - in serial or parallel way, and wait until
        all of them are in `Bound` state.
        In case of not all PVC(s) get into Bound state whithin 2 sec. per PVC,
        timeout exception will be raise.

        Args:
            msg_prefix (str): prefix message for the logging
            pvcs (int): number of PVC(s) to create
            pvc_size (str): The PVC size to create - the unit is part of the string
                e.g : 1Gi
            burst (bool): if more then one PVC will be created - do it in paralle or serial

        Return:
            datetime : the timestamp when the creation started, for log parsing

        Raise:
            TimeoutExpiredError : if not all PVC(s) get into Bound state whithin 2 sec. per PVC
        """
        # Creating PVC(s) for creation time mesurment and wait for bound state
        timeout = pvcs * 2
        start_time = self.get_time(time_format="csi")
        log.info(f"{msg_prefix} Start creating new {pvcs} PVCs")
        self.pvc_objs, _ = helpers.create_multiple_pvcs(
            sc_name=Interface_Info[self.interface]["sc"],
            namespace=self.namespace,
            number_of_pvc=pvcs,
            size=pvc_size,
            burst=burst,
            do_reload=False,
        )

        log.info("Wait for all of the PVCs to be in Bound state")
        performance_lib.wait_for_resource_bulk_status(
            "pvc", pvcs, self.namespace, constants.STATUS_BOUND, timeout, 5
        )
        # incase of creation faliure, the wait_for_resource_bulk_status function
        # will raise an exception. so in this point the creation succeed
        log.info("All PVCs was created and in Bound state.")

        # Reload all PVC(s) information
        for pvc_obj in self.pvc_objs:
            pvc_obj.reload()

        return start_time

    def run_io(self):
        """
        Creating POD(s), attache them tp PVC(s), run IO to fill 70% of the PVC
        and wait until the I/O operation is completed.
        In the end, delete the POD(s).

        Return:
            bool : Running I/O success

        Raise:
            TimeoutExpiredError : if not all completed I/O whithin 20 Min.

        """
        # wait up to 60 Min for all pod(s) to complete running IO, this tuned for up to
        # 120 PVCs of 25GiB each.
        timeout = 3600  # old value 1200
        pod_objs = []
        # Create PODs, connect them to the PVCs and run IO on them
        for pvc_obj in self.pvc_objs:
            log.info("Creating Pod and Starting IO on it")
            pod_obj = helpers.create_pod(
                pvc_name=pvc_obj.name,
                namespace=self.namespace,
                interface_type=self.interface,
                pod_dict_path=self.pod_yaml_file.name,
            )
            assert pod_obj, "Failed to create pod"
            pod_objs.append(pod_obj)

        log.info("Wait for all of the POD(s) to be created, and compleat running I/O")
        performance_lib.wait_for_resource_bulk_status(
            "pod", len(pod_objs), self.namespace, constants.STATUS_COMPLETED, timeout, 5
        )
        log.info("I/O Completed on all POD(s)")

        # Delete all created POD(s)
        log.info("Try to delete all created PODs")
        for pod_obj in pod_objs:
            pod_obj.delete(wait=False)

        log.info("Wait for all PODS(s) to be deleted")
        performance_lib.wait_for_resource_bulk_status(
            "pod", 0, self.namespace, constants.STATUS_COMPLETED, timeout, 5
        )
        log.info("All pOD(s) was deleted")
        return True

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an empty ResultsAnalyse object

        Returns:
            ResultsAnalyse (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])
        full_results.add_key("storageclass", Interface_Info[self.interface]["type"])
        return full_results

    @pytest.mark.parametrize(
        argnames=["interface_type", "pvc_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "5Gi"],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "15Gi"],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "25Gi"],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "5Gi"],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "15Gi"],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "25Gi"],
            ),
        ],
    )
    def test_pvc_creation_deletion_measurement_performance(
        self, interface_type, pvc_size
    ):
        """
        Measuring PVC creation and deletion times for pvc samples.
        filling up each PVC with 70% of data.
        Verifying that those times are within the required limits

        Args:
            interface_type (str): the interface type to run against -
                CephBlockPool or CephFileSystem
            pvc_size (str): the size of the pvc to create
        """

        # Initializing test variables
        self.interface = interface_type

        num_of_samples = 5
        if self.dev_mode:
            num_of_samples = 2

        accepted_creation_time = 2  # old_value=1
        accepted_deletion_time = Interface_Info[self.interface]["delete_time"]
        accepted_creation_deviation_percent = 50
        accepted_deletion_deviation_percent = 50

        all_mesuring_times = {
            "create": [],
            "delete": [],
            "csi_create": [],
            "csi_delete": [],
        }

        msg_prefix = f"Interface: {self.interface}, PVC size: {pvc_size}."

        self.set_results_path_and_file(
            "test_pvc_creation_deletion_measurement_performance"
        )

        self.start_time = self.get_time()

        self.get_env_info()

        # Initialize the results doc file.
        self.full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "pvc_create_delete_fullres",
            )
        )
        self.full_results.add_key("pvc_size", pvc_size)
        self.full_results.add_key("samples", num_of_samples)

        self.create_fio_pod_yaml(pvc_size=int(pvc_size.replace("Gi", "")))

        # Creating PVC(s) for creation time mesurment
        start_time = self.create_pvcs_and_wait_for_bound(
            msg_prefix, num_of_samples, pvc_size, burst=False
        )

        # Fillup the PVC with data (70% of the total PVC size)
        self.run_io()

        # Deleting PVC(s) for deletion time mesurment
        log.info("Try to delete all created PVCs")
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()

        log.info("Wait for all PVC(s) to be deleted")
        performance_lib.wait_for_resource_bulk_status(
            "pvc", 0, self.namespace, constants.STATUS_BOUND, num_of_samples * 2, 5
        )
        log.info("All PVC(s) was deleted")

        mesure_data = "create"
        rec_policy = performance_lib.run_oc_command(
            f'get sc {Interface_Info[self.interface]["sc"]} -o jsonpath="'
            + '{.reclaimPolicy}"'
        )[0].strip('"')

        if rec_policy == constants.RECLAIM_POLICY_DELETE:
            log.info("Wait for all PVC(s) backed PV(s) to be deleted")
            # Timeout for each PV to be deleted is 20 sec.
            performance_lib.wait_for_resource_bulk_status(
                "pv", 0, self.namespace, self.namespace, num_of_samples * 20, 5
            )
            log.info("All backed PV(s) was deleted")
            mesure_data = "all"

        # Mesuring the time it took to create and delete the PVC(s)
        log.info("Reading Creation/Deletion time from provisioner logs")
        self.results_times = performance_lib.get_pvc_provision_times(
            interface=self.interface,
            pvc_name=self.pvc_objs,
            start_time=start_time,
            time_type="all",
            op=mesure_data,
        )

        # Analaysing the test results
        for i, pvc_res in enumerate(self.results_times):
            data = self.results_times[pvc_res]
            msg = f"{msg_prefix} PVC number {i + 1} was"
            for op in Operations_Mesurment:
                log.info(f"{msg} {op}d in {data[op]['time']} seconds.")

            if data["create"]["time"] > accepted_creation_time:
                raise ex.PerformanceException(
                    f"{msg_prefix} PVC creation time is {data['create']['time']} and is greater than "
                    f"{accepted_creation_time} seconds."
                )

            if rec_policy == constants.RECLAIM_POLICY_DELETE:
                if data["delete"]["time"] > accepted_deletion_time:
                    raise ex.PerformanceException(
                        f"{msg_prefix} PVC deletion time is {data['delete']['time']} and is greater than "
                        f"{accepted_deletion_time} seconds."
                    )
                all_mesuring_times["delete"].append(data["delete"]["time"])
                all_mesuring_times["csi_delete"].append(data["csi_delete"]["time"])

            all_mesuring_times["create"].append(data["create"]["time"])
            all_mesuring_times["csi_create"].append(data["csi_create"]["time"])

        for op in Operations_Mesurment:
            if rec_policy == constants.RECLAIM_POLICY_DELETE and "del" in op:
                self.process_time_measurements(
                    op,
                    all_mesuring_times[op],
                    accepted_deletion_deviation_percent,
                    msg_prefix,
                )
            if "create" in op:
                self.process_time_measurements(
                    op,
                    all_mesuring_times[op],
                    accepted_creation_deviation_percent,
                    msg_prefix,
                )

        self.full_results.all_results = self.results_times
        self.end_time = self.get_time()
        self.full_results.add_key(
            "test_time", {"start": self.start_time, "end": self.end_time}
        )
        if self.full_results.es_write():
            res_link = self.full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (6 - according to the parameters)
            self.write_result_to_file(res_link)

    def process_time_measurements(
        self, action_name, time_measures, accepted_deviation_percent, msg_prefix
    ):
        """
            Analyses the given time measured. If the standard deviation of these
            times is bigger than the provided accepted deviation percent, fails the test.
            Adding the average results (as the std_deviation percentage between samples)
            to the ES report

        Args:
            action_name (str): Name of the action for which these measurements were collected;
                    used for the logging
            time_measures (list of floats): A list of time measurements
            accepted_deviation_percent (int): Accepted deviation percent to which computed
                    standard deviation may be compared
            msg_prefix (str) : A string for comprehensive logging

        """
        average = float("{:.3f}".format(statistics.mean(time_measures)))
        self.full_results.add_key(f"{action_name.replace('-','_')}_time", average)

        log.info(
            f"{msg_prefix} The average {action_name} time for the sampled  "
            f"{len(time_measures)} PVCs is {average} seconds."
        )

        st_deviation = statistics.stdev(time_measures)
        st_deviation_percent = float("{:.3f}".format(st_deviation / average * 100.0))
        if st_deviation_percent > accepted_deviation_percent:
            log.error(
                f"{msg_prefix} The standard deviation percent for {action_name} "
                f"of {len(time_measures)} sampled PVCs is {st_deviation_percent}% "
                f"which is bigger than accepted {accepted_deviation_percent}."
            )
        else:
            log.info(
                f"{msg_prefix} The standard deviation percent for {action_name} "
                f"of {len(time_measures)} sampled PVCs is {st_deviation_percent}% "
                "and is within the accepted range."
            )
        self.full_results.add_key(
            f"{action_name.replace('-','_')}_deviation_pct", st_deviation_percent
        )

    @pytest.mark.parametrize(
        argnames=["interface_type"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM],
            ),
        ],
    )
    @pytest.mark.polarion_id("OCS-2618")
    def test_multiple_pvc_deletion_measurement_performance(self, interface_type):
        """
        Measuring PVC deletion time of 120 PVCs in 180 seconds

        Args:
            interface_type: the inteface type which the test run with - RBD / CephFS.

        """
        # Initialize the test variables
        self.interface = interface_type

        number_of_pvcs = 120
        if self.dev_mode:
            number_of_pvcs = 5

        pvc_size = "1Gi"

        # accepted deletion time is 2 secs for each PVC
        accepted_pvc_deletion_time = number_of_pvcs * 2

        msg_prefix = f"Interface: {self.interface}, PVC size: {pvc_size}."
        self.set_results_path_and_file(
            "test_multiple_pvc_deletion_measurement_performance"
        )
        bulk_data = {
            "create": {"start": [], "end": []},
            "csi_create": {"start": [], "end": []},
            "delete": {"start": [], "end": []},
            "csi_delete": {"start": [], "end": []},
        }
        bulk_times = {
            "create": None,
            "delete": None,
            "csi_create": None,
            "csi_delete": None,
        }

        self.start_time = self.get_time()

        self.get_env_info()

        # Initialize the results doc file.
        self.full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "pvc_bulk_deletion_fullres",
            )
        )
        self.full_results.add_key("bulk_size", number_of_pvcs)
        self.full_results.add_key("pvc_size", pvc_size)

        self.create_fio_pod_yaml(pvc_size=int(pvc_size.replace("Gi", "")))

        # Creating PVC(s) for creation time mesurment and wait for bound state
        start_time = self.create_pvcs_and_wait_for_bound(
            msg_prefix, number_of_pvcs, pvc_size, burst=True
        )

        # Fillup the PVC with data (70% of the total PVC size)
        self.run_io()

        # Deleting PVC(s) for deletion time mesurment
        log.info("Try to delete all created PVCs")
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete(wait=False)

        performance_lib.wait_for_resource_bulk_status(
            "pvc", 0, self.namespace, constants.STATUS_BOUND, number_of_pvcs * 2, 5
        )
        log.info("All PVC(s) was deleted")

        log.info("Wait for all PVC(s) backed PV(s) to be deleted")
        # Timeout for each PV to be deleted is 20 sec.
        performance_lib.wait_for_resource_bulk_status(
            "pv", 0, self.namespace, self.namespace, number_of_pvcs * 20, 5
        )
        log.info("All backed PV(s) was deleted")

        # Mesuring the time it took to delete the PVC(s)
        log.info("Reading Creation/Deletion time from provisioner logs")
        self.results_times = performance_lib.get_pvc_provision_times(
            interface=self.interface,
            pvc_name=self.pvc_objs,
            start_time=start_time,
            time_type="all",
            op="all",
        )
        for i, pvc_res in enumerate(self.results_times):
            data = self.results_times[pvc_res]
            msg = f"{msg_prefix} PVC number {i + 1} was"
            for op in Operations_Mesurment:
                log.info(f"{msg} {op}d in {data[op]['time']} seconds.")

                bulk_data[op]["start"].append(data[op]["start"])
                bulk_data[op]["end"].append(data[op]["end"])

            if data["delete"]["time"] > accepted_pvc_deletion_time:
                raise ex.PerformanceException(
                    f"{msg_prefix} {number_of_pvcs} PVCs deletion time is {data['delete']['time']} "
                    f"and is greater than {accepted_pvc_deletion_time} seconds"
                )

        for op in Operations_Mesurment:
            bulk_times[op] = {
                "start": sorted(bulk_data[op]["start"])[0],
                "end": sorted(bulk_data[op]["end"])[-1],
                "time": None,
            }
            bulk_times[op]["time"] = performance_lib.calculate_operation_time(
                f"bulk_{op}", bulk_times[op]
            )

            log.info(f"Bulk {op}ion Time is : { bulk_times[op]['time']} seconds")
            self.full_results.add_key(f"multi_{op}", bulk_times[op]["time"])

        self.full_results.all_results = self.results_times
        self.end_time = self.get_time()
        self.full_results.add_key(
            "test_time", {"start": self.start_time, "end": self.end_time}
        )

        if self.full_results.es_write():
            res_link = self.full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (3 - according to the parameters)
            self.write_result_to_file(res_link)

    def test_getting_all_results(self):
        """
        This is not a test - it is only check that previous test ran and finish as expected
        and reporting the full results (links in the ES) of previous tests (2)
        """
        self.add_test_to_results_check(
            test="test_pvc_creation_deletion_measurement_performance",
            test_count=6,
            test_name="PVC Create-Delete",
        )
        self.add_test_to_results_check(
            test="test_multiple_pvc_deletion_measurement_performance",
            test_count=2,
            test_name="PVC Multiple-Delete",
        )
        self.check_results_and_push_to_dashboard()
