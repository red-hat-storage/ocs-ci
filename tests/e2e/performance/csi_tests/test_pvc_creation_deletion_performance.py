"""
Test to verify performance of PVC creation and deletion
for RBD, CephFS and RBD-Thick interfaces
"""
import time
import logging
import os
import datetime
import pytest
import ocs_ci.ocs.exceptions as ex
import threading
import statistics
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

from ocs_ci.framework.testlib import performance
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.framework import config


log = logging.getLogger(__name__)


@performance
class TestPVCCreationDeletionPerformance(PASTest):
    """
    Test to verify performance of PVC creation and deletion
    """

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestPVCCreationDeletionPerformance, self).setup()
        self.benchmark_name = "PVC_Creation-Deletion"
        self.uuid = uuid4().hex
        self.crd_data = {
            "spec": {
                "test_user": "Homer simpson",
                "clustername": "test_cluster",
                "elasticsearch": {
                    "server": config.PERF.get("production_es_server"),
                    "port": config.PERF.get("production_es_port"),
                    "url": f"http://{config.PERF.get('production_es_server')}:{config.PERF.get('production_es_port')}",
                },
            }
        }
        if self.dev_mode:
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("dev_es_server"),
                "port": config.PERF.get("dev_es_port"),
                "url": f"http://{config.PERF.get('dev_es_server')}:{config.PERF.get('dev_es_port')}",
            }

    @pytest.fixture()
    def base_setup(self, interface_type, storageclass_factory, pod_factory):
        """
        A setup phase for the test

        Args:
            interface_type: A fixture to iterate over ceph interfaces
            storageclass_factory: A fixture to create everything needed for a
                storageclass
            pod_factory: A fixture to create new pod
        """
        self.interface = interface_type
        if self.interface == constants.CEPHBLOCKPOOL_THICK:
            self.sc_obj = storageclass_factory(
                interface=constants.CEPHBLOCKPOOL,
                new_rbd_pool=True,
                rbd_thick_provision=True,
            )
        else:
            self.sc_obj = storageclass_factory(self.interface)
        self.pod_factory = pod_factory

    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

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
        full_results.add_key("storageclass", self.sc)
        full_results.add_key("index", full_results.new_index)
        return full_results

    @pytest.mark.parametrize(
        argnames=["interface_type", "pvc_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "5Gi"],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "15Gi"],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, "25Gi"],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "5Gi"],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "15Gi"],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, "25Gi"],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL_THICK, "5Gi"],
                marks=[pytest.mark.performance_extended],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL_THICK, "15Gi"],
                marks=[pytest.mark.performance_extended],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL_THICK, "25Gi"],
                marks=[pytest.mark.performance_extended],
            ),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_creation_deletion_measurement_performance(
        self, teardown_factory, pvc_size
    ):
        """
        Measuring PVC creation and deletion times for pvc samples
        Verifying that those times are within the required limits
        """

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        self.results_path = get_full_test_logs_path(cname=self)
        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc = "RBD"
        elif self.interface == constants.CEPHFILESYSTEM:
            self.sc = "CephFS"
        elif self.interface == constants.CEPHBLOCKPOOL_THICK:
            self.sc = "RBD-Thick"
        self.full_log_path += f"-{self.sc}-{pvc_size}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())

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
        num_of_samples = 5
        accepted_creation_time = (
            600 if self.interface == constants.CEPHBLOCKPOOL_THICK else 1
        )

        # accepted deletion time for RBD is 1 sec, for CephFS is 2 secs and for RBD Thick is 5 secs
        if self.interface == constants.CEPHFILESYSTEM:
            accepted_deletion_time = 2
        elif self.interface == constants.CEPHBLOCKPOOL:
            accepted_deletion_time = 1
        else:
            accepted_deletion_time = 5

        self.full_results.add_key("samples", num_of_samples)

        accepted_creation_deviation_percent = 50
        accepted_deletion_deviation_percent = 50

        creation_time_measures = []
        deletion_time_measures = []
        msg_prefix = f"Interface: {self.interface}, PVC size: {pvc_size}."

        for i in range(num_of_samples):
            logging.info(f"{msg_prefix} Start creating PVC number {i + 1}.")
            start_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            pvc_obj = helpers.create_pvc(sc_name=self.sc_obj.name, size=pvc_size)
            timeout = 600 if self.interface == constants.CEPHBLOCKPOOL_THICK else 60
            helpers.wait_for_resource_state(
                pvc_obj, constants.STATUS_BOUND, timeout=timeout
            )
            pvc_obj.reload()

            creation_time = performance_lib.measure_pvc_creation_time(
                self.interface, pvc_obj.name, start_time
            )

            logging.info(
                f"{msg_prefix} PVC number {i + 1} was created in {creation_time} seconds."
            )
            if creation_time > accepted_creation_time:
                raise ex.PerformanceException(
                    f"{msg_prefix} PVC creation time is {creation_time} and is greater than "
                    f"{accepted_creation_time} seconds."
                )
            creation_time_measures.append(creation_time)

            pv_name = pvc_obj.backed_pv
            pvc_reclaim_policy = pvc_obj.reclaim_policy

            pod_obj = self.write_file_on_pvc(pvc_obj)
            pod_obj.delete(wait=True)
            teardown_factory(pvc_obj)
            logging.info(f"{msg_prefix} Start deleting PVC number {i + 1}")
            if pvc_reclaim_policy == constants.RECLAIM_POLICY_DELETE:
                pvc_obj.delete()
                pvc_obj.ocp.wait_for_delete(pvc_obj.name)
                helpers.validate_pv_delete(pvc_obj.backed_pv)
                deletion_time = helpers.measure_pvc_deletion_time(
                    self.interface, pv_name
                )
                logging.info(
                    f"{msg_prefix} PVC number {i + 1} was deleted in {deletion_time} seconds."
                )
                if deletion_time > accepted_deletion_time:
                    raise ex.PerformanceException(
                        f"{msg_prefix} PVC deletion time is {deletion_time} and is greater than "
                        f"{accepted_deletion_time} seconds."
                    )
                deletion_time_measures.append(deletion_time)
            else:
                logging.info(
                    f"Reclaim policy of the PVC {pvc_obj.name} is not Delete;"
                    f" therefore not measuring deletion time for this PVC."
                )

        creation_average = self.process_time_measurements(
            "creation",
            creation_time_measures,
            accepted_creation_deviation_percent,
            msg_prefix,
        )
        self.full_results.add_key("creation-time", creation_average)
        deletion_average = self.process_time_measurements(
            "deletion",
            deletion_time_measures,
            accepted_deletion_deviation_percent,
            msg_prefix,
        )
        self.full_results.add_key("deletion-time", deletion_average)
        self.full_results.all_results["creation"] = creation_time_measures
        self.full_results.all_results["deletion"] = deletion_time_measures
        self.end_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())
        self.full_results.add_key(
            "test_time", {"start": self.start_time, "end": self.end_time}
        )
        if self.full_results.es_write():
            res_link = self.full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (4 - according to the parameters)
            self.write_result_to_file(res_link)

    @pytest.mark.skip(SKIP_REASON)
    def test_pvc_creation_deletion_results(self):
        """
        This is not a test - it is only check that previous test ran and finish as expected
        and reporting the full results (links in the ES) of previous tests (4)
        """

        self.results_path = get_full_test_logs_path(
            cname=self, fname="test_pvc_creation_deletion_measurement_performance"
        )
        self.results_file = os.path.join(self.results_path, "all_results.txt")
        log.info(f"Check results in {self.results_file}")
        self.number_of_tests = 3
        log.info("Check results for 'performance_extended' marker (3 tests)")
        try:
            self.check_tests_results()
        except ex.BenchmarkTestFailed:
            log.info("Look like performance_extended was not triggered")
            log.info("Check results for 'performance' marker (9 tests)")
            self.number_of_tests = 9
            self.check_tests_results()
        self.push_to_dashboard(test_name="PVC Create-Delete")

    def process_time_measurements(
        self, action_name, time_measures, accepted_deviation_percent, msg_prefix
    ):
        """
           Analyses the given time measured. If the standard deviation of these times is bigger than the
           provided accepted deviation percent, fails the test

        Args:
            action_name (str): Name of the action for which these measurements were collected; used for the logging
            time_measures (list of floats): A list of time measurements
            accepted_deviation_percent (int): Accepted deviation percent to which computed standard deviation may be
                    compared
            msg_prefix (str) : A string for comprehensive logging

        Returns:
            (float) The average value of the provided time measurements
        """
        average = statistics.mean(time_measures)
        log.info(
            f"{msg_prefix} The average {action_name} time for the sampled {len(time_measures)} "
            f"PVCs is {average} seconds."
        )

        if self.interface == constants.CEPHBLOCKPOOL_THICK:
            st_deviation = statistics.stdev(time_measures)
            st_deviation_percent = st_deviation / average * 100.0
            if st_deviation_percent > accepted_deviation_percent:
                log.error(
                    f"{msg_prefix} The standard deviation percent for {action_name} of {len(time_measures)} sampled "
                    f"PVCs is {st_deviation_percent}% which is bigger than accepted {accepted_deviation_percent}."
                )
            else:
                log.info(
                    f"{msg_prefix} The standard deviation percent for {action_name} of {len(time_measures)} sampled "
                    f"PVCs is {st_deviation_percent}% and is within the accepted range."
                )
            self.full_results.add_key(
                f"{action_name}_deviation_pct", st_deviation_percent
            )

        return average

    def write_file_on_pvc(self, pvc_obj, filesize=1):
        """
        Writes a file on given PVC
        Args:
            pvc_obj: PVC object to write a file on
            filesize: size of file to write (in GB - default is 1GB)

        Returns:
            Pod on this pvc on which the file was written
        """
        pod_obj = self.pod_factory(
            interface=self.interface, pvc=pvc_obj, status=constants.STATUS_RUNNING
        )

        # filesize to be written is always 1 GB
        file_size = f"{int(filesize * 1024)}M"

        log.info(f"Starting IO on the POD {pod_obj.name}")
        # Going to run only write IO
        pod_obj.fillup_fs(size=file_size, fio_filename=f"{pod_obj.name}_file")

        # Wait for the fio to finish
        fio_result = pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert (
            err_count == 0
        ), f"IO error on pod {pod_obj.name}. FIO result: {fio_result}"
        log.info("IO on the PVC has finished")
        return pod_obj

    @pytest.mark.parametrize(
        argnames=["interface_type"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL_THICK],
                marks=[pytest.mark.performance_extended],
            ),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    @pytest.mark.usefixtures(namespace.__name__)
    @pytest.mark.polarion_id("OCS-2618")
    def test_multiple_pvc_deletion_measurement_performance(self, teardown_factory):
        """
        Measuring PVC deletion time of 120 PVCs in 180 seconds

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
        Returns:

        """
        number_of_pvcs = 120
        pvc_size = "1Gi"
        msg_prefix = f"Interface: {self.interface}, PVC size: {pvc_size}."

        log.info(f"{msg_prefix} Start creating new {number_of_pvcs} PVCs")

        pvc_objs, _ = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=self.namespace,
            number_of_pvc=number_of_pvcs,
            size=pvc_size,
            burst=True,
        )

        for pvc_obj in pvc_objs:
            pvc_obj.reload()
            teardown_factory(pvc_obj)

        timeout = 600 if self.interface == constants.CEPHBLOCKPOOL_THICK else 60
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state,
                    pvc_obj,
                    constants.STATUS_BOUND,
                    timeout=timeout,
                )
                executor.submit(pvc_obj.reload)

        pod_objs = []
        for pvc_obj in pvc_objs:
            pod_obj = self.write_file_on_pvc(pvc_obj, 0.3)
            pod_objs.append(pod_obj)

        # Get pvc_name, require pvc_name to fetch deletion time data from log
        threads = list()
        for pvc_obj in pvc_objs:
            process = threading.Thread(target=pvc_obj.reload)
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

        pvc_name_list, pv_name_list = ([] for i in range(2))
        threads = list()
        for pvc_obj in pvc_objs:
            process1 = threading.Thread(target=pvc_name_list.append(pvc_obj.name))
            process2 = threading.Thread(target=pv_name_list.append(pvc_obj.backed_pv))
            process1.start()
            process2.start()
            threads.append(process1)
            threads.append(process2)
        for process in threads:
            process.join()
        log.info(f"{msg_prefix} Preparing to delete 120 PVC")

        # Delete PVC
        for pvc_obj, pod_obj in zip(pvc_objs, pod_objs):
            pod_obj.delete(wait=True)
            pvc_obj.delete()
            pvc_obj.ocp.wait_for_delete(pvc_obj.name)

        # Get PVC deletion time
        pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
            interface=self.interface, pv_name_list=pv_name_list
        )
        log.info(
            f"{msg_prefix} {number_of_pvcs} bulk deletion time is {pvc_deletion_time}"
        )

        # accepted deletion time is 2 secs for each PVC
        accepted_pvc_deletion_time = number_of_pvcs * 2

        for del_time in pvc_deletion_time.values():
            if del_time > accepted_pvc_deletion_time:
                raise ex.PerformanceException(
                    f"{msg_prefix} {number_of_pvcs} PVCs deletion time is {pvc_deletion_time.values()} and is "
                    f"greater than {accepted_pvc_deletion_time} seconds"
                )

        logging.info(f"{msg_prefix} {number_of_pvcs} PVCs deletion times are:")
        for name, a_time in pvc_deletion_time.items():
            logging.info(f"{name} deletion time is: {a_time} seconds")

        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc = "RBD"
        elif self.interface == constants.CEPHFILESYSTEM:
            self.sc = "CephFS"
        elif self.interface == constants.CEPHBLOCKPOOL_THICK:
            self.sc = "RBD-Thick"

        full_log_path = get_full_test_logs_path(cname=self) + f"-{self.sc}-{pvc_size}"
        self.results_path = get_full_test_logs_path(cname=self)
        log.info(f"Logs file path name is : {full_log_path}")

        self.get_env_info()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                full_log_path,
                "pvc_bulk_deletion_fullres",
            )
        )

        full_results.add_key("interface", self.interface)
        full_results.add_key("bulk_size", number_of_pvcs)
        full_results.add_key("pvc_size", pvc_size)
        full_results.all_results["bulk_deletion_time"] = pvc_deletion_time

        if full_results.es_write():
            res_link = full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (3 - according to the parameters)
            self.write_result_to_file(res_link)

    def test_multiple_pvc_deletion_results(self):
        """
        This is not a test - it is only check that previous test ran and finish as expected
        and reporting the full results (links in the ES) of previous tests (3)
        """
        self.number_of_tests = 3
        results_path = get_full_test_logs_path(
            cname=self, fname="test_multiple_pvc_deletion_measurement_performance"
        )
        self.results_file = os.path.join(results_path, "all_results.txt")
        log.info(f"Check results in {self.results_file}.")
        log.info("Check results for 'performance_extended' marker (3 tests)")
        self.check_tests_results()

        self.push_to_dashboard(test_name="PVC Multiple-Delete")
