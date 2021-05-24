"""
Test to verify PVC deletion performance
"""
import time
import logging
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
from ocs_ci.ocs import defaults, constants
from ocs_ci.utility.performance_dashboard import push_to_pvc_time_dashboard
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.framework import config


log = logging.getLogger(__name__)


class ResultsAnalyse(PerfResult):
    """
    This class generates results for all tests as one unit
    and save them to an elasticsearch server

    """

    def __init__(self, uuid, crd, full_log_path):
        """
        Initialize the object by reading some of the data from the CRD file and
        by connecting to the ES server and read all results from it.

        Args:
            uuid (str): the unique uid of the test
            crd (dict): dictionary with test parameters - the test yaml file
                        that modify it in the test itself.
            full_log_path (str): the path of the results files to be found

        """
        super(ResultsAnalyse, self).__init__(uuid, crd)
        self.new_index = "pvc_create_delete_fullres"
        self.full_log_path = full_log_path
        # make sure we have connection to the elastic search server
        self.es_connect()


@performance
class TestPVCCreationDeletionPerformance(PASTest):
    """
    Test to verify PVC creation and deletion performance
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
    def base_setup(self, interface_iterate, storageclass_factory, pod_factory):
        """
        A setup phase for the test

        Args:
            interface_iterate: A fixture to iterate over ceph interfaces
            storageclass_factory: A fixture to create everything needed for a
                storageclass
            pod_factory: A fixture to create new pod
        """
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)
        self.pod_factory = pod_factory

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an empty FIOResultsAnalyse object

        Returns:
            FIOResultsAnalyse (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])
        full_results.add_key("storageclass", self.sc)
        full_results.add_key("index", full_results.new_index)
        return full_results

    @pytest.mark.parametrize(
        argnames=["pvc_size"],
        argvalues=[
            pytest.param(*["25Gi"], marks=pytest.mark.polarion_id("OCS-2007")),
            pytest.param(*["50Gi"], marks=pytest.mark.polarion_id("OCS-2007")),
            pytest.param(*["100Gi"], marks=pytest.mark.polarion_id("OCS-2007")),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    def test_pvc_creation_deletion_measurement_performance(
        self, teardown_factory, pvc_size
    ):
        """
        Measuring PVC creation and deletion times for pvc samples
        Verifying that those times are within required limits
        """

        # Getting the full path for the test logs
        self.full_log_path = get_full_test_logs_path(cname=self)
        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc = "RBD"
        if self.interface == constants.CEPHFILESYSTEM:
            self.sc = "CephFS"
        self.full_log_path += f"-{self.sc}-{pvc_size}"
        log.info(f"Logs file path name is : {self.full_log_path}")

        self.start_time = time.strftime("%Y-%m-%dT%H:%M:%SGMT", time.gmtime())

        self.get_env_info()

        # Initialize the results doc file.
        self.full_results = self.init_full_results(
            ResultsAnalyse(self.uuid, self.crd_data, self.full_log_path)
        )
        self.full_results.add_key("pvc_size", pvc_size)
        num_of_samples = 5
        accepted_creation_time = 1
        accepted_deletion_time = 2 if self.interface == constants.CEPHFILESYSTEM else 1
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
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
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
        self.full_results.es_write()
        log.info(f"The Result can be found at : {self.full_results.results_link()}")

        if not self.dev_mode:
            # all the results are OK, the test passes, push the results to the codespeed
            push_to_pvc_time_dashboard(
                self.interface, "1-pvc-creation", creation_average
            )
            push_to_pvc_time_dashboard(
                self.interface, "1-pvc-deletion", deletion_average
            )

    def process_time_measurements(
        self, action_name, time_measures, accepted_deviation_percent, msg_prefix
    ):
        """
           Analyses the given time measured. If the standard deviation of these times is bigger than the
           provided accepted deviation percent, fails the test

        Args:
            action_name (str): Name of the action for which these measurements were collected; used for the logging
            time_measures (list of floats): A list of time measurements
            accepted_deviation_percent (int): Accepted deviation percent,
                if the standard  deviation of the provided time measurements is bigger than this value, the test fails
            msg_prefix (str) : A string for comprehensive logging

        Returns:
            (float) The average value of the provided time measurements
        """
        average = statistics.mean(time_measures)
        log.info(
            f"{msg_prefix} The average {action_name} time for the sampled {len(time_measures)} "
            f"PVCs is {average} seconds."
        )

        st_deviation = statistics.stdev(time_measures)
        st_deviation_percent = st_deviation / average * 100.0
        if st_deviation_percent > accepted_deviation_percent:
            raise ex.PerformanceException(
                f"{msg_prefix} PVC ${action_name} time deviation is {st_deviation_percent}% "
                f"and is greater than the allowed {accepted_deviation_percent}%."
            )

        self.full_results.add_key(f"{action_name}_deviation_pct", st_deviation_percent)
        log.info(
            f"{msg_prefix} The standard deviation percent for {action_name} of {len(time_measures)} sampled "
            f"PVCs is {st_deviation_percent}%."
        )

        return average

    def write_file_on_pvc(self, pvc_obj, filesize=10):
        """
        Writes a file on given PVC
        Args:
            pvc_obj: PVC object to write a file on
            filesize: size of file to write (in GB)

        Returns:
            Pod on this pvc on which the file was written
        """
        pod_obj = self.pod_factory(
            interface=self.interface, pvc=pvc_obj, status=constants.STATUS_RUNNING
        )

        # filesize to be written is always 10 GB
        file_size = f"{int(filesize * 1024)}M"

        log.info(f"Starting IO on the POD {pod_obj.name}")
        # Going to run only write IO
        pod_obj.fillup_fs(size=file_size, fio_filename=f"{pod_obj.name}_file")

        # Wait for fio to finish
        fio_result = pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert (
            err_count == 0
        ), f"IO error on pod {pod_obj.name}. FIO result: {fio_result}"
        log.info("IO on the PVC has finished")
        return pod_obj

    @pytest.mark.usefixtures(base_setup.__name__)
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

        log.info(f"{msg_prefix} Start creating new 120 PVCs")

        pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            number_of_pvc=number_of_pvcs,
            size=pvc_size,
            burst=True,
        )

        for pvc_obj in pvc_objs:
            pvc_obj.reload()
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj, constants.STATUS_BOUND
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
