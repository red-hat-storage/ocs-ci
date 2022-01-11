"""
Test to verify PVC creation performance
"""
import logging
import pytest
import math
import datetime
import os
from uuid import uuid4

import ocs_ci.ocs.exceptions as ex
import ocs_ci.ocs.resources.pvc as pvc
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework import config
from ocs_ci.framework.testlib import performance, polarion_id, bugzilla
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs import constants
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.perfresult import ResultsAnalyse

log = logging.getLogger(__name__)


@performance
class TestPVCCreationPerformance(PASTest):
    def setup(self):
        """
        Setting up test parameters
        """
        logging.info("Starting the test setup")
        super(TestPVCCreationPerformance, self).setup()
        self.benchmark_name = "pvc_creation_performance"
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
        # during development use the dev ES so the data in the Production ES will be clean.
        if self.dev_mode:
            self.crd_data["spec"]["elasticsearch"] = {
                "server": config.PERF.get("dev_es_server"),
                "port": config.PERF.get("dev_es_port"),
                "url": f"http://{config.PERF.get('dev_es_server')}:{config.PERF.get('dev_es_port')}",
            }

    def init_full_results(self, full_results):
        """
        Initialize the full results object which will send to the ES server

        Args:
            full_results (obj): an FIOResultsAnalyse object

        Returns:
            FIOResultsAnalyse (obj): the input object fill with data

        """
        for key in self.environment:
            full_results.add_key(key, self.environment[key])
        full_results.add_key("index", full_results.new_index)
        return full_results

    """
    Test to verify PVC creation and deletion performance
    """

    pvc_size = "1Gi"

    @pytest.fixture()
    def base_setup(self, interface_type, storageclass_factory):
        """
        A setup phase for the test

        Args:
            interface_type: Interface type
            storageclass_factory: A fixture to create everything needed for a
                storageclass
        """
        self.interface = interface_type
        self.sc_obj = storageclass_factory(self.interface)

        if self.interface == constants.CEPHFILESYSTEM:
            sc = "CephFS"
        if self.interface == constants.CEPHBLOCKPOOL:
            sc = "RBD"

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{sc}"

    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

    @pytest.mark.parametrize(
        argnames=["interface_type", "bulk_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 60],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 240],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 60],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 240],
                marks=[pytest.mark.performance],
            ),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    @pytest.mark.usefixtures(namespace.__name__)
    @polarion_id("OCS-1620")
    def test_bulk_pvc_creation_deletion_measurement_performance(
        self, teardown_factory, bulk_size
    ):

        """
        Measuring PVC creation and deletion time of bulk_size PVCs
        and sends results to the Elastic Search DB

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
            bulk_size: Size of the bulk to be tested
        Returns:

        """
        bulk_creation_time_limit = bulk_size / 2
        log.info(f"Start creating new {bulk_size} PVCs")

        pvc_objs, yaml_creation_dir = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=self.namespace,
            number_of_pvc=bulk_size,
            size=self.pvc_size,
            burst=True,
        )
        logging.info(f"PVC creation dir is {yaml_creation_dir}")

        for pvc_obj in pvc_objs:
            pvc_obj.reload()
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj, constants.STATUS_BOUND
                )
                executor.submit(pvc_obj.reload)

        start_time = helpers.get_provision_time(
            self.interface, pvc_objs, status="start"
        )
        end_time = helpers.get_provision_time(self.interface, pvc_objs, status="end")
        total_time = (end_time - start_time).total_seconds()
        logging.info(f"{bulk_size} Bulk PVCs creation time is {total_time} seconds.")

        if total_time > bulk_creation_time_limit:
            raise ex.PerformanceException(
                f"{bulk_size} Bulk PVCs creation time is {total_time} and "
                f"greater than {bulk_creation_time_limit} seconds"
            )

        pv_names_list = []
        for pvc_obj in pvc_objs:
            pv_names_list.append(pvc_obj.backed_pv)

        logging.info(f"Starting to delete bulk of {bulk_size} PVCs")
        helpers.delete_bulk_pvcs(
            yaml_creation_dir, pv_names_list, namespace=self.namespace
        )
        logging.info(f"Deletion of bulk of {bulk_size} PVCs successfully completed")

        log_deletion_times = helpers.measure_pv_deletion_time_bulk(
            self.interface, pv_names_list, return_log_times=True
        )

        all_start_times = [a_tuple[0] for a_tuple in log_deletion_times.values()]
        bulk_start_time = sorted(all_start_times)[0]  # the eariles start time
        start_deletion_time = datetime.datetime.strptime(
            bulk_start_time, helpers.DATE_TIME_FORMAT
        )

        all_end_times = [a_tuple[1] for a_tuple in log_deletion_times.values()]
        bulk_deletion_time = sorted(all_end_times)[-1]  # the latest end time
        end_deletion_time = datetime.datetime.strptime(
            bulk_deletion_time, helpers.DATE_TIME_FORMAT
        )

        total_deletion_time = (end_deletion_time - start_deletion_time).total_seconds()
        logging.info(
            f"{bulk_size} Bulk PVCs deletion time is {total_deletion_time} seconds."
        )

        self.results_path = get_full_test_logs_path(cname=self)
        # Produce ES report
        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "bulk_creation_deletion_measurement",
            )
        )

        full_results.add_key("interface", self.interface)
        full_results.add_key("bulk_size", bulk_size)
        full_results.add_key("pvc_size", self.pvc_size)
        full_results.add_key("bulk_pvc_creation_time", total_time)
        full_results.add_key("bulk_pvc_deletion_time", total_deletion_time)

        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (4 - according to the parameters)
            self.write_result_to_file(res_link)

    @pytest.fixture()
    def base_setup_creation_after_deletion(
        self, interface_iterate, storageclass_factory
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

        if self.interface == constants.CEPHFILESYSTEM:
            sc = "CephFS"
        if self.interface == constants.CEPHBLOCKPOOL:
            sc = "RBD"

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{sc}"

    @pytest.mark.usefixtures(base_setup_creation_after_deletion.__name__)
    @pytest.mark.usefixtures(namespace.__name__)
    @polarion_id("OCS-1270")
    @bugzilla("1741612")
    def test_bulk_pvc_creation_after_deletion_performance(self, teardown_factory):
        """
        Measuring PVC creation time of bulk of 75% of initial PVC bulk (120) in the same
        rate after deleting ( serial deletion) 75% of the initial PVCs
        and sends results to the Elastic Search DB

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
        Returns:

        """
        initial_number_of_pvcs = 120
        number_of_pvcs = math.ceil(initial_number_of_pvcs * 0.75)

        log.info(f"Start creating new {initial_number_of_pvcs} PVCs in a bulk")
        pvc_objs, _ = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=self.namespace,
            number_of_pvc=initial_number_of_pvcs,
            size=self.pvc_size,
            burst=True,
        )
        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor() as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj, constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)
        log.info(f"Deleting 75% of the PVCs - {number_of_pvcs} PVCs")
        assert pvc.delete_pvcs(
            pvc_objs[:number_of_pvcs], True
        ), "Deletion of 75% of PVCs failed"
        log.info(f"Re-creating the {number_of_pvcs} PVCs")
        pvc_objs, _ = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=self.namespace,
            number_of_pvc=number_of_pvcs,
            size=self.pvc_size,
            burst=True,
        )
        start_time = helpers.get_provision_time(
            self.interface, pvc_objs, status="start"
        )
        end_time = helpers.get_provision_time(self.interface, pvc_objs, status="end")
        total = end_time - start_time
        total_time = total.total_seconds()
        logging.info(
            f"Creation after deletion time of {number_of_pvcs} is {total_time} seconds."
        )

        for pvc_obj in pvc_objs:
            teardown_factory(pvc_obj)
        with ThreadPoolExecutor() as executor:
            for pvc_obj in pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj, constants.STATUS_BOUND
                )

                executor.submit(pvc_obj.reload)
        if total_time > 50:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation (after initial deletion of "
                f"75% of PVCs) time is {total_time} and greater than 50 seconds."
            )
        logging.info(f"{number_of_pvcs} PVCs creation time took less than a 50 seconds")

        self.results_path = get_full_test_logs_path(cname=self)
        # Produce ES report
        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid,
                self.crd_data,
                self.full_log_path,
                "bulk_pvc_creation_after_deletion_measurement",
            )
        )

        full_results.add_key("interface", self.interface)
        full_results.add_key("number_of_pvcs", number_of_pvcs)
        full_results.add_key("pvc_size", self.pvc_size)
        full_results.add_key("creation_after_deletion_time", total_time)

        # Write the test results into the ES server
        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (2 - according to the parameters)
            self.write_result_to_file(res_link)

    def test_bulk_pvc_creation_deletion_performance_results(self):
        """
        This is not a test - it is only check that previous tests ran and finished as expected
        and reporting the full results (links in the ES) of previous tests (4 + 2)
        """

        workloads = [
            {
                "name": "test_bulk_pvc_creation_deletion_measurement_performance",
                "tests": 4,
                "test_name": "PVC Bulk Creation-Deletion",
            },
            {
                "name": "test_bulk_pvc_creation_after_deletion_performance",
                "tests": 2,
                "test_name": "PVC Bulk Creation-After-Deletion",
            },
        ]
        for wl in workloads:
            self.number_of_tests = wl["tests"]
            self.results_path = get_full_test_logs_path(cname=self, fname=wl["name"])
            self.results_file = os.path.join(self.results_path, "all_results.txt")
            log.info(f"Check results for [{wl['name']}] in : {self.results_file}")
            self.check_tests_results()
            self.push_to_dashboard(test_name=wl["test_name"])
