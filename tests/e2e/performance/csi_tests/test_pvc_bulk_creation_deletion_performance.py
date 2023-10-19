"""
Test to verify PVC creation performance
"""
import logging
import os
import pytest
import math
import datetime

import ocs_ci.ocs.exceptions as ex
import ocs_ci.ocs.resources.pvc as pvc
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.framework.testlib import performance, performance_a, polarion_id
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.perfresult import ResultsAnalyse

log = logging.getLogger(__name__)

Interface_Types = {constants.CEPHFILESYSTEM: "CephFS", constants.CEPHBLOCKPOOL: "RBD"}


@grey_squad
@performance
@performance_a
class TestPVCCreationPerformance(PASTest):
    """
    Test to verify PVC creation and deletion performance
    """

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestPVCCreationPerformance, self).setup()
        self.benchmark_name = "pvc_creation_performance"
        # Create new project (namespace for the test)
        self.create_test_project()
        self.pvc_size = "1Gi"
        self.pvc_objs = []

    def teardown(self):
        """
        Cleanup the test environment
        """

        log.info("Starting the test environment celanup")
        # Delete All PVC (if exists)
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
        # Delete the test project (namespace)
        self.delete_test_project()
        super(TestPVCCreationPerformance, self).teardown()

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
        full_results.add_key("interface", Interface_Types[self.interface])
        full_results.add_key("pvc_size", self.pvc_size)
        return full_results

    def pvc_bulk_create_and_wait_for_bound(self, bulk_size):
        """
        Creating a bulk of PVCs and wait until all of them are bounded

        Args:
        bulk_size (int): the number of pvcs to create

        """
        self.pvc_objs, self.yaml_creation_dir = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=self.namespace,
            number_of_pvc=bulk_size,
            size=self.pvc_size,
            burst=True,
        )
        with ThreadPoolExecutor(max_workers=5) as executor:
            for pvc_obj in self.pvc_objs:
                executor.submit(
                    helpers.wait_for_resource_state, pvc_obj, constants.STATUS_BOUND
                )
                executor.submit(pvc_obj.reload)

    def get_bulk_creation_time(self):
        """
        Getting the creation time for the pvcs (as bulk). this time is the total
        time it took to create, from user Perspective.

        Returns:

            int : the total time in seconds

        """
        start_time = helpers.get_provision_time(
            self.interface, self.pvc_objs, status="start"
        )
        end_time = helpers.get_provision_time(
            self.interface, self.pvc_objs, status="end"
        )
        total_time = (end_time - start_time).total_seconds()
        return total_time

    @pytest.mark.parametrize(
        argnames=["interface_type", "bulk_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 60],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 240],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 60],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 240],
            ),
        ],
    )
    @polarion_id("OCS-1620")
    def test_bulk_pvc_creation_deletion_measurement_performance(
        self, storageclass_factory, interface_type, bulk_size
    ):
        """
        Measuring PVC creation and deletion time of bulk_size PVCs
        and sends results to the Elastic Search DB

        Args:
            bulk_size: Size of the bulk to be tested
        Returns:

        """
        self.interface = interface_type
        self.sc_obj = storageclass_factory(self.interface)

        bulk_creation_time_limit = bulk_size  # old value was bulk_size / 2

        log.info(f"Start creating new {bulk_size} PVCs")

        # Getting the start time of the test.
        self.test_start_time = self.get_time()

        # Run the Bulk Creation test
        csi_bulk_start_time = self.get_time(time_format="csi")
        self.pvc_bulk_create_and_wait_for_bound(bulk_size)
        log.info(f"PVC creation dir is {self.yaml_creation_dir}")

        total_time = self.get_bulk_creation_time()
        log.info(f"{bulk_size} Bulk PVCs creation time is {total_time} seconds.")
        csi_creation_times = performance_lib.csi_bulk_pvc_time_measure(
            self.interface, self.pvc_objs, "create", csi_bulk_start_time
        )

        if total_time > bulk_creation_time_limit:
            raise ex.PerformanceException(
                f"{bulk_size} Bulk PVCs creation time is {total_time} and "
                f"greater than {bulk_creation_time_limit} seconds"
            )

        # Run the Bulk Deletion test
        pv_names_list = []
        for pvc_obj in self.pvc_objs:
            pv_names_list.append(pvc_obj.backed_pv)

        log.info(f"Starting to delete bulk of {bulk_size} PVCs")
        helpers.delete_bulk_pvcs(
            self.yaml_creation_dir, pv_names_list, namespace=self.namespace
        )
        log.info(f"Deletion of bulk of {bulk_size} PVCs successfully completed")

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
        log.info(
            f"{bulk_size} Bulk PVCs deletion time is {total_deletion_time} seconds."
        )

        csi_deletion_times = performance_lib.csi_bulk_pvc_time_measure(
            self.interface, self.pvc_objs, "delete", csi_bulk_start_time
        )
        # Getting the end time of the test
        self.test_end_time = self.get_time()

        # reset the list oc PVCs since thay was deleted, and do not need to be deleted
        # in the teardown phase.
        self.pvc_objs = []

        # Produce ES report
        self.results_path = os.path.join(
            "/",
            *self.results_path,
            "test_bulk_pvc_creation_deletion_measurement_performance",
        )

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

        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": self.test_start_time, "end": self.test_end_time}
        )
        full_results.add_key("bulk_size", bulk_size)
        full_results.add_key("bulk_pvc_creation_time", total_time)
        full_results.add_key("bulk_pvc_csi_creation_time", csi_creation_times)
        full_results.add_key("bulk_pvc_deletion_time", total_deletion_time)
        full_results.add_key("bulk_pvc_csi_deletion_time", csi_deletion_times)

        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (4 - according to the parameters)
            self.write_result_to_file(res_link)

    @polarion_id("OCS-1270")
    def test_bulk_pvc_creation_after_deletion_performance(
        self, interface_iterate, storageclass_factory
    ):
        """
        Measuring PVC creation time of bulk of 75% of initial PVC bulk (120) in the same
        rate after deleting ( serial deletion) 75% of the initial PVCs
        and sends results to the Elastic Search DB

        """
        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)
        initial_number_of_pvcs = 120
        number_of_pvcs = math.ceil(initial_number_of_pvcs * 0.75)

        # Getting the test start time
        self.test_start_time = self.get_time()

        log.info(f"Start creating new {initial_number_of_pvcs} PVCs in a bulk")
        self.pvc_bulk_create_and_wait_for_bound(initial_number_of_pvcs)

        log.info(f"Deleting 75% of the PVCs - {number_of_pvcs} PVCs")
        assert pvc.delete_pvcs(
            self.pvc_objs[:number_of_pvcs], True
        ), "Deletion of 75% of PVCs failed"
        # save the list of pvcs which not deleted, for the teardown phase
        original_pvcs = self.pvc_objs[number_of_pvcs:]

        log.info(f"Re-creating the {number_of_pvcs} PVCs")
        csi_bulk_start_time = self.get_time(time_format="csi")
        self.pvc_bulk_create_and_wait_for_bound(number_of_pvcs)

        # Get the bulk recraation time - total time.
        total_time = self.get_bulk_creation_time()
        log.info(
            f"Creation after deletion time of {number_of_pvcs} is {total_time} seconds."
        )

        total_time_accpeted = 600  # old value was 60
        if total_time > total_time_accpeted:
            raise ex.PerformanceException(
                f"{number_of_pvcs} PVCs creation (after initial deletion of "
                f"75% of PVCs) time is {total_time} and greater than {total_time_accpeted} seconds."
            )
        log.info(
            f"{number_of_pvcs} PVCs creation time took less than a {total_time_accpeted} seconds"
        )

        csi_creation_times = performance_lib.csi_bulk_pvc_time_measure(
            self.interface, self.pvc_objs, "create", csi_bulk_start_time
        )
        # Getting the end time of the test
        self.test_end_time = self.get_time()

        # update the list of pvcs for the teardown process
        self.pvc_objs += original_pvcs

        # Produce ES report
        self.results_path = os.path.join(
            "/",
            *self.results_path,
            "test_bulk_pvc_creation_after_deletion_performance",
        )
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

        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": self.test_start_time, "end": self.test_end_time}
        )

        full_results.add_key("number_of_pvcs", number_of_pvcs)
        full_results.add_key("creation_after_deletion_time", total_time)
        full_results.add_key("creation_after_deletion_csi_time", csi_creation_times)

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

        self.add_test_to_results_check(
            test="test_bulk_pvc_creation_deletion_measurement_performance",
            test_count=4,
            test_name="PVC Bulk Creation-Deletion",
        )
        self.add_test_to_results_check(
            test="test_bulk_pvc_creation_after_deletion_performance",
            test_count=2,
            test_name="PVC Bulk Creation-After-Deletion",
        )
        self.check_results_and_push_to_dashboard()
