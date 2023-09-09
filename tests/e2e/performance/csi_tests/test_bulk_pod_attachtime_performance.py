"""
Test to verify performance of attaching number of pods as a bulk, each pod attached to one pvc only
The test results will be uploaded to the ES server
"""
import json
import logging
import pytest
import pathlib
import time
from datetime import datetime

from ocs_ci.framework.testlib import performance, performance_a, polarion_id
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.helpers.storageclass_helpers import storageclass_name

log = logging.getLogger(__name__)

Interfaces_info = {
    constants.CEPHBLOCKPOOL: {
        "name": "RBD",
        "sc_interface": constants.OCS_COMPONENTS_MAP["blockpools"],
    },
    constants.CEPHFILESYSTEM: {
        "name": "CephFS",
        "sc_interface": constants.OCS_COMPONENTS_MAP["cephfs"],
    },
}


@performance
@performance_a
class TestBulkPodAttachPerformance(PASTest):
    """
    Test to measure performance of attaching pods to pvc in a bulk
    """

    pvc_size = "1Gi"

    def setup(self):
        """
        Setting up test parameters
        """
        log.info("Starting the test setup")
        super(TestBulkPodAttachPerformance, self).setup()
        self.benchmark_name = "bulk_pod_attach_time"

        self.create_test_project()
        # Pulling the pod image to the worker node, so pull image will not calculate
        # in the total attach time
        helpers.pull_images(constants.PERF_IMAGE)

        # Initializing some parameters
        self.pvc_objs = list()
        self.pods_obj = locals()

    def teardown(self):
        """
        Cleanup the test environment
        """
        log.info("Starting the test environment celanup")

        # Deleting All POD(s)
        log.info("Try to delete all created PODs")
        try:
            self.pods_obj.delete(namespace=self.namespace)
        except Exception as ex:
            log.warn(f"Failed to delete POD(s) [{ex}]")
        log.info("Wait for all PODs to be deleted")
        performance_lib.wait_for_resource_bulk_status(
            "pod", 0, self.namespace, constants.STATUS_BOUND, len(self.pvc_objs) * 2, 10
        )
        log.info("All POD(s) was deleted")

        # Deleting PVC(s) for deletion time measurement
        log.info("Try to delete all created PVCs")
        for pvc_obj in self.pvc_objs:
            pvc_obj.delete()
        log.info("Wait for all PVC(s) to be deleted")
        performance_lib.wait_for_resource_bulk_status(
            "pvc", 0, self.namespace, constants.STATUS_BOUND, len(self.pvc_objs) * 2, 10
        )
        log.info("All PVC(s) was deleted")
        log.info("Wait for all PVC(s) backed PV(s) to be deleted")
        # Timeout for each PV to be deleted is 20 sec.
        performance_lib.wait_for_resource_bulk_status(
            "pv", 0, self.namespace, self.namespace, len(self.pvc_objs) * 20, 10
        )
        log.info("All backed PV(s) was deleted")

        # Delete the test project (namespace)
        self.delete_test_project()

        super(TestBulkPodAttachPerformance, self).teardown()

    @pytest.mark.parametrize(
        argnames=["interface_type", "bulk_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 120],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 240],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 120],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 240],
            ),
        ],
    )
    @polarion_id("OCS-1620")
    def test_bulk_pod_attach_performance(self, interface_type, bulk_size):

        """
        Measures pods attachment time in bulk_size bulk

        Args:
            interface_type (str): The interface type to be tested - CephBlockPool / CephFileSystem.
            bulk_size (int): Size of the bulk to be tested
        Returns:

        """
        self.interface = interface_type

        if self.dev_mode:
            bulk_size = 3

        # Initialize some variables
        timeout = bulk_size * 5
        pvc_names_list = list()
        pod_data_list = list()

        # Getting the test start time
        test_start_time = self.get_time()
        csi_start_time = self.get_time("csi")

        log.info(f"Start creating bulk of new {bulk_size} PVCs")
        self.pvc_objs, _ = helpers.create_multiple_pvcs(
            sc_name=storageclass_name(Interfaces_info[self.interface]["sc_interface"]),
            namespace=self.namespace,
            number_of_pvc=bulk_size,
            size=self.pvc_size,
            burst=True,
            do_reload=False,
        )
        log.info("Wait for all of the PVCs to be in Bound state")
        performance_lib.wait_for_resource_bulk_status(
            "pvc", bulk_size, self.namespace, constants.STATUS_BOUND, timeout, 10
        )
        # in case of creation failure, the wait_for_resource_bulk_status function
        # will raise an exception. so in this point the creation succeed
        log.info("All PVCs was created and in Bound state.")

        # Reload all PVC(s) information
        for pvc_obj in self.pvc_objs:
            pvc_obj.reload()
            pvc_names_list.append(pvc_obj.name)
        log.debug(f"The PVCs names are : {pvc_names_list}")

        # Create kube_job for pod creation
        pod_data_list.extend(
            scale_lib.attach_multiple_pvc_to_pod_dict(
                pvc_list=pvc_names_list,
                namespace=self.namespace,
                pvcs_per_pod=1,
            )
        )
        self.pods_obj = ObjectConfFile(
            name="pod_kube_obj",
            obj_dict_list=pod_data_list,
            project=self.namespace,
            tmp_path=pathlib.Path(ocsci_log_path()),
        )
        log.debug(f"PODs data list is : {json.dumps(pod_data_list, indent=3)}")

        log.info(f"{self.interface} : Before pod attach")
        bulk_start_time = time.time()
        self.pods_obj.create(namespace=self.namespace)
        # Check all the PODs reached Running state
        log.info("Checking that pods are running")
        performance_lib.wait_for_resource_bulk_status(
            "pod", bulk_size, self.namespace, constants.STATUS_RUNNING, timeout, 2
        )
        log.info("All the POD(s) are in Running State.")
        bulk_end_time = time.time()
        bulk_total_time = bulk_end_time - bulk_start_time

        bulk_start_time_str = datetime.fromtimestamp(bulk_start_time).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        bulk_end_time_str = datetime.fromtimestamp(bulk_end_time).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        log.info(
            f"Total bulk attach start time of {bulk_size} pods is {bulk_start_time_str}"
        )
        log.info(
            f"Total bulk attach end time of {bulk_size} pods is {bulk_end_time_str})"
        )
        log.info(
            f"Total bulk attach time of {bulk_size} pods is {bulk_total_time} seconds"
        )

        csi_bulk_total_time = performance_lib.pod_bulk_attach_csi_time(
            self.interface, self.pvc_objs, csi_start_time, self.namespace
        )
        log.info(
            f"CSI bulk attach time of {bulk_size} pods is {csi_bulk_total_time} seconds"
        )

        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        full_results = self.init_full_results(
            ResultsAnalyse(
                self.uuid, self.crd_data, self.full_log_path, "pod_bulk_attachtime"
            )
        )

        full_results.add_key("storageclass", Interfaces_info[self.interface]["name"])
        full_results.add_key("pod_bulk_attach_time", bulk_total_time)
        full_results.add_key("pod_csi_bulk_attach_time", csi_bulk_total_time)
        full_results.add_key("pvc_size", self.pvc_size)
        full_results.add_key("bulk_size", bulk_size)

        # Getting the test end time
        test_end_time = self.get_time()

        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": test_start_time, "end": test_end_time}
        )

        # Write the test results into the ES server
        self.results_path = helpers.get_full_test_logs_path(cname=self)
        if full_results.es_write():
            res_link = full_results.results_link()
            # write the ES link to the test results in the test log.
            log.info(f"The result can be found at : {res_link}")

            # Create text file with results of all subtests (4 - according to the parameters)
            self.write_result_to_file(res_link)

    def test_bulk_pod_attach_results(self):
        """
        This is not a test - it is only check that previous test ran and finish as expected
        and reporting the full results (links in the ES) of previous tests (4)
        """

        self.add_test_to_results_check(
            test="test_bulk_pod_attach_performance",
            test_count=4,
            test_name="Bulk Pod Attach Time",
        )
        self.check_results_and_push_to_dashboard()

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
