"""
Test to verify performance of attaching number of pods as a bulk, each pod attached to one pvc only
The test results will be uploaded to the ES server
"""
import logging
import pytest
import pathlib
import time
from uuid import uuid4

from concurrent.futures import ThreadPoolExecutor
from ocs_ci.framework.testlib import performance, polarion_id
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.framework import config
from ocs_ci.ocs import defaults, constants, scale_lib
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.utility.utils import ocsci_log_path

log = logging.getLogger(__name__)


class ResultsAnalyse(PerfResult):
    """
    This class generates results for all tests as one unit
    and saves them to an elastic search server on the cluster

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
        self.new_index = "pod_bulk_attachtime"
        self.full_log_path = full_log_path
        # make sure we have connection to the elastic search server
        self.es_connect()


@performance
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

    @pytest.fixture()
    def base_setup(self, project_factory, interface_type, storageclass_factory):
        """
        A setup phase for the test

        Args:
            interface_type: Interface type
            storageclass_factory: A fixture to create everything needed for a storage class
        """
        self.interface = interface_type
        self.sc_obj = storageclass_factory(self.interface)

        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

        if self.interface == constants.CEPHFILESYSTEM:
            self.sc = "CephFS"
        if self.interface == constants.CEPHBLOCKPOOL:
            self.sc = "RBD"

    @pytest.mark.parametrize(
        argnames=["interface_type", "bulk_size"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 120],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 240],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 120],
                marks=[pytest.mark.performance],
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 240],
                marks=[pytest.mark.performance],
            ),
        ],
    )
    @pytest.mark.usefixtures(base_setup.__name__)
    @polarion_id("OCS-1620")
    def test_bulk_pod_attach_performance(self, teardown_factory, bulk_size):

        """
        Measures pods attachment time in bulk_size bulk

        Args:
            teardown_factory: A fixture used when we want a new resource that was created during the tests
                               to be removed in the teardown phase.
            bulk_size: Size of the bulk to be tested
        Returns:

        """
        # Getting the test start time
        test_start_time = PASTest.get_time()

        log.info(f"Start creating bulk of new {bulk_size} PVCs")

        pvc_objs, _ = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj.name,
            namespace=self.namespace,
            number_of_pvc=bulk_size,
            size=self.pvc_size,
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

        start_time = helpers.get_provision_time(
            self.interface, pvc_objs, status="start"
        )
        end_time = helpers.get_provision_time(self.interface, pvc_objs, status="end")
        total_time = (end_time - start_time).total_seconds()
        log.info(
            f"{self.interface}: Bulk of {bulk_size} PVCs creation time is {total_time} seconds."
        )

        pvc_names_list = []
        for pvc_obj in pvc_objs:
            pvc_names_list.append(pvc_obj.name)

        log.info(f"{self.interface} : Before pod attach")
        bulk_start_time = time.time()
        pod_data_list = list()
        pod_data_list.extend(
            scale_lib.attach_multiple_pvc_to_pod_dict(
                pvc_list=pvc_names_list,
                namespace=self.namespace,
                pvcs_per_pod=1,
            )
        )

        lcl = locals()
        tmp_path = pathlib.Path(ocsci_log_path())
        obj_name = "obj1"
        # Create kube_job for pod creation
        lcl[f"pod_kube_{obj_name}"] = ObjectConfFile(
            name=f"pod_kube_{obj_name}",
            obj_dict_list=pod_data_list,
            project=defaults.ROOK_CLUSTER_NAMESPACE,
            tmp_path=tmp_path,
        )
        lcl[f"pod_kube_{obj_name}"].create(namespace=self.namespace)

        log.info("Checking that pods are running")
        # Check all the PODs reached Running state
        pod_running_list = scale_lib.check_all_pod_reached_running_state_in_kube_job(
            kube_job_obj=lcl[f"pod_kube_{obj_name}"],
            namespace=self.namespace,
            no_of_pod=len(pod_data_list),
            timeout=180,
        )
        for pod_name in pod_running_list:
            pod_obj = get_pod_obj(pod_name, self.namespace)
            teardown_factory(pod_obj)

        bulk_end_time = time.time()
        bulk_total_time = bulk_end_time - bulk_start_time
        log.info(
            f"Bulk attach time of {len(pod_running_list)} pods is {bulk_total_time} seconds"
        )

        # Collecting environment information
        self.get_env_info()

        # Initialize the results doc file.
        full_log_path = get_full_test_logs_path(cname=self)
        full_log_path += f"-{self.sc}"
        full_results = self.init_full_results(
            ResultsAnalyse(self.uuid, self.crd_data, full_log_path)
        )

        full_results.add_key("storageclass", self.sc)
        full_results.add_key("pod_bulk_attach_time", bulk_total_time)
        full_results.add_key("pvc_size", self.pvc_size)
        full_results.add_key("interface", self.interface)
        full_results.add_key("bulk_size", bulk_size)

        # Getting the test end time
        test_end_time = PASTest.get_time()

        # Add the test time to the ES report
        full_results.add_key(
            "test_time", {"start": test_start_time, "end": test_end_time}
        )

        # Write the test results into the ES server
        full_results.es_write()
        # write the ES link to the test results in the test log.
        log.info(f"The result can be found at : {full_results.results_link()}")

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
        full_results.add_key("index", full_results.new_index)
        return full_results
