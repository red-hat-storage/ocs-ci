import datetime
import logging
import os
import tempfile
import time
from uuid import uuid4
from ocs_ci.framework import config
import statistics

import yaml
import pytest

from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.perfresult import ResultsAnalyse
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    performance,
)
from ocs_ci.helpers.helpers import get_full_test_logs_path
from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.helpers import performance_lib

log = logging.getLogger(__name__)


@performance
@skipif_ocp_version("<4.6")
@skipif_ocs_version("<4.6")
class TestPvcMultiClonePerformance(PASTest):
    def setup(self):
        """
        Setting up test parameters
        """
        logging.info("Starting the test setup")
        super(TestPvcMultiClonePerformance, self).setup()
        self.benchmark_name = "pvc_multi_clone_performance"
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
    Tests to measure PVC clones creation performance ( time and speed)
    The test is supposed to create the maximum number of clones for one PVC
    """

    @pytest.mark.polarion_id("OCS-2622")
    def test_pvc_multiple_clone_performance(
        self,
        interface_iterate,
        teardown_factory,
        storageclass_factory,
        pvc_factory,
        pod_factory,
    ):
        """
        1. Creating PVC
           PVC size is calculated in the test and depends on the storage capacity, but not less then 1 GiB
           it will use ~75% capacity of the Storage, Min storage capacity 1 TiB
        2. Fill the PVC with 70% of data
        3. Take a clone of the PVC and measure time and speed of creation by reading start creation and end creation
            times from relevant logs
        4. Repeat the previous step number of times (maximal num_of_clones is 512)
        5. Print all measured statistics for all the clones.

        Raises:
            StorageNotSufficientException: in case of not enough capacity on the cluster

        """
        num_of_clones = 512

        # Getting the total Storage capacity
        ceph_cluster = CephCluster()
        ceph_capacity = int(ceph_cluster.get_ceph_capacity())

        # Use 70% of the storage capacity in the test
        capacity_to_use = int(ceph_capacity * 0.7)

        # since we do not want to use more then 65%, we add 35% to the needed
        # capacity, and minimum PVC size is 1 GiB
        need_capacity = int((num_of_clones + 2) * 1.35)
        # Test will run only on system with enough capacity
        if capacity_to_use < need_capacity:
            err_msg = (
                f"The system have only {ceph_capacity} GiB, "
                f"we want to use only {capacity_to_use} GiB, "
                f"and we need {need_capacity} GiB to run the test"
            )
            log.error(err_msg)
            raise exceptions.StorageNotSufficientException(err_msg)

        # Calculating the PVC size in GiB
        pvc_size = int(capacity_to_use / (num_of_clones + 2))

        self.interface = interface_iterate
        self.sc_obj = storageclass_factory(self.interface)

        if self.interface == constants.CEPHFILESYSTEM:
            sc = "CephFS"
        if self.interface == constants.CEPHBLOCKPOOL:
            sc = "RBD"

        self.full_log_path = get_full_test_logs_path(cname=self)
        self.full_log_path += f"-{sc}"

        self.pvc_obj = pvc_factory(
            interface=self.interface, size=pvc_size, status=constants.STATUS_BOUND
        )

        self.pod_obj = pod_factory(
            interface=self.interface, pvc=self.pvc_obj, status=constants.STATUS_RUNNING
        )

        # Calculating the file size as 70% of the PVC size
        filesize = self.pvc_obj.size * 0.70
        # Change the file size to MB for the FIO function
        file_size = f"{int(filesize * constants.GB2MB)}M"
        file_name = self.pod_obj.name

        log.info(
            f"Total capacity size is : {ceph_capacity} GiB, "
            f"Going to use {need_capacity} GiB, "
            f"With {num_of_clones} clones to {pvc_size} GiB PVC. "
            f"File size to be written is : {file_size} "
            f"with the name of {file_name}"
        )
        self.params = {}
        self.params["clonenum"] = f"{num_of_clones}"
        self.params["filesize"] = file_size
        self.params["ERRMSG"] = "Error in command"

        clone_yaml = self.build_params()
        performance_lib.write_fio_on_pod(self.pod_obj, file_size)

        # Running the test
        results = []
        for test_num in range(1, int(self.params["clonenum"]) + 1):
            log.info(f"Starting test number {test_num}")
            ct = self.create_clone(test_num, clone_yaml)
            speed = self.params["datasize"] / ct
            results.append({"Clone Num": test_num, "time": ct, "speed": speed})
            log.info(
                f"Results for clone number {test_num} are : "
                f"Creation time is {ct} secs, Creation speed {speed} MB/sec"
            )

        for r in results:
            log.info(
                f"Clone number {r['Clone Num']} creation time is {r['time']} secs."
            )
            log.info(
                f"Clone number {r['Clone Num']} creation speed is {r['speed']} MB/sec."
            )

        creation_time_list = [r["time"] for r in results]
        average_creation_time = statistics.mean(creation_time_list)
        log.info(f"Average creation time is  {average_creation_time} secs.")

        creation_speed_list = [r["speed"] for r in results]
        average_creation_speed = statistics.mean(creation_speed_list)
        log.info(f"Average creation speed is  {average_creation_time} MB/sec.")

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
                "pvc_multiple_clone_measurement",
            )
        )

        full_results.add_key("interface", self.interface)
        full_results.add_key("clones_num", num_of_clones)
        full_results.add_key("clone_size", pvc_size)
        full_results.add_key("multi_clone_creation_time", creation_time_list)
        full_results.add_key("multi_clone_creation_time_average", average_creation_time)
        full_results.add_key("multi_clone_creation_speed", creation_speed_list)
        full_results.add_key(
            "multi_clone_creation_speed_average", average_creation_speed
        )

        # Write the test results into the ES server
        if full_results.es_write():
            res_link = full_results.results_link()
            log.info(f"The Result can be found at : {res_link}")

            # Create text file with results of all subtest (4 - according to the parameters)
            self.write_result_to_file(res_link)

    def build_params(self):
        log.info("Start building params")

        self.params["nspace"] = self.pvc_obj.namespace
        self.params["pvcname"] = self.pvc_obj.name

        log_file_name = os.path.basename(__file__).replace(".py", ".log")

        full_log = f"{ocsci_log_path()}/{log_file_name}"
        logging.basicConfig(
            filename=full_log, level=logging.INFO, format=constants.LOG_FORMAT
        )

        self.params["datasize"] = int(self.params["filesize"].replace("M", ""))

        self.params["clone_yaml"] = constants.CSI_CEPHFS_PVC_CLONE_YAML
        if self.interface == constants.CEPHBLOCKPOOL:
            self.params["clone_yaml"] = constants.CSI_RBD_PVC_CLONE_YAML

        output = performance_lib.run_oc_command(
            cmd=f"get pod {self.pod_obj.name} -o yaml", namespace=self.params["nspace"]
        )

        results = yaml.safe_load("\n".join(output))
        self.params["path"] = results["spec"]["containers"][0]["volumeMounts"][0][
            "mountPath"
        ]
        log.info(f"path - {self.params['path']}")

        fd, tmpfile = tempfile.mkstemp(suffix=".yaml", prefix="Clone")
        self.params["tmpfile"] = tmpfile

        # reading template of clone yaml file
        with open(self.params["clone_yaml"], "r") as stream:
            try:
                clone_yaml = yaml.safe_load(stream)
                clone_yaml["spec"]["storageClassName"] = self.pvc_obj.backed_sc
                clone_yaml["spec"]["dataSource"]["name"] = self.params["pvcname"]
                clone_yaml["spec"]["resources"]["requests"]["storage"] = (
                    str(self.pvc_obj.size) + "Gi"
                )
            except yaml.YAMLError as exc:
                log.error(f"Can not read template yaml file {exc}")
        log.info(
            f'Clone yaml file : {self.params["clone_yaml"]} '
            f"Content of clone yaml file {clone_yaml}"
        )

        return clone_yaml

    def create_clone(self, clone_num, clone_yaml):
        """
        Creating clone for pvc, measure the creation time

        Args:
            clone_num (int) the number of clones to create
            clone_yaml : a template of clone yaml

        Returns:
            int: the creation time of the clone (in secs.)

        """
        log.info(f"Creating clone number {clone_num} for interface {self.interface}")

        clone_name = f"pvc-clone-{clone_num}-"
        clone_name += self.params["pvcname"].split("-")[-1]
        clone_yaml["metadata"]["name"] = clone_name
        tmpfile = self.params["tmpfile"]

        log.info(f"Going to create {tmpfile}")
        with open(tmpfile, "w") as f:
            yaml.dump(clone_yaml, f, default_flow_style=False)
        start_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        log.info(f"Clone yaml file is {clone_yaml}")
        res = performance_lib.run_oc_command(
            f"create -f {tmpfile}", self.params["nspace"]
        )
        if self.params["ERRMSG"] in res[0]:
            raise Exception(f"Can not create clone : {res}")
        # wait until clone is ready
        self.wait_for_clone_creation(clone_name)
        create_time = performance_lib.measure_pvc_creation_time(
            self.interface, clone_name, start_time
        )

        log.info(f"Creation time of clone {clone_name} is {create_time} secs.")

        return create_time

    def wait_for_clone_creation(self, clone_name, timeout=600):
        """
        Waits for creation of clone for defined period of time
        Raises exception and fails the test if clone was not created during that time
        Args:
            clone_name: name of the clone being created
            timeout: optional argument, time period in seconds to wait for creation

        """
        while timeout > 0:
            res = performance_lib.run_oc_command(
                f"get pvc {clone_name} -o yaml", self.params["nspace"]
            )
            if self.params["ERRMSG"] not in res[0]:
                res = yaml.safe_load("\n".join(res))
                log.info(f"Result yaml is {res}")
                if res["status"]["phase"] == "Bound":
                    log.info(f"{clone_name} Created and ready to use")
                    break
                else:
                    log.info(
                        f"{clone_name} is not ready yet, sleep 5 sec before re-check"
                    )
                    time.sleep(5)
                    timeout -= 5
            else:
                raise Exception(f"Can not get clone status {res}")
        if timeout <= 0:
            raise Exception(
                f"Clone {clone_name}  for {self.interface} interface was not created for 600 seconds"
            )

    def test_multi_clone_performance_results(self):
        """
        This is not a test - it is only check that previous tests ran and finished as expected
        and reporting the full results (links in the ES) of previous tests (2)
        """

        self.number_of_tests = 2
        self.results_path = get_full_test_logs_path(
            cname=self, fname="test_pvc_multiple_clone_performance"
        )
        self.results_file = os.path.join(self.results_path, "all_results.txt")
        log.info(
            f"Check results for test_pvc_multiple_clone_performance in : {self.results_file}"
        )
        self.check_tests_results()
        self.push_to_dashboard(test_name="PVC Multi Clone Performance")
