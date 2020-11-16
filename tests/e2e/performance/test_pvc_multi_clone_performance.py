import datetime
import logging
import os
import subprocess
import tempfile
import time

import yaml

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    E2ETest,
    performance,
)
from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.utils import ocsci_log_path

log = logging.getLogger(__name__)


@performance
@skipif_ocp_version("<4.6")
@skipif_ocs_version("<4.6")
class TestPvcMultiClonePerformance(E2ETest):
    """
    Tests to measure PVC clones creation performance ( time and speed)
    The test is supposed to create the maximum number of clones for one PVC
    """

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
           PVS size is calculated in the test and depends on the storage capacity, but not less then 1 GiB
           it will use ~75% capacity of the Storage, Min storage capacity 1 TiB
        2. Fill the PVC with 70% of data
        3. Take a clone of the PVC and measure time and speed of creation by reading start creation and end creation
            times from relevant logs
        4. Repeat the previous step number of times (maximal num_of_clones is 512)
        5. Rrint all measured statistics for all the clones.

        Raises:
            StorageNotSufficientException: in case of not enough capacity on the cluster

        """
        num_of_clones = 512
        if interface_iterate == constants.CEPHBLOCKPOOL:
            num_of_clones = 512  # bz_1896831

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
        self.get_log_names()
        self.run_fio_on_pod(file_size)

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

        output = self.run_oc_command(
            cmd=f"get pod {self.pod_obj.name} -o yaml", namespace=self.params["nspace"]
        )

        results = yaml.safe_load("\n".join(output))
        self.params["path"] = results["spec"]["containers"][0]["volumeMounts"][0][
            "mountPath"
        ]
        log.info(f"path - {self.params['path']}")

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

    def get_log_names(self):
        """
        Finds names for log files pods in which logs for clone creation are located
        For CephFS: 2 pods that start with "csi-cephfsplugin-provisioner" prefix
        For RBD: 2 pods that start with "csi-rbdplugin-provisioner" prefix

        """
        self.params["logs"] = []

        pods = self.run_oc_command(cmd="get pod", namespace="openshift-storage")
        if self.params["ERRMSG"] in pods:
            raise Exception("Can not get csi controller pod")

        provisioning_name = "csi-cephfsplugin-provisioner"
        if self.interface == constants.CEPHBLOCKPOOL:
            provisioning_name = "csi-rbdplugin-provisioner"

        for line in pods:
            if provisioning_name in line:
                self.params["logs"].append(line.split()[0])
        log.info(f'The logs pods are : {self.params["logs"]}')

    def run_fio_on_pod(self, file_size):
        """
        Args:
        file_size (str): Size of file to write in MB, e.g. 200M or 50000M

        """
        file_name = self.pod_obj.name
        log.info(f"Starting IO on the POD {self.pod_obj.name}")
        # Going to run only write IO to fill the PVC for the before creating a clone
        self.pod_obj.fillup_fs(size=file_size, fio_filename=file_name)

        # Wait for fio to finish - non default timeout is needed for big pvc/clones
        fio_result = self.pod_obj.get_fio_results(timeout=18000)
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {self.pod_obj.name}. " f"FIO result: {fio_result}."
        )
        log.info("IO on the PVC Finished")

        # Verify presence of the file on pvc
        file_path = pod.get_file_path(self.pod_obj, file_name)
        log.info(f"Actual file path on the pod is {file_path}.")
        assert pod.check_file_existence(
            self.pod_obj, file_path
        ), f"File {file_name} does not exist"
        log.info(f"File {file_name} exists in {self.pod_obj.name}.")

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

        fd, tmpfile = tempfile.mkstemp(suffix=".yaml", prefix="Clone")
        log.info(f"Going to create {tmpfile}")
        with open(tmpfile, "w") as f:
            yaml.dump(clone_yaml, f, default_flow_style=False)
        start_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        log.info(f"Clone yaml file is {clone_yaml}")
        res = self.run_oc_command(f"create -f {tmpfile}", self.params["nspace"])
        if self.params["ERRMSG"] in res[0]:
            raise Exception(f"Can not create clone : {res}")
        # wait until clone is ready
        timeout = 600
        while timeout > 0:
            res = self.run_oc_command(
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

        create_time = self.measure_clone_creation_time(clone_name, start_time)

        log.info(f"Creation time of clone {clone_name} is {create_time} secs.")

        return create_time

    def run_command(self, cmd):
        """
        Running command on the OS and return the STDOUT & STDERR outputs
        in case of argument is not string or list, return error message

        Args:
            cmd (str/list): the command to execute

        Returns:
            list : all STDOUT / STDERR output as list of lines

        """
        if isinstance(cmd, str):
            command = cmd.split()
        elif isinstance(cmd, list):
            command = cmd
        else:
            return self.params["ERRMSG"]

        log.info(f"Going to run {cmd}")
        cp = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            timeout=600,
        )
        output = cp.stdout.decode()
        err = cp.stderr.decode()
        # exit code is not zero
        if cp.returncode:
            log.error(f"Command finished with non zero ({cp.returncode}) {err}")
            output += f"{self.params['ERRMSG']} {err}"

        output = output.split("\n")  # convert output to list
        output.pop()  # remove last empty element from the list
        return output

    def run_oc_command(self, cmd, namespace):
        """
        Running an 'oc' command

        Args:
            cmd (str): the command to run
            namespace (str): the namespace where to run the command

        Returns:
            list : the results of the command as list of lines

        """

        cluster_dir_kubeconfig = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN.get("kubeconfig_location")
        )
        if os.getenv("KUBECONFIG"):
            kubeconfig = os.getenv("KUBECONFIG")
        elif os.path.exists(cluster_dir_kubeconfig):
            kubeconfig = cluster_dir_kubeconfig
        else:
            kubeconfig = None

        command = f"oc --kubeconfig {kubeconfig} -n {namespace} {cmd}"
        return self.run_command(command)

    def measure_clone_creation_time(self, clone_name, start_time):
        """
        Args:
            clone_name: Name of the clone for which we measure the time
            start_time: Time from which and on to search the relevant logs

        Returns:
            creation time for each clone

        """
        logs = []
        for l in self.params["logs"]:
            logs.append(
                self.run_oc_command(
                    f"logs {l} -c csi-provisioner --since-time={start_time}",
                    "openshift-storage",
                )
            )

        format = "%H:%M:%S.%f"

        st = None
        et = None
        for sublog in logs:
            for line in sublog:
                if (
                    st is None
                    and "provision" in line
                    and clone_name in line
                    and "started" in line
                ):
                    st = line.split(" ")[1]
                    st = datetime.datetime.strptime(st, format)
                elif (
                    et is None
                    and "provision" in line
                    and clone_name in line
                    and "succeeded" in line
                ):
                    et = line.split(" ")[1]
                    et = datetime.datetime.strptime(et, format)
                if st is not None and et is not None:
                    break
            if st is not None and et is not None:
                break

        if st is None:
            log.error(f"Can not find start time of {clone_name}")
            raise Exception(f"Can not find start time of {clone_name}")

        if et is None:
            log.error(f"Can not find end time of {clone_name}")
            raise Exception(f"Can not find end time of {clone_name}")

        return (et - st).total_seconds()
