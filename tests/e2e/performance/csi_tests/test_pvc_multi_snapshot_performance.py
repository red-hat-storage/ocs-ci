"""
Test to run the maximum supportable snapshots
"""

# Builtin modules
import datetime
import logging
import tempfile
import time

# 3ed party modules
import json
import pytest
import yaml

# Local modules
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
    performance,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.ocs import constants, exceptions
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.ocs.perftests import PASTest

log = logging.getLogger(__name__)

# error message to look in a command output
ERRMSG = "Error in command"
# Time formattin in the csi-driver logs
format = "%H:%M:%S.%f"


@performance
@skipif_ocp_version("<4.6")
@skipif_ocs_version("<4.6")
class TestPvcMultiSnapshotPerformance(PASTest):
    """
    Tests to measure PVC snapshots creation performance & scale
    The test is trying to to take the maximal number of snapshot for one PVC
    """

    def setup(self):
        """
        Setting up the test environment :
            Creating namespace (project) for the test
            Creating a PVC which will used in the test
            Connect a POD to the tested PVC

        """
        log.info("Setting up test environment")

        # Getting the total Storage capacity
        try:
            self.ceph_cluster = CephCluster()
            self.ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        except Exception as err:
            err_msg = f"Failed to get Storage capacity : {err}"
            log.error(err_msg)
            raise Exception(err_msg)

        # Use 70% of the storage capacity in the test
        self.capacity_to_use = int(self.ceph_capacity * 0.7)

        # Generate a unique name for the test namespace
        self.nss_name = helpers.create_unique_resource_name("test", "namespace")

        log.info(f"Creating new namespace ({self.nss_name}) for the test")
        try:
            self.proj = helpers.create_project(project_name=self.nss_name)
        except Exception:
            log.error("Can not create new project")
            raise CommandFailed(f"{self.nss_name} was not created")

        self.snapshot = OCP(kind="volumesnapshot", namespace=self.nss_name)

        super(TestPvcMultiSnapshotPerformance, self).setup()

    def teardown(self):
        """
        Cleaning up the environment :
            Delete all snapshot
            Delete the POD
            Delete the PVC
            Switch to the default namespace
            Delete the tested namespace

        """
        log.info("Cleanup the test environment")

        try:
            vs_list = self.snapshot.get(all_namespaces=True)["items"]
        except Exception as err:
            log.error(f"Cannot get the list of snapshots : {err}")
            vs_list = []

        log.info(f"Deleting all Snapshots ({len(vs_list)})")
        log.debug(f"The list of all snapshots is : {json.dumps(vs_list, indent=3)}")
        for vs in vs_list:
            snap = vs.get("metadata").get("name")
            log.info(f"Try to delete {snap}")
            try:
                self.snapshot.delete(resource_name=snap)
            except Exception as err:
                log.error(f"Can not delete {snap} : {err}")

        # Deleting the pod which wrote data to the pvc
        try:
            log.info(f"Deleting the test POD : {self.pod_obj.name}")
            self.pod_obj.delete()
        except Exception as ex:
            log.error(f"Can not delete the test pod : {ex}")

        # Deleting the PVC which used in the test.
        try:
            log.info(f"Delete the PVC : {self.pvc_obj.name}")
            self.pvc_obj.delete()
        except Exception as ex:
            log.error(f"Can not delete the test pvc : {ex}")

        log.info(f"Deleting the test namespace : {self.nss_name}")
        switch_to_default_rook_cluster_project()
        try:
            self.proj.delete(resource_name=self.nss_name)
        except CommandFailed:
            log.error(f"Can not delete project {self.nss_name}")
            raise CommandFailed(f"{self.nss_name} was not created")
        self.proj.wait_for_delete(resource_name=self.nss_name, timeout=60, sleep=10)

        super(TestPvcMultiSnapshotPerformance, self).teardown()

    def get_csi_pod(self, namespace):
        """
        Getting pod list in specific namespace, for the provision logs

        Args:
            namespace (str): the namespace where the pod is deployed.

        Returns:
            list : list of lines from the output of the command.

        """
        results = run_oc_command(cmd="get pod", namespace=namespace)
        if ERRMSG in results:
            err_msg = "Can not get the CSI controller pod"
            log.error(err_msg)
            raise Exception(err_msg)
        return results

    def get_log_names(self):
        """
        Finding the name of snapshot logging file
        the start time is in the 'csi-snapshot-controller' pod, and
        the end time is in the provisioner pod (csi-snapshotter container)

        """
        self.log_names = {"start": [], "end": []}
        log.info("Looking for logs pod name")

        # Getting csi log name for snapshot start creation messages
        results = self.get_csi_pod(namespace="openshift-cluster-storage-operator")
        for line in results:
            if "csi-snapshot-controller" in line and "operator" not in line:
                self.log_names["start"].append(line.split()[0])
        log.info(f'The Start log pod is : {self.log_names["start"]}')

        # Getting csi log name for snapshot end creation messages
        results = self.get_csi_pod(namespace="openshift-storage")
        for line in results:
            if "prov" in line and self.fs_type in line:
                self.log_names["end"].append(line.split()[0])
        log.info(f'The end log pods is : {self.log_names["end"]}')

    def build_fio_command(self):
        """
        Building the FIO command that will be run on the pod before each snapshot

        """
        # Find the path that the PVC is mounted within the POD
        path = (
            self.pod_obj.get("spec")
            .get("spec")
            .get("containers")[0]
            .get("volumeMounts")[0]
            .get("mountPath")
        )
        self.fio_cmd = (
            "fio --name=fio-fillup --rw=write --bs=4m --direct=1 --numjobs=1"
            " --time_based=0 --runtime=36000 --ioengine=libaio --end_fsync=1"
            f" --filename={path}/{self.file_name} --size={self.file_size}"
            " --output-format=json"
        )
        log.info(f"The FIO command is : {self.fio_cmd}")

    def create_snapshot(self, snap_num):
        """
        Creating snapshot of volume, and measure the creation time

        Args:
            snap_num (int) the number of snapshot to create

        Returns:
            int: the creation time of the snapshot (in sec.)

        """
        log.info(f"Taking snapshot number {snap_num}")
        # Getting UTC time before test starting for log retrieve
        UTC_datetime = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        snap_name = f"pvc-snap-{snap_num}-"
        snap_name += self.pvc_obj.name.split("-")[-1]
        self.snap_templ["metadata"]["name"] = snap_name

        fd, tmpfile = tempfile.mkstemp(suffix=".yaml", prefix="Snap")
        log.debug(f"Going to create {tmpfile}")
        with open(tmpfile, "w") as f:
            yaml.dump(self.snap_templ, f, default_flow_style=False)

        res = run_oc_command(cmd=f"create -f {tmpfile}", namespace=self.nss_name)
        if ERRMSG in res[0]:
            err_msg = f"Failed to create snapshot : {res}"
            log.error(err_msg)
            raise Exception(err_msg)

        # wait until snapshot is ready
        timeout = 600
        sleep_time = 10

        while timeout > 0:
            res = run_oc_command(
                f"get volumesnapshot {snap_name} -o yaml", namespace=self.nss_name
            )

            if ERRMSG not in res[0]:
                res = yaml.safe_load("\n".join(res))
                log.debug(f"The command output is : {yaml.dump(res)}")
                try:
                    if res["status"]["readyToUse"]:
                        log.info(f"{snap_name} Created and ready to use")
                        snap_con_name = res["status"]["boundVolumeSnapshotContentName"]
                        break
                    else:
                        log.info(
                            f"{snap_name} is not ready yet, sleep 5 sec before re-check"
                        )
                        time.sleep(sleep_time)
                        timeout -= sleep_time
                except Exception:
                    log.info(
                        f"{snap_name} is not ready yet, sleep 5 sec before re-check"
                    )
                    time.sleep(sleep_time)
                    timeout -= sleep_time

            else:
                err_msg = f"Can not get snapshot status {res}"
                log.error(err_msg)
                raise Exception(err_msg)

        return self.get_creation_time(snap_name, snap_con_name, UTC_datetime)

    def read_logs(self, kind, namespace, start_time):
        """
        Reading the csi-driver logs, since we use different logs for the start time
        for end time (creation snapshot), we call this function twice.

        Args:
            kind (str): the kind of logs to read 'start' or 'end'
            namespace (str): in which namespace the pod exists
            start_time (time): the start time of the specific test,
               so we dont need to read the full log

        Returns:
            list : the contant of all read logs(s) - can be more then one log

        """
        logs = []
        # The pod with the logs for 'start' creation time have only one container
        container = ""
        if kind == "end":
            # The pod with the logs for 'end' creation time have more then one container
            container = "-c csi-snapshotter"
        for l in self.log_names[kind]:
            logs.append(
                run_oc_command(
                    f"logs {l} {container} --since-time={start_time}",
                    namespace=namespace,
                )
            )
        return logs

    def get_creation_time(self, snap_name, content_name, start_time):
        """
        Calculate the creation time of the snapshot.
        find the start / end time in the logs, and calculate the total time.

        Args:
            snap_name (str): the snapshot name that create
            content_name (str): the content name of the snapshot, the end time
             lodged on the content name and not on the snap name.

        Returns:
            int: creation time in seconds

        Raises:
            General exception : can not found start/end of creation time

        """

        # Start and End snapshot creation time
        st, et = (None, None)

        logs = self.read_logs("start", "openshift-cluster-storage-operator", start_time)
        for sublog in logs:
            for line in sublog:
                if snap_name in line and "Creating content for snapshot" in line:
                    st = line.split(" ")[1]
                    st = datetime.datetime.strptime(st, format)

        if st is None:
            err_msg = f"Can not find start time of {snap_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        # Getting end creation time
        logs = self.read_logs("end", "openshift-storage", start_time)
        for sublog in logs:
            for line in sublog:
                if content_name in line and "readyToUse true" in line:
                    et = line.split(" ")[1]
                    et = datetime.datetime.strptime(et, format)

        if et is None:
            err_msg = f"Can not find end time of {snap_name}"
            log.error(err_msg)
            raise Exception(err_msg)

        results = (et - st).total_seconds()
        log.debug(
            f"Start creation time is : {st}, End creation time is : {et}"
            f" and Total creation time is {results}"
        )

        return results

    def run(self):
        """
        Running the test
            for each snapshot : write data on the pod and take snapshot
        """
        results = []
        for test_num in range(1, self.num_of_snaps + 1):
            log.info(f"Starting test number {test_num}")

            # Running IO on the POD - (re)-write data on the PVC
            self.pod_obj.exec_cmd_on_pod(self.fio_cmd, out_yaml_format=False)

            # Taking Snapshot of the PVC
            ct = self.create_snapshot(test_num)
            speed = self.filesize / ct
            results.append({"Snap Num": test_num, "time": ct, "speed": speed})
            log.info(
                f"Results for snapshot number {test_num} are : "
                f"Creation time is {ct} , Creation speed {speed}"
            )

        log.debug(f"All results are : {json.dumps(results, indent=3)}")
        return results

    @pytest.mark.polarion_id("OCS-2623")
    @pytest.mark.parametrize(
        argnames=["interface_type", "snap_number"],
        argvalues=[
            pytest.param(*[constants.CEPHBLOCKPOOL, 512]),
            pytest.param(*[constants.CEPHFILESYSTEM, 100]),
        ],
    )
    def test_pvc_multiple_snapshot_performance(
        self,
        pvc_factory,
        pod_factory,
        interface_type,
        snap_number,
    ):
        """
        1. Creating PVC
           size is depend on storage capacity, but not less then 1 GiB
           it will use ~75% capacity of the Storage, Min storage capacity 1 TiB
        2. Fill the PVC with 80% of data
        3. Take a snapshot of the PVC and measure the time of creation.
        4. re-write the data on the PVC
        5. Take a snapshot of the PVC and measure the time of creation.
        6. repeat steps 4-5 the numbers of snapshot we want to take : 512
           this will be run by outside script for low memory consumption
        7. print all information.

        Raises:
            StorageNotSufficientException: in case of not enough capacity

        """

        # Number od snapshot for CephFS is 100 and for RBD is 512
        self.num_of_snaps = snap_number
        if self.dev_mode:
            self.num_of_snaps = 25
        log.info(f"Going to Create {self.num_of_snaps} {interface_type} snapshots")

        # since we do not want to use more then 65%, we add 35% to the needed
        # capacity, and minimum PVC size is 1 GiB
        self.need_capacity = int((self.num_of_snaps + 2) * 1.35)

        # Test will run only on system with enough capacity
        if self.capacity_to_use < self.need_capacity:
            err_msg = (
                f"The system have only {self.ceph_capacity} GiB, "
                f"we want to use only {self.capacity_to_use} GiB, "
                f"and we need {self.need_capacity} GiB to run the test"
            )
            log.error(err_msg)
            raise exceptions.StorageNotSufficientException(err_msg)

        # Calculating the PVC size in GiB
        self.pvc_size = int(self.capacity_to_use / (self.num_of_snaps + 2))
        if self.dev_mode:
            self.pvc_size = 5

        self.interface = interface_type

        log.info(f"Creating {self.pvc_size} GiB PVC of {interface_type}")
        self.pvc_obj = pvc_factory(
            interface=self.interface,
            size=self.pvc_size,
            status=constants.STATUS_BOUND,
            project=self.proj,
        )

        log.info("Creating A POD")
        self.pod_obj = pod_factory(
            interface=self.interface,
            pvc=self.pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=constants.PERF_POD_YAML,
        )

        # Calculating the file size as 80% of the PVC size
        self.filesize = self.pvc_obj.size * 0.80
        # Change the file size to MB for the FIO function
        self.file_size = f"{int(self.filesize * constants.GB2MB)}M"
        self.file_name = self.pod_obj.name

        log.info(
            f"Total capacity size is : {self.ceph_capacity} GiB, "
            f"Going to use {self.need_capacity} GiB, "
            f"With {self.num_of_snaps} Snapshots to {self.pvc_size} GiB PVC. "
            f"File size to be written is : {self.file_size} "
            f"with the name of {self.file_name}"
        )

        # Reading basic snapshot yaml file
        self.snap_yaml = constants.CSI_CEPHFS_SNAPSHOT_YAML
        self.sc = constants.DEFAULT_VOLUMESNAPSHOTCLASS_CEPHFS
        self.fs_type = "cephfs"
        if interface_type == constants.CEPHBLOCKPOOL:
            self.snap_yaml = constants.CSI_RBD_SNAPSHOT_YAML
            self.fs_type = "rbd"
            self.sc = constants.DEFAULT_VOLUMESNAPSHOTCLASS_RBD
        with open(self.snap_yaml, "r") as stream:
            try:
                self.snap_templ = yaml.safe_load(stream)
                self.snap_templ["spec"]["volumeSnapshotClassName"] = self.sc
                self.snap_templ["spec"]["source"][
                    "persistentVolumeClaimName"
                ] = self.pvc_obj.name
            except yaml.YAMLError as exc:
                log.error(f"Can not read template yaml file {exc}")
        log.debug(
            f"Snapshot yaml file : {self.snap_yaml} "
            f"Content of snapshot yaml file {json.dumps(self.snap_templ, indent=4)}"
        )

        self.get_log_names()
        self.build_fio_command()

        self.run()

        # TODO: push all results to elasticsearch server
