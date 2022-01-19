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
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP, switch_to_default_rook_cluster_project
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.utility import templating
from ocs_ci.ocs.resources import ocs
from ocs_ci.framework.testlib import ignore_leftovers

log = logging.getLogger(__name__)

# Error message to look in a command output
ERRMSG = "Error in command"
# Time formatting in the csi-driver logs
time_format = "%H:%M:%S.%f"


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
            Calculating the amount of storage which available for the test
            Creating namespace (project) for the test

        """
        log.info("Setting up the test environment")

        super(TestPvcMultiSnapshotPerformance, self).setup()

        # Getting the total Storage capacity
        try:
            self.ceph_capacity = int(self.ceph_cluster.get_ceph_capacity())
        except Exception as err:
            err_msg = f"Failed to get Storage capacity : {err}"
            log.error(err_msg)
            raise Exception(err_msg)

        # Use 70% of the storage capacity in the test
        self.capacity_to_use = int(self.ceph_capacity * 0.7)

        # Creating new namespace for the test
        self.nss_name = "pas-test-namespace"
        log.info(f"Creating new namespace ({self.nss_name}) for the test")
        try:
            self.proj = helpers.create_project(project_name=self.nss_name)
        except CommandFailed as ex:
            if str(ex).find("(AlreadyExists)"):
                log.warning("The Namespace is Already Exists !")
            log.error("Can not create new project")
            raise CommandFailed(f"{self.nss_name} was not created")

        # Initialize a general Snapshot object to use in the test
        self.snapshot = OCP(kind="volumesnapshot", namespace=self.nss_name)

    def teardown(self):
        """
        Cleaning up the environment :
            Delete all snapshot
            Delete the POD
            Delete the PVC and the PV
            Delete the StorageClass
            Delete the VolumeSnapshotClass
            Delete the data pool
            Switch to the default namespace
            Delete the tested namespace

        """
        log.info("Cleanup the test environment")

        # Getting the name of the PCV's backed PV
        try:
            pv = self.pvc_obj.get("spec")["spec"]["volumeName"]
        except KeyError:
            log.error(
                f"Can not found key in the PVC object {json.dumps(self.pvc_obj.get('spec').get('spec'), indent=3)}"
            )

        # Getting the list of all snapshots
        try:
            snapshot_list = self.snapshot.get(all_namespaces=True)["items"]
        except Exception as err:
            log.error(f"Cannot get the list of snapshots : {err}")
            snapshot_list = []

        # Deleting al snapshots from the cluster
        log.info(f"Trying to delete all ({len(snapshot_list)}) Snapshots")
        log.debug(
            f"The list of all snapshots is : {json.dumps(snapshot_list, indent=3)}"
        )
        for vs in snapshot_list:
            snap_name = vs["metadata"]["name"]
            log.info(f"Try to delete {snap_name}")
            try:
                self.snapshot.delete(resource_name=snap_name)
            except Exception as err:
                log.error(f"Can not delete {snap_name} : {err}")

        # Deleting the pod which wrote data to the pvc
        log.info(f"Deleting the test POD : {self.pod_obj.name}")
        try:
            self.pod_obj.delete()
            log.info("Wait until the pod is deleted.")
            self.pod_obj.ocp.wait_for_delete(resource_name=self.pod_obj.name)
        except Exception as ex:
            log.error(f"Can not delete the test pod : {ex}")

        # Deleting the PVC which used in the test.
        log.info(f"Delete the PVC : {self.pvc_obj.name}")
        try:
            self.pvc_obj.delete()
            log.info("Wait until the pvc is deleted.")
            self.pvc_obj.ocp.wait_for_delete(resource_name=self.pvc_obj.name)
        except Exception as ex:
            log.error(f"Can not delete the test pvc : {ex}")

        # Delete the backend PV of the PVC
        log.info(f"Try to delete the backend PV : {pv}")
        try:
            run_oc_command(f"delete pv {pv}")
        except Exception as ex:
            err_msg = f"can not delete PV {pv} - [{ex}]"
            log.error(err_msg)

        # Deleting the StorageClass used in the test
        log.info(f"Deleting the test StorageClass : {self.sc_obj.name}")
        try:
            self.sc_obj.delete()
            log.info("Wait until the SC is deleted.")
            self.sc_obj.ocp.wait_for_delete(resource_name=self.sc_obj.name)
        except Exception as ex:
            log.error(f"Can not delete the test sc : {ex}")

        # Deleting the VolumeSnapshotClass used in the test
        log.info(f"Deleting the test Snapshot Class : {self.snap_class.name}")
        try:
            self.snap_class.delete()
            log.info("Wait until the VSC is deleted.")
            self.snap_class.ocp.wait_for_delete(resource_name=self.snap_class.name)
        except Exception as ex:
            log.error(f"Can not delete the test vsc : {ex}")

        # Deleting the Data pool
        log.info(f"Deleting the test storage pool : {self.sc_name}")
        self.delete_ceph_pool(self.sc_name)
        # Verify deletion by checking the backend CEPH pools using the toolbox
        results = self.ceph_cluster.toolbox.exec_cmd_on_pod("ceph osd pool ls")
        log.debug(f"Existing pools are : {results}")
        if self.sc_name in results.split():
            log.warning("The pool did not deleted by CSI, forcing delete it manually")
            self.ceph_cluster.toolbox.exec_cmd_on_pod(
                f"ceph osd pool delete {self.sc_name} {self.sc_name} "
                "--yes-i-really-really-mean-it"
            )
        else:
            log.info(f"The pool {self.sc_name} was deleted successfully")

        # Deleting the namespace used by the test
        log.info(f"Deleting the test namespace : {self.nss_name}")
        switch_to_default_rook_cluster_project()
        try:
            self.proj.delete(resource_name=self.nss_name)
            self.proj.wait_for_delete(resource_name=self.nss_name, timeout=60, sleep=10)
        except CommandFailed:
            log.error(f"Can not delete project {self.nss_name}")
            raise CommandFailed(f"{self.nss_name} was not created")

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

        # Getting csi log name for snapshot end creation messages
        results = self.get_csi_pod(namespace="openshift-storage")
        for line in results:
            if "prov" in line and self.fs_type in line:
                self.log_names["end"].append(line.split()[0])

        log.info(
            f"The CSI logs for the test are : {json.dumps(self.log_names, indent=4)}"
        )

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

    def create_snapshotclass(self, interface):
        """
        Creates own VolumeSnapshotClass

        Args:
            interface (str): Interface type used

        Returns:
            ocs_obj (obj): SnapshotClass obj instances

        """
        if interface == constants.CEPHFILESYSTEM:
            snapclass_name = "pas-test-cephfs-snapshot-class"
        else:
            snapclass_name = "pas-test-rbd-snapshot-class"

        yaml_files = {
            constants.CEPHBLOCKPOOL: constants.CSI_RBD_SNAPSHOTCLASS_YAML,
            constants.CEPHFILESYSTEM: constants.CSI_CEPHFS_SNAPSHOTCLASS_YAML,
        }
        snapshotclass_data = templating.load_yaml(yaml_files[interface])

        snapshotclass_data["metadata"]["name"] = snapclass_name
        ocs_obj = ocs.OCS(**snapshotclass_data)
        log.info(f"Creating new snapshot class : {snapclass_name}")
        try:
            created_snapclass = ocs_obj.create(do_reload=True)
            log.debug(created_snapclass)
        except Exception as ex:
            err_msg = f"Failed to create new snapshot class : {snapclass_name} [{ex}]"
            log.error(err_msg)
            raise Exception(err_msg)
        return ocs_obj

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
        self.snap_templ["spec"]["volumeSnapshotClassName"] = self.snap_class.name

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
        timeout = 720
        sleep_time = 10
        snap_con_name = None
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
                            f"{snap_name} is not ready yet, sleep {sleep_time} sec before re-check"
                        )
                        time.sleep(sleep_time)
                        timeout -= sleep_time
                except Exception:
                    log.info(
                        f"{snap_name} is not ready yet, sleep {sleep_time} sec before re-check"
                    )
                    time.sleep(sleep_time)
                    timeout -= sleep_time

            else:
                err_msg = f"Can not get snapshot status {res}"
                log.error(err_msg)
                raise Exception(err_msg)
        if snap_con_name:
            return self.get_creation_time(snap_name, snap_con_name, UTC_datetime)
        else:
            err_msg = "Snapshot did not created on time"
            log.error(err_msg)
            raise TimeoutError(err_msg)

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
            start_time (time): time of test starting so, retrieving log will be short as possible

        Returns:
            int: creation time in seconds

        Raises:
            General exception : can not found start/end of creation time

        """

        # Start and End snapshot creation time
        times = {"start": None, "end": None}
        logs_info = {
            "start": {
                "ns": "openshift-cluster-storage-operator",
                "log_line": "Creating content for snapshot",
            },
            "end": {"ns": "openshift-storage", "log_line": "readyToUse true"},
        }

        for op in ["start", "end"]:
            logs = self.read_logs(op, logs_info[op]["ns"], start_time)
            for sublog in logs:
                for line in sublog:
                    if (snap_name in line or content_name in line) and logs_info[op][
                        "log_line"
                    ] in line:
                        times[op] = line.split(" ")[1]
                        times[op] = datetime.datetime.strptime(times[op], time_format)
            if times[op] is None:
                err_msg = f"Can not find {op} time of {snap_name}"
                log.error(err_msg)
                raise Exception(err_msg)

        results = (times["end"] - times["start"]).total_seconds()
        log.debug(
            f"Start creation time is : {times['start']}, End creation time is : {times['end']}"
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

    @ignore_leftovers
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
        secret_factory,
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

        self.num_of_snaps = snap_number
        if self.dev_mode:
            self.num_of_snaps = 2

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
        self.sc_name = "pas-testing-rbd"
        pool_name = self.sc_name
        if self.interface == constants.CEPHFILESYSTEM:
            self.sc_name = "pas-testing-cephfs"
            pool_name = f"{self.sc_name}-data0"

        # Creating new storage pool
        self.create_new_pool(self.sc_name)

        # Creating new StorageClass (pool) for the test.
        secret = secret_factory(interface=self.interface)
        self.sc_obj = helpers.create_storage_class(
            interface_type=self.interface,
            interface_name=pool_name,
            secret_name=secret.name,
            sc_name=self.sc_name,
            fs_name=self.sc_name,
        )
        log.info(f"The new SC is : {self.sc_obj.name}")
        log.debug(f"All Sc data is {json.dumps(self.sc_obj.data, indent=3)}")

        # Create new VolumeSnapshotClass
        self.snap_class = self.create_snapshotclass(self.interface)

        # Create new PVC
        log.info(f"Creating {self.pvc_size} GiB PVC of {interface_type}")
        self.pvc_obj = pvc_factory(
            interface=self.interface,
            storageclass=self.sc_obj,
            size=self.pvc_size,
            status=constants.STATUS_BOUND,
            project=self.proj,
        )

        # Create POD which will attache to the new PVC
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
