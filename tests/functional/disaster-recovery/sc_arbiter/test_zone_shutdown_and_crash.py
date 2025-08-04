import pytest
import logging
import time
import random
import concurrent.futures as futures
from datetime import datetime, timezone, timedelta

from ocs_ci.helpers.cnv_helpers import cal_md5sum_vm
from ocs_ci.helpers.stretchcluster_helper import (
    recover_from_ceph_stuck,
    check_for_logwriter_workload_pods,
    verify_vm_workload,
    verify_data_loss,
    verify_data_corruption,
)
from ocs_ci.ocs.resources.stretchcluster import StretchCluster
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.node import (
    wait_for_nodes_status,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import (
    get_debug_pods,
)
from ocs_ci.ocs.exceptions import (
    CommandFailed,
)
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    turquoise_squad,
    stretchcluster_required,
    jira,
)
from ocs_ci.utility.retry import retry


log = logging.getLogger(__name__)


@tier1
@stretchcluster_required
@turquoise_squad
class TestZoneShutdownsAndCrashes:

    zones = constants.DATA_ZONE_LABELS

    @pytest.mark.parametrize(
        argnames="iteration, immediate, delay",
        argvalues=[
            pytest.param(
                1,
                False,
                5,
                marks=[
                    pytest.mark.polarion_id("OCS-5088"),
                ],
            ),
            pytest.param(
                1,
                True,
                5,
                marks=[
                    pytest.mark.polarion_id("OCS-5064"),
                    pytest.mark.polarion_id("OCS-5850"),
                ],
            ),
        ],
        ids=[
            "Normal-Shutdown",
            "Immediate-Shutdown",
        ],
    )
    def test_zone_shutdowns(
        self,
        node_restart_teardown,
        iteration,
        immediate,
        delay,
        nodes,
        reset_conn_score,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        cnv_workload,
        setup_cnv,
    ):
        """
        This test will test the shutdown scenarios when CephFS, RBD and VM workloads
        are running.
        Steps:
            1) Run both the logwriter and logreader CephFS and RBD workloads
               CephFS workload uses RWX volume and RBD workload uses RWO volumes
            2) Create VM using standalone PVC. Create some data inside the VM instance
            3) Reset the connection scores for the mons
            4) Induce the shutdown
               In case of normal shutdown we shut-down and wait for about 15 mins
               before start of nodes whereas immediate shutdown would involve starting
               nodes immediately just after 5 mins.
            5) Make sure ceph is accessible during the crash duration
            6) Repeat the shutdown process as many times as number of iterations
            7) Check VM data integrity is maintained post netsplit. Check if New IO is possible in VM and out of VM.
            8) Make sure logreader job pods have Completed state.
               Check if there is any write or read pause. Fail only when neccessary.
            9) Delete the old logreader job and create new logreader job to verify the data corruption
            10) Make sure there is no data loss
            11) Validate the connection scores
            12) Do a complete cluster sanity and make sure there is no issue post recovery

        """

        sc_obj = StretchCluster()

        if immediate:
            sc_obj.default_shutdown_duration = 180

        # Run the logwriter cephFs and RBD workloads
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=0)

        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )

        # setup vm and write some data to the VM instance
        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(command="mkdir /test && sudo chmod -R 777 /test")
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_1.txt && sync"
        )
        md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
        log.debug(
            f"This is the file_1.txt content:\n{vm_obj.run_ssh_cmd(command='cat /test/file_1.txt')}"
        )

        start_time = None
        end_time = None

        for i in range(iteration):
            log.info(f"------ Iteration {i+1} ------")

            check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
            log.info("CephFS and RBD workloads are running successfully")

            # note the file names created
            sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

            # Fetch the nodes in zone that needs to be shutdown
            zone = random.choice(self.zones)
            nodes_to_shutdown = sc_obj.get_nodes_in_zone(zone)

            assert (
                len(nodes_to_shutdown) != 0
            ), f"There are 0 zone nodes labeled as {constants.ZONE_LABEL}={zone}!!"

            nodes.stop_nodes(nodes=nodes_to_shutdown)
            wait_for_nodes_status(
                node_names=[node.name for node in nodes_to_shutdown],
                status=constants.NODE_NOT_READY,
                timeout=300,
            )
            log.info(f"Nodes of zone {zone} are shutdown successfully")

            # note down the start_time and calculate the end_time
            if not immediate or not start_time:
                start_time = datetime.now(timezone.utc)
                end_time = start_time + timedelta(
                    minutes=sc_obj.default_shutdown_duration / 60
                )
            else:
                end_time += timedelta(minutes=sc_obj.default_shutdown_duration / 60)

            # get the nodes not in quorum
            sc_obj.non_quorum_nodes = [
                node_obj.name for node_obj in sc_obj.get_nodes_in_zone(zone)
            ]

            # check ceph accessibility while the nodes are down
            if not sc_obj.check_ceph_accessibility(
                timeout=sc_obj.default_shutdown_duration
                - int(((datetime.now(timezone.utc)) - start_time).total_seconds())
            ):
                assert recover_from_ceph_stuck(
                    sc_obj,
                ), "Something went wrong. not expected. please check rook-ceph logs"
            log.info("There is no issue with ceph access seen")
            time_now = datetime.now(timezone.utc)
            if time_now < end_time:
                time.sleep((end_time - time_now).total_seconds())

            # start the nodes
            try:
                nodes.start_nodes(nodes=nodes_to_shutdown)
            except Exception:
                log.error("Something went wrong!")

            # Validate all nodes are in READY state and up
            wait_for_nodes_status(timeout=600)

            log.info(f"Nodes of zone {zone} are started successfully")
            log.info(f"Failure started at {start_time} and ended at {end_time}")

            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_CEPHFS_LABEL, exp_num_replicas=0
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGREADER_CEPHFS_LABEL, exp_num_replicas=0
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=0
            )

            if not immediate:
                sc_obj.post_failure_checks(
                    start_time, end_time, wait_for_read_completion=False
                )
                log.info(
                    "Successfully verified with post failure checks for the workloads"
                )

            log.info(f"Waiting {delay} mins before the next iteration!")
            time.sleep(delay * 60)

        # check vm data written before the failure for integrity
        log.info("Waiting for VM SSH connectivity!")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, md5sum_before
        )

        # stop the VM
        vm_obj.stop()
        log.info("Stoped the VM successfully")

        # In-case of immediate shutdown-restart check the for failures now
        if immediate:
            sc_obj.post_failure_checks(
                start_time, end_time, wait_for_read_completion=False
            )
            log.info("Successfully verified with post failure checks for the workloads")

        # update the logwriter/reader pod details with the latest
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)

        # check for any data loss through logwriter logs
        verify_data_loss(sc_obj)

        # check for data corruption through logreader logs
        sc_obj.cephfs_logreader_job.delete()
        log.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old CephFS logreader pods are deleted")
        verify_data_corruption(sc_obj, logreader_workload_factory)

    @jira("DFBUGS-3636")
    @pytest.mark.parametrize(
        argnames="iteration, delay",
        argvalues=[
            pytest.param(
                1,
                5,
                marks=[
                    pytest.mark.polarion_id("OCS-5062"),
                    pytest.mark.polarion_id("OCS-5850"),
                ],
            ),
        ],
    )
    def test_zone_crashes(
        self,
        node_restart_teardown,
        reset_conn_score,
        iteration,
        delay,
        setup_logwriter_cephfs_workload_factory,
        setup_logwriter_rbd_workload_factory,
        logreader_workload_factory,
        nodes,
        cnv_workload,
        setup_cnv,
    ):
        """
        This test will test the crash scenarios when CephFS, RBD and VM workloads
        are running.
        Steps:
            1) Run both the logwriter and logreader CephFS and RBD workloads
               CephFS workload uses RWX volume and RBD workload uses RWO volumes
            2) Create VM using standalone PVC. Create some data inside the VM instance
            3) Reset the connection scores for the mons
            4) Crash the zone nodes
            5) Repeat the crash process as many times as number of iterations
            6) Make sure ceph is accessible during the crash duration
            7) Check VM data integrity is maintained post netsplit. Check if New IO is possible in VM and out of VM.
            8) Make sure logreader job pods have Completed state.
               Check if there is any write or read pause. Fail only when neccessary.
            9) Delete the old logreader job and create new logreader job to verify the data corruption
            10) Make sure there is no data loss
            11) Validate the connection scores
            12) Do a complete cluster sanity and make sure there is no issue post recovery

        """

        sc_obj = StretchCluster()

        # Run the logwriter cephFs and RBD workloads
        log.info("Running logwriter cephFS and RBD workloads")
        (
            sc_obj.cephfs_logwriter_dep,
            sc_obj.cephfs_logreader_job,
        ) = setup_logwriter_cephfs_workload_factory(read_duration=0)
        sc_obj.rbd_logwriter_sts = setup_logwriter_rbd_workload_factory(
            zone_aware=False
        )

        # setup vm and write some data to the VM instance
        vm_obj = cnv_workload(volume_interface=constants.VM_VOLUME_PVC)
        vm_obj.run_ssh_cmd(command="mkdir /test && sudo chmod -R 777 /test")
        vm_obj.run_ssh_cmd(
            command="< /dev/urandom tr -dc 'A-Za-z0-9' | head -c 10485760 > /test/file_1.txt && sync"
        )
        md5sum_before = cal_md5sum_vm(vm_obj, file_path="/test/file_1.txt")
        log.info(
            f"This is the file_1.txt content:\n{vm_obj.run_ssh_cmd(command='cat /test/file_1.txt')}"
        )

        for i in range(iteration):
            log.info(f"------ Iteration {i+1} ------")
            check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
            log.info("All logwriter workload pods are running successfully")

            # note the file names created
            sc_obj.get_logfile_map(label=constants.LOGWRITER_CEPHFS_LABEL)
            sc_obj.get_logfile_map(label=constants.LOGWRITER_RBD_LABEL)

            # Fetch the nodes in zone that needs to be crashed
            zone = random.choice(self.zones)
            nodes_to_shutdown = sc_obj.get_nodes_in_zone(zone)

            assert (
                len(nodes_to_shutdown) != 0
            ), f"There are 0 zone nodes labeled as {constants.ZONE_LABEL}={zone}!!"

            # crash zone nodes
            log.info(f"Crashing zone {zone}")
            thread_exec = futures.ThreadPoolExecutor(max_workers=len(nodes_to_shutdown))
            futures_obj = []
            crash_cmd = "echo c > /proc/sysrq-trigger"
            for node in nodes_to_shutdown:
                futures_obj.append(
                    thread_exec.submit(
                        OCP().exec_oc_debug_cmd, node=node.name, cmd_list=[crash_cmd]
                    )
                )
                log.info(f"Crashed {node.name}")
            start_time = datetime.now(timezone.utc)

            # get the nodes not in quorum
            sc_obj.non_quorum_nodes = [
                node_obj.name for node_obj in sc_obj.get_nodes_in_zone(zone)
            ]

            # wait for the crash tasks to complete
            log.info("Wait for the crash tasks to complete!")
            futures.wait(futures_obj)

            # delete debug pods if not deleted already
            debug_pods = get_debug_pods([node.name for node in nodes_to_shutdown])
            for pod in debug_pods:
                try:
                    pod.delete(force=True)
                except CommandFailed:
                    continue
                else:
                    log.info(f"Deleted pod {pod.name}")

            # wait for the nodes to come back to READY status
            log.info("Waiting for the nodes to come up automatically after the crash")
            wait_for_nodes_status(timeout=600)

            end_time = datetime.now(timezone.utc)
            log.info(f"Crash start time : {start_time} & Crash end time : {end_time}")

            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_CEPHFS_LABEL, exp_num_replicas=0
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGREADER_CEPHFS_LABEL, exp_num_replicas=0
            )
            sc_obj.get_logwriter_reader_pods(
                label=constants.LOGWRITER_RBD_LABEL, exp_num_replicas=0
            )

            # check the ceph access again after the nodes are completely up
            sc_obj.post_failure_checks(
                start_time, end_time, wait_for_read_completion=False
            )

            log.info(f"Waiting {delay} mins before the next iteration!")
            time.sleep(delay * 60)

        # check vm data written before the failure for integrity
        log.info("Waiting for VM SSH connectivity!")
        retry(CommandFailed, tries=5, delay=10)(vm_obj.wait_for_ssh_connectivity)()
        retry(CommandFailed, tries=5, delay=10)(verify_vm_workload)(
            vm_obj, md5sum_before
        )

        # stop the VM
        vm_obj.stop()
        log.info("Stoped the VM successfully")

        # check for any data loss
        check_for_logwriter_workload_pods(sc_obj, nodes=nodes)
        verify_data_loss(sc_obj)

        # check for data corruption
        sc_obj.cephfs_logreader_job.delete()
        log.info(sc_obj.cephfs_logreader_pods)
        for pod in sc_obj.cephfs_logreader_pods:
            pod.wait_for_pod_delete(timeout=120)
        log.info("All old CephFS logreader pods are deleted")
        verify_data_corruption(sc_obj, logreader_workload_factory)
