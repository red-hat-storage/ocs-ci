import logging
import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources import pod as Pod
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import get_all_pods
from ocs_ci.framework.pytest_customization.marks import orange_squad
from ocs_ci.framework.testlib import E2ETest, scale
from ocs_ci.helpers.helpers import (
    default_storage_class,
    validate_pod_oomkilled,
    validate_pods_are_running_and_not_restarted,
)
from ocs_ci.utility.utils import ceph_health_check

log = logging.getLogger(__name__)


@orange_squad
@scale
@pytest.mark.parametrize(
    argnames=["interface"],
    argvalues=[
        pytest.param(
            constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2048")
        ),
        pytest.param(
            constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-2049")
        ),
    ],
)
class TestPodAreNotOomkilledWhileRunningIO(E2ETest):
    """
    A test case to validate no memory leaks found
    when heavy IOs run continuously and
    ceph, cluster health is good

    """

    @pytest.fixture()
    def base_setup(self, teardown_factory, interface, pvc_factory, pod_factory):
        """
        A setup phase for the test:
        get all the ceph pods information,
        create maxsize pvc, pod and run IO

        """
        # Setting the io_size_gb to 40% of the total PVC capacity
        ceph_pod = Pod.get_ceph_tools_pod()
        external = config.DEPLOYMENT["external_mode"]
        if external:
            ocp_obj = ocp.OCP()
            if interface == constants.CEPHBLOCKPOOL:
                resource_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD
            elif interface == constants.CEPHFILESYSTEM:
                resource_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
            cmd = f"get sc {resource_name} -o yaml"
            pool_data = ocp_obj.exec_oc_cmd(cmd)
            pool = pool_data["parameters"]["pool"]

        else:
            pool = (
                constants.DEFAULT_BLOCKPOOL
                if interface == constants.CEPHBLOCKPOOL
                else constants.DATA_POOL
            )

        ceph_replica = ceph_pod.exec_ceph_cmd(ceph_cmd=f"ceph osd pool get {pool} size")
        replica = ceph_replica["size"]
        ceph_status = ceph_pod.exec_ceph_cmd(ceph_cmd="ceph df")
        ceph_capacity = (
            int(ceph_status["stats"]["total_bytes"]) / replica / constants.GB
        )
        pvc_size_gb = int(ceph_capacity * 0.5)
        io_size_gb = int(pvc_size_gb * 0.4)
        io_size_gb = 400 if io_size_gb >= 400 else io_size_gb

        pod_objs = get_all_pods(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=["noobaa", "rook-ceph-osd-prepare", "rook-ceph-drain-canary"],
            exclude_selector=True,
        )

        # Create maxsize pvc, app pod and run ios
        self.sc = default_storage_class(interface_type=interface)

        self.pvc_obj = pvc_factory(
            interface=interface,
            storageclass=self.sc,
            size=pvc_size_gb,
        )
        self.pvc_obj.reload()
        teardown_factory(self.pvc_obj)

        self.pod_obj = pod_factory(interface=interface, pvc=self.pvc_obj)

        log.info(f"Running FIO to fill PVC size: {io_size_gb}G")
        self.pod_obj.run_io(
            "fs", size=f"{io_size_gb}G", io_direction="write", runtime=480
        )

        log.info("Waiting for IO results")
        self.pod_obj.get_fio_results()

        return pod_objs

    def test_pods_are_not_oomkilled_while_running_ios(self, base_setup):
        """
        Create maxsize pvc and run IOs continuously.
        While IOs are running make sure all pods are in running state and
        not OOMKILLED.

        """
        pod_objs = base_setup

        for pod in pod_objs:
            pod_name = pod.get().get("metadata").get("name")
            if "debug" in pod_name:
                log.info(f"Skipping {pod_name} pod from validation")
                continue
            restart_count = (
                pod.get().get("status").get("containerStatuses")[0].get("restartCount")
            )
            for item in pod.get().get("status").get("containerStatuses"):
                # Validate pod is oomkilled
                container_name = item.get("name")
                assert validate_pod_oomkilled(
                    pod_name=pod_name, container=container_name
                ), f"Pod {pod_name} OOMKILLED while running IOs"

            # Validate pod is running and not restarted
            assert validate_pods_are_running_and_not_restarted(
                pod_name=pod_name,
                pod_restart_count=restart_count,
                namespace=config.ENV_DATA["cluster_namespace"],
            ), f"Pod {pod_name} is either not running or restarted while running IOs"

        # Check ceph health is OK
        ceph_health_check()
