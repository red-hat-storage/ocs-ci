import logging
import pytest
import time

import yaml

from ocs_ci.ocs import constants, node
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    polarion_id,
    tier1,
    post_upgrade,
    skipif_ocs_version,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.resources import pvc

log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames="interface",
    argvalues=[
        pytest.param(*[constants.CEPHBLOCKPOOL]),
        pytest.param(*[constants.CEPHFILESYSTEM]),
    ],
)
@green_squad
@tier1
@post_upgrade
@skipif_ocs_version("<4.15")
class TestRwopPvc(ManageTest):
    """
    Tests ReadWriteOncePod PVC
    """

    @pytest.fixture(autouse=True)
    def setup(self, pvc_factory, interface):
        """
        Create PVC

        """
        self.pvc_obj = pvc_factory(
            interface=interface,
            access_mode=constants.ACCESS_MODE_RWOP,
            size=10,
        )

    @polarion_id("OCS-5456")
    def test_pod_with_same_priority(self, pod_factory, interface):
        """
        Test RBD Block volume mode RWOP PVC

        """
        self.node0_name = node.get_worker_nodes()[0]
        # Creating a pod
        pod_obj1 = pod_factory(pvc=self.pvc_obj, node_name=self.node0_name)

        # Verify that PVCs are reusable by creating new pods
        log.info(f"PVC obj {self.pvc_obj}")
        pod_obj2 = self.create_pod_and_validate_pending(pod_factory, interface)

        # delete the first pod
        pod_obj1.delete()
        pod_obj1.ocp.wait_for_delete(resource_name=pod_obj1.name)

        # verify that the second pod is now in the Running state
        time.sleep(60)
        self.validate_pod_status(pod_obj2, constants.STATUS_RUNNING)

        self.pvc_obj.resize_pvc(20, True)

        self.create_pod_and_validate_pending(pod_factory, interface)

    def test_pvc_clone_and_snapshot(
        self, pvc_clone_factory, snapshot_factory, pod_factory, interface
    ):
        """
        Test cloning and snapshots on PVC witr RWOP access mode
        1. Create pod and run IO
        2. Clone pvc and verify that it has RWOP access mode
        3. Make snapshot and restore pvc, verify that restored pvc has RWOP access mode

        """

        pod_obj = pod_factory(pvc=self.pvc_obj, interface=interface)
        log.info(f"{pod_obj.name} created successfully and mounted {self.pvc_obj.name}")

        # Run IO on each app pod for sometime
        log.info(f"Running FIO on {pod_obj.name}")
        pod_obj.run_io("fs", size="500M")

        clone_pvc_obj = pvc_clone_factory(
            self.pvc_obj,
            clone_name=f"{self.pvc_obj.name}-{interface.lower()}-clone",
        )
        log.info(f"Clone {clone_pvc_obj.name} created successfully")
        assert clone_pvc_obj.get_pvc_access_mode == constants.ACCESS_MODE_RWOP, (
            f"Cloned PVC has {clone_pvc_obj.get_pvc_access_mode} access mode instead "
            f"of expected {constants.ACCESS_MODE_RWOP}"
        )

        snap_name = f"{self.pvc_obj.name}-{interface.lower()}-snapshot"
        snap_obj = snapshot_factory(self.pvc_obj, snap_name)
        log.info(f"Snapshot {snap_name} successfully created")

        restore_pvc_yaml = constants.CSI_RBD_PVC_RESTORE_YAML
        if interface == constants.CEPHFILESYSTEM:
            restore_pvc_yaml = constants.CSI_CEPHFS_PVC_RESTORE_YAML

        log.info("Restoring the PVC from snapshot")
        restored_pvc_obj = pvc.create_restore_pvc(
            sc_name=self.pvc_obj.backed_sc,
            snap_name=snap_obj.name,
            namespace=self.pvc_obj.namespace,
            size=f"{self.pvc_obj.size}Gi",
            pvc_name=f"{snap_name}-restored",
            restore_pvc_yaml=restore_pvc_yaml,
            access_mode=constants.ACCESS_MODE_RWOP,
        )
        helpers.wait_for_resource_state(
            restored_pvc_obj, constants.STATUS_BOUND, timeout=600
        )
        restored_pvc_obj.reload()
        log.info("PVC was restored from the snapshot")
        assert restored_pvc_obj.get_pvc_access_mode == constants.ACCESS_MODE_RWOP, (
            f"Restored PVC has {restored_pvc_obj.get_pvc_access_mode} access mode "
            f"instead of expected {constants.ACCESS_MODE_RWOP}"
        )
        restored_pvc_obj.delete()

    def validate_pod_status(self, pod_obj, status):
        """
        Validates that the pod is in the desired status, throws error if this is not the case

        Args:
            pod_obj (obj): pod object to be validated
            status (string) the desired status

        """
        yaml_output = run_cmd(
            "oc get pod " + str(pod_obj.name) + " -o yaml", timeout=60
        )
        log.info(f"yaml_output of the pod {pod_obj.name} - {yaml_output}")

        # Validating the pod status
        results = yaml.safe_load(yaml_output)
        log.info(f"Status of the Pod : {results['status']['phase']}")
        if results["status"]["phase"] != status:
            raise UnexpectedBehaviour(
                f"Pod {pod_obj.name} using RWOP pvc {self.pvc_obj.name} is not in {status} state"
            )

    def create_pod_and_validate_pending(self, pod_factory, interface):
        """
        Creates pod and verifies that it is in the Pending state

        Returns:
            Pod object created

        """

        new_pod_obj = helpers.create_pods(
            [self.pvc_obj], pod_factory, interface, nodes=[self.node0_name]
        )[0]

        time.sleep(60)

        self.validate_pod_status(new_pod_obj, constants.STATUS_PENDING)

        return new_pod_obj

    @polarion_id("OCS-5471")
    def test_rwop_with_high_priority_pod(self, pod_factory, teardown_factory):
        """
        Test RBD Block volume access mode RWOP between priority pods
        """

        log.info("Creating high value Priority class")
        priority_class_obj = helpers.create_priority_class(priority="high", value=100)

        # creating a low priority pod
        log.info("creating a pod")
        self.low_pod_obj = pod_factory(
            pvc=self.pvc_obj,
        )

        time.sleep(60)

        # creating a high priority pod
        log.info("creating a high priority pod")
        self.high_pod_obj = pod_factory(
            pvc=self.pvc_obj,
            priorityClassName=priority_class_obj.name,
        )

        self.low_pod_obj.ocp.wait_for_delete(resource_name=self.low_pod_obj.name)

        yaml_output = yaml.dump(self.high_pod_obj.get())
        log.info(f"yaml_output of the pod {self.high_pod_obj.name} - {yaml_output}")

        # Validating the pod status
        results = yaml.safe_load(yaml_output)
        log.info(f"Status of the Pod : {results['status']['phase']}")
        if results["status"]["phase"] != "Running":
            raise UnexpectedBehaviour(
                f"Pod {self.high_pod_obj.name} using RWOP pvc {self.pvc_obj.name} is not in Running state"
            )

        self.low_pod_obj.set_deleted()

        log.info("Deleting the priority class")
        teardown_factory(priority_class_obj)

    @polarion_id("OCS-5470")
    def test_rwop_with_low_priority_pod(self, pod_factory, teardown_factory):
        """
        Test RBD Block volume access mode RWOP between priority pods
        """

        log.info("Creating higher value Priority class")
        high_priority_class_obj = helpers.create_priority_class(
            priority="high", value=100
        )

        # creating a high priority pod
        self.pod_obj = pod_factory(
            pvc=self.pvc_obj,
            priorityClassName=high_priority_class_obj.name,
        )

        log.info("Creating lower value Priority class")
        low_priority_class_obj = helpers.create_priority_class(priority="low", value=10)

        # creating a low priority pod
        self.low_pod_obj = pod_factory(
            pvc=self.pvc_obj,
            status=None,
            priorityClassName=low_priority_class_obj.name,
        )
        time.sleep(60)

        yaml_output = yaml.dump(self.low_pod_obj.get())
        log.info(f"yaml_output of the pod {self.low_pod_obj.name} - {yaml_output}")

        # Validating the pod status
        results = yaml.safe_load(yaml_output)
        log.info(f"Status of the Pod : {results['status']['phase']}")
        if results["status"]["phase"] != "Pending":
            raise UnexpectedBehaviour(
                f"Pod {self.low_pod_obj.name} using RWOP pvc {self.pvc_obj.name} is not in Pending state"
            )
        log.info("Deleting the priority classes")
        teardown_factory([high_priority_class_obj, low_priority_class_obj])

    @polarion_id("OCS-5472")
    def test_rwop_create_pod_on_different_node(self, pod_factory, interface):
        """
        Test RBD Block volume access mode by creating pods on different nodes
        """

        worker_nodes_list = node.get_worker_nodes()
        log.info(f"Creating pod on {worker_nodes_list[0]}")
        pod_factory(
            interface=interface,
            pvc=self.pvc_obj,
            node_name=worker_nodes_list[0],
        )

        log.info(f"Creating second pod on {worker_nodes_list[1]}")
        second_pod_obj = pod_factory(
            interface=interface,
            pvc=self.pvc_obj,
            status=None,
            node_name=worker_nodes_list[1],
        )

        # Validating the pod status
        yaml_output = yaml.dump(second_pod_obj.get())
        results = yaml.safe_load(yaml_output)
        log.info(f"Status of the Pod : {results['status']['phase']}")
        if results["status"]["phase"] != "Pending":
            raise UnexpectedBehaviour(
                f"Pod {self.pod_obj.name} using RWOP pvc {self.pvc_obj.name} is not in Pending state"
                f"Pod {second_pod_obj.name} using RWOP pvc {self.pvc_obj.name} is not in Pending state"
            )
