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
    skipif_ocs_version,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.utils import run_cmd

log = logging.getLogger(__name__)


@green_squad
@skipif_ocs_version("<4.15")
class TestRwopPvc(ManageTest):
    """
    Tests ReadWriteOncePod RBD PVC
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, pvc_factory, pod_factory):
        """
        Create PVC and pods

        """
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_mode=constants.ACCESS_MODE_RWOP,
            size=10,
        )

    @polarion_id("OCS-5456")
    @tier1
    def test_pod_with_same_priority(self, pod_factory):
        """
        Test RBD Block volume mode RWOP PVC

        """
        # Creating a pod
        self.pod_obj = pod_factory(
            pvc=self.pvc_obj,
        )

        # Verify that PVCs are reusable by creating new pods
        log.info(f"PVC obj {self.pvc_obj}")
        new_pod_obj = helpers.create_pods(
            [self.pvc_obj],
            pod_factory,
            constants.CEPHBLOCKPOOL,
        )

        # sleep for 60s
        time.sleep(60)

        yaml_output = run_cmd(
            "oc get pod " + str(new_pod_obj[0].name) + " -o yaml", timeout=60
        )
        log.info(f"yaml_output of the pod {new_pod_obj[0].name} - {yaml_output}")

        # Validating the pod status
        results = yaml.safe_load(yaml_output)
        log.info(f"Status of the Pod : {results['status']['phase']}")
        if results["status"]["phase"] != "Pending":
            raise UnexpectedBehaviour(
                f"Pod {new_pod_obj[0].name} using RWOP pvc {self.pvc_obj.name} is not in Pending state"
            )

    @polarion_id("OCS-5471")
    @tier1
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
    @tier1
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
    @tier1
    def test_rwop_create_pod_on_different_node(self, pod_factory):
        """
        Test RBD Block volume access mode by creating pods on different nodes
        """

        worker_nodes_list = node.get_worker_nodes()
        log.info(f"Creating pod on {worker_nodes_list[0]}")
        pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=self.pvc_obj,
            node_name=worker_nodes_list[0],
        )

        log.info(f"Creating second pod on {worker_nodes_list[1]}")
        second_pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
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
