import logging
import pytest

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier1
from tests.helpers import wait_for_resource_state

log = logging.getLogger(__name__)


@tier1
@pytest.mark.skipif(
    config.ENV_DATA['platform'].lower() == 'ibm_cloud',
    reason=(
        "Skipping tests on IBM Cloud due to bug 1871315 "
        "https://bugzilla.redhat.com/show_bug.cgi?id=1871315"
    )
)
@pytest.mark.parametrize(
    argnames='interface',
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL],
            marks=[
                pytest.mark.polarion_id("OCS-1177"),
                pytest.mark.bugzilla("1772990")
            ]
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM],
            marks=[
                pytest.mark.polarion_id("OCS-1179"),
                pytest.mark.bugzilla("1772990")
            ]
        )
    ]
)
class TestRwoUsingMultiplePods(ManageTest):
    """
    This test class consists of tests to verify RWO access mode by using
    RWO PVC in multiple pods
    """
    @pytest.fixture(autouse=True)
    def setup(self, interface, pvc_factory, pod_factory):
        """
        Create pvc and pod
        """
        # Create a PVC
        self.pvc_obj = pvc_factory(
            interface=interface,
            project=None,
            storageclass=None,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            custom_data=None,
            status=constants.STATUS_BOUND
        )

        # Create a pod
        self.pod_obj = pod_factory(
            interface=interface,
            pvc=self.pvc_obj,
            custom_data=None,
            status=constants.STATUS_RUNNING
        )

    def test_verify_rwo_using_multiple_pods(self, interface, pod_factory):
        """
        This test case verifies RWO access mode by trying to mount same RWO
        PVC on different pods, delete running pods one by one and ensure that
        only one pod is running at a time
        """
        pod_objs_running = [self.pod_obj]
        pod_objs_not_running = []

        # Create 5 new pods using same PVC
        for _ in range(5):
            pod_obj = pod_factory(
                interface=interface,
                pvc=self.pvc_obj,
                custom_data=None,
                status=""
            )
            pod_objs_not_running.append(pod_obj)

        # Check the status of pods, delete running pod and verify new pod is
        # coming up running
        while pod_objs_running:
            pod_running_node = pod_objs_running[0].get()['spec']['nodeName']

            # Verify status of pods
            log.info("Check the status of pods")
            for pod_obj in pod_objs_not_running:
                try:
                    wait_for_resource_state(
                        resource=pod_obj, state=constants.STATUS_RUNNING,
                        timeout=60
                    )
                    assert (
                        pod_obj.get()['spec']['nodeName'] == pod_running_node
                    ), (
                        f"Unexpected: Pod {pod_obj} is in Running state. "
                        f"RWO PVC {self.pvc_obj.name} is mounted on pods "
                        f"which are on different nodes."
                    )
                    log.info(
                        f"Expected: Pod {pod_obj.name} is in Running. "
                        f"Pods which are running are on the same node "
                        f"{pod_running_node}"
                    )
                    pod_objs_running.append(pod_obj)
                except ResourceWrongStatusException:
                    log.info(f"Verified: Pod {pod_obj.name} is not Running")

            pod_objs_not_running = [pod for pod in pod_objs_not_running if (
                pod not in pod_objs_running
            )]

            if not pod_objs_not_running:
                log.info("Verified all pods.")
                break

            # Delete running pods
            log.info("Deleting pods which are in Running state.")
            for pod_obj in pod_objs_running:
                pod_obj.delete()

            # Confirm that pods are deleted
            for pod_obj in pod_objs_running:
                pod_obj.ocp.wait_for_delete(pod_obj.name)
            log.info("All running pods are deleted.")

            pod_objs_running.clear()

            # Wait for a pod to come up running
            log.info("Waiting for a pod to be up and running")
            for pod_obj in pod_objs_not_running:
                try:
                    wait_for_resource_state(
                        resource=pod_obj, state=constants.STATUS_RUNNING,
                        timeout=120
                    )
                    pod_objs_running.append(pod_obj)
                    break
                except ResourceWrongStatusException:
                    log.info(
                        f"{pod_obj.name} is not running. Checking status "
                        f"of other nodes."
                    )

            pod_objs_not_running = [pod for pod in pod_objs_not_running if (
                pod not in pod_objs_running
            )]
