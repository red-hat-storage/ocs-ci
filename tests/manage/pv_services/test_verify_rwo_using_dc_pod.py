import logging
import pytest
from itertools import cycle

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ResourceWrongStatusException, TimeoutExpiredError
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.ocp import OCP
from ocs_ci.helpers.helpers import wait_for_resource_state, create_pod

from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@green_squad
@tier2
@pytest.mark.parametrize(
    argnames="interface",
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL],
            marks=[
                pytest.mark.polarion_id("OCS-896"),
            ],
        ),
    ],
)
class TestVerifyRwoUsingReplicatedPod(ManageTest):
    """
    This test class consists of tests to verify RWO volume is exclusively
    mounted.
    """

    @pytest.fixture(autouse=True)
    def setup(self, interface, pvc_factory, service_account_factory, teardown_factory):
        """
        Create dc pod with replica 5
        """
        self.replica_count = 5
        pvc_obj = pvc_factory(interface=interface, size=3)
        sa_obj = service_account_factory(project=pvc_obj.project)
        try:
            pod1 = create_pod(
                interface_type=interface,
                pvc_name=pvc_obj.name,
                namespace=pvc_obj.namespace,
                sa_name=sa_obj.name,
                dc_deployment=True,
                replica_count=self.replica_count,
                deploy_pod_status=constants.STATUS_RUNNING,
            )
        except TimeoutExpiredError:
            # The test cannot be continued if all the pods are created on the same node
            pods = pod.get_all_pods(namespace=pvc_obj.namespace)
            pod_nodes = [pod.get_pod_node(pod_obj).name for pod_obj in pods]
            if set(pod_nodes) == 1:
                pytest.skip(
                    "All pods are created on same node and reached Running state"
                )
            raise

        self.name = pod1.labels["name"]
        self.namespace = pod1.namespace

        dc_obj = OCP(
            kind=constants.DEPLOYMENTCONFIG,
            namespace=self.namespace,
            resource_name=self.name,
        )
        dc_info = dc_obj.get(resource_name=self.name, selector=f"app={self.name}")[
            "items"
        ][0]

        dc_obj = OCS(**dc_info)
        teardown_factory(dc_obj)

    def wait_for_pods_and_verify(self):
        """
        Wait for the pods to be created and verify only one pod is running
        """
        # Wait for pods
        for pods in TimeoutSampler(
            360,
            2,
            func=pod.get_all_pods,
            namespace=self.namespace,
            selector=[self.name],
            selector_label="name",
        ):
            if len(pods) == self.replica_count:
                break

        pods_iter = cycle(pods)

        # Wait for one pod to be in Running state
        curr_pod = next(pods_iter)
        sampler = TimeoutSampler(360, 2, curr_pod.get)
        for pod_info in sampler:
            if pod_info["status"]["phase"] == constants.STATUS_RUNNING:
                self.running_pod = curr_pod
                log.info(f"Pod {curr_pod.name} reached state Running.")
                break
            curr_pod = next(pods_iter)
            sampler.func = curr_pod.get

        pods.remove(self.running_pod)
        pod_running_node = self.running_pod.get()["spec"]["nodeName"]
        # Verify the other pods are not coming up Running
        for pod_obj in pods:
            try:
                wait_for_resource_state(
                    resource=pod_obj, state=constants.STATUS_RUNNING, timeout=10
                )
                assert pod_obj.get()["spec"]["nodeName"] == pod_running_node, (
                    f"Unexpected: Pod {pod_obj.name} is in Running state. "
                    f"RWO PVC is mounted on pods which are on different nodes."
                )
                log.info(
                    f"Expected: Pod {pod_obj.name} is Running. "
                    f"Pods which are running are on the same node "
                    f"{pod_running_node}"
                )
            except ResourceWrongStatusException:
                log.info(f"Verified: Pod {pod_obj.name} is not in " f"running state.")

    def test_verify_rwo_using_replicated_pod(self):
        """
        This test case verifies that RWO volume is exclusively mounted by using
        replica 5 dc
        """
        self.wait_for_pods_and_verify()

        # Delete running pod
        self.running_pod.delete()
        self.running_pod.ocp.wait_for_delete(self.running_pod.name)

        self.wait_for_pods_and_verify()
