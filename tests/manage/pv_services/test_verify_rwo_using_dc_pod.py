import logging
import pytest
from itertools import cycle

from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import (
    ResourceWrongStatusException, UnexpectedBehaviour
)
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.ocp import OCP
from tests.helpers import wait_for_resource_state, create_pod

from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@tier2
@pytest.mark.parametrize(
    argnames='interface',
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL],
            marks=[
                pytest.mark.polarion_id('OCS-896'),
            ]
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM],
            marks=[
                pytest.mark.polarion_id('OCS-897'),
            ]
        )
    ]
)
class TestVerifyRwoUsingReplica2DcPod(ManageTest):
    """
    This test class consists of tests to verify RWO volume is exclusively
    mounted.
    """
    @pytest.fixture(autouse=True)
    def setup(
        self, interface, pvc_factory, service_account_factory, teardown_factory
    ):
        """
        Create dc pod with replica 2
        """
        pvc_obj = pvc_factory(interface=interface, size=3)
        sa_obj = service_account_factory(project=pvc_obj.project)
        pod1 = create_pod(
            interface_type=interface, pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace, sa_name=sa_obj.name,
            dc_deployment=True, replica_count=2,
            deploy_pod_status=constants.STATUS_RUNNING
        )
        self.name = pod1.labels['name']
        self.namespace = pod1.namespace

        dc_obj = OCP(
            kind=constants.DEPLOYMENTCONFIG, namespace=self.namespace,
            resource_name=self.name
        )
        dc_info = dc_obj.get(
            resource_name=self.name, selector=f'app={self.name}'
        )['items'][0]

        dc_obj = OCS(**dc_info)
        teardown_factory(dc_obj)

    def wait_for_pods_and_verify(self):
        """
        Wait for both the pods to be created and verify only one pod is running
        """
        # Wait for pods
        for pods in TimeoutSampler(
            180, 2, func=pod.get_all_pods, namespace=self.namespace,
            selector=[self.name], selector_label='name'
        ):
            if len(pods) == 2:
                break

        pods_iter = cycle(pods)

        # Wait for one pod to be in Running state
        sampler = TimeoutSampler(180, 2, next(pods_iter).get)
        for pod_info in sampler:
            sampler.func = next(pods_iter).get
            if pod_info['status']['phase'] == constants.STATUS_RUNNING:
                self.running_pod = next(pods_iter)
                break

        # Verify the other pod is not coming up Running
        try:
            self.pod_not_running = next(pods_iter)
            wait_for_resource_state(
                resource=self.pod_not_running, state=constants.STATUS_RUNNING,
                timeout=10
            )
            raise UnexpectedBehaviour(
                f"Unexpected: Pod {self.pod_not_running.name} is in "
                f"Running state"
            )
        except ResourceWrongStatusException:
            log.info(
                f"Verified: Only one pod {self.running_pod.name} is in "
                f"running state."
            )

    def test_verify_rwo_using_replica_2_dc_pod(self):
        """
        This test case verifies that RWO volume is exclusively mounted by using
        replica 2 dc
        """
        self.wait_for_pods_and_verify()

        # Delete running pod
        self.running_pod.delete()
        self.running_pod.ocp.wait_for_delete(self.running_pod.name)

        self.wait_for_pods_and_verify()
