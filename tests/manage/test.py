import logging
import pytest
import random
from ocs_ci.framework.testlib import (
    tier4, tier4b, ManageTest, aws_platform_required,
    ipi_deployment_required, ignore_leftovers)
from ocs_ci.ocs import machine
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from tests.helpers import wait_for_resource_state
from tests.sanity_helpers import Sanity
from ocs_ci.ocs.node import add_new_node_and_label_it
from tests.helpers import get_worker_nodes


log = logging.getLogger(__name__)


@ignore_leftovers
@tier4
@tier4b
@aws_platform_required
@ipi_deployment_required
class TestAutomatedRecoveryFromFailedNodes(ManageTest):
    """
    Knip-678 Automated recovery from failed nodes
    """


    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.mark.parametrize(
        argnames="failure",
        argvalues=[
            # pytest.param(
            #     *["shutdown"]
            # )
            pytest.param(
                *["terminate"]
            )
        ]
    )

    def test_automated_recovery_from_failed_nodes_IPI_reactive(
            self, nodes, pvc_factory, pod_factory, failure, dc_pod_factory
    ):
        """
        Knip-678 Automated recovery from failed nodes
        Reactive case - IPI
        """


        # Create app pods on all the nodes
        dc_rbd1 = dc_pod_factory(
            interface=constants.CEPHBLOCKPOOL)
        pod.run_io_in_bg(dc_rbd1, expect_to_fail=True, fedora_dc=True)
        # dc_rbd2 = dc_pod_factory(
        #     interface=constants.CEPHBLOCKPOOL)
        # pod.run_io_in_bg(dc_rbd2, expect_to_fail=False, fedora_dc=True)

        rbd3 = pod_factory(interface=constants.CEPHBLOCKPOOL)
        pod.run_io_in_bg(rbd3, expect_to_fail=False)