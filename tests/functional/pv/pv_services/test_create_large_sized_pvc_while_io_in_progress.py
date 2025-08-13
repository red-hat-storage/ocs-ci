import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    provider_mode,
    run_on_all_clients_push_missing_configs,
)
from ocs_ci.framework.testlib import ManageTest, tier2


log = logging.getLogger(__name__)


@provider_mode
@green_squad
@tier2
class TestCreateLargeSizedPVCWhileIOInProgress(ManageTest):
    """
    Create large sized PVC while IO is in progress

    """

    @pytest.mark.parametrize(
        argnames=["interface"],
        argvalues=[
            pytest.param(
                constants.CEPHBLOCKPOOL, marks=pytest.mark.polarion_id("OCS-2087")
            ),
            pytest.param(
                constants.CEPHFILESYSTEM, marks=pytest.mark.polarion_id("OCS-2088")
            ),
        ],
    )
    @run_on_all_clients_push_missing_configs
    def test_create_large_sized_pvc_while_io_in_progress(
        self, interface, pvc_factory, pod_factory, cluster_index
    ):
        """
        Flow is as below
        *. Create a large sized PVC
        *. Create an app pod and mount the PVC
        *. Start IO to run in background
        *. While IO is in-progress, repeat above all steps
        test_cyclic_largesized_pvc_app
        test_consecutive_largesized_pvc_and_app_pod_creation
        """
        # Repeating the above flow for 5 times
        for i in range(5):
            log.info(f"Creating {interface} based PVC")
            pvc_obj = pvc_factory(interface=interface, size="500")
            pod_obj = pod_factory(pvc=pvc_obj, interface=interface)
            pod.run_io_in_bg(pod_obj)
