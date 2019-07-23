import pytest
import logging

from ocs_ci.ocs import exceptions, constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import ManageTest, tier1

logger = logging.getLogger(__name__)


@pytest.mark.polarion_id("OCS-371")
class TestDeletePVCWhileRunningIO(ManageTest):
    """
    Delete PVC while IO is in progress
    """

    @tier1
    def test_run_io_and_delete_pvc(self, rbd_pod_factory):
        """
        Delete PVC while IO is in progress
        """
        rbd_pod = rbd_pod_factory()
        thread = pod.run_io_in_bg(rbd_pod, expect_to_fail=True)
        rbd_pod.pvc.delete(wait=False)

        # This is a workaround for bug 1715627 (replaces wait_for_resource)
        pvc_out = rbd_pod.pvc.get(out_yaml_format=False)
        assert constants.STATUS_TERMINATING in pvc_out, (
            f"PVC {rbd_pod.pvc.name} "
            f"failed to reach status {constants.STATUS_TERMINATING}"
        )

        thread.join(timeout=15)

        rbd_pod.delete()

        # The PVC will no longer exist because the pod got deleted while it was
        # in Terminating status. Hence, catching this exception
        try:
            rbd_pod.pvc.get(out_yaml_format=False)
        except exceptions.CommandFailed as ex:
            if "NotFound" in str(ex):
                pass
