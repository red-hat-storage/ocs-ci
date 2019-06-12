import pytest
import logging

from ocs import exceptions, constants
from resources import pod
from ocsci.testlib import ManageTest, tier1

logger = logging.getLogger(__name__)


@tier1
@pytest.mark.usefixtures(
    "create_rbd_secret",
    "create_ceph_block_pool",
    "create_rbd_storageclass",
    "create_pvc",
    "create_pod"
)
class TestCaseOCS371(ManageTest):
    """
    Delete PVC while IO is in progress
    """

    def test_run_io_and_delete_pvc(self):
        """
        Delete PVC while IO is in progress
        """
        thread = pod.run_io_in_bg(self.pod_obj, expect_to_fail=True)
        self.pvc_obj.delete(wait=False)

        # This is a workaround for bug 1715627 (replaces wait_for_resource)
        pvc_out = self.pvc_obj.get(out_yaml_format=False)
        assert constants.STATUS_TERMINATING in pvc_out, (
            f"PVC {self.pvc_obj.name} failed to reach status {constants.STATUS_TERMINATING}"
        )

        thread.join(timeout=15)
        logger.info(f"Deleting PVC {self.pod_obj.name}")
        self.pod_obj.delete()

        # The PVC will no longer exist because the pod got deleted while it was
        # in Terminating status. Hence, catching this exception
        try:
            self.pvc_obj.get(out_yaml_format=False)
        except exceptions.CommandFailed as ex:
            if "NotFound" in str(ex):
                pass
