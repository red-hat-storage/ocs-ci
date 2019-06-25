import pytest
import logging

from ocs import exceptions, constants
from resources import pod
from ocsci.testlib import ManageTest, tier1
from tests.fixtures import (
    create_rbd_storageclass, create_pod, create_pvc, create_ceph_block_pool,
    create_rbd_secret
)

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
    create_pvc.__name__,
    create_pod.__name__
)
@pytest.mark.polarion_id("OCS-371")
class TestCaseOCS371(ManageTest):
    """
    Delete PVC while IO is in progress

    https://polarion.engineering.redhat.com/polarion/#/project/
    OpenShiftContainerStorage/workitem?id=OCS-371
    """

    @tier1
    def test_run_io_and_delete_pvc(self):
        """
        Delete PVC while IO is in progress
        """
        thread = pod.run_io_in_bg(self.pod_obj, expect_to_fail=True)
        self.pvc_obj.delete(wait=False)

        # This is a workaround for bug 1715627 (replaces wait_for_resource)
        pvc_out = self.pvc_obj.get(out_yaml_format=False)
        assert constants.STATUS_TERMINATING in pvc_out, (
            f"PVC {self.pvc_obj.name} failed to reach status "
            f"{constants.STATUS_TERMINATING}"
        )

        thread.join(timeout=15)

        self.pod_obj.delete()

        # The PVC will no longer exist because the pod got deleted while it was
        # in Terminating status. Hence, catching this exception
        try:
            self.pvc_obj.get(out_yaml_format=False)
        except exceptions.CommandFailed as ex:
            if "NotFound" in str(ex):
                pass
