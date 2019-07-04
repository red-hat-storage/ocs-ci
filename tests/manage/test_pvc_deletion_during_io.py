import pytest
import logging

from ocs_ci.ocs import exceptions, constants
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import ManageTest, tier1
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
class TestDeletePVCWhileRunningIO(ManageTest):
    """
    Delete PVC while IO is in progress
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
            f"PVC {self.pvc_obj.name} "
            f"failed to reach status {constants.STATUS_TERMINATING}"
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

    @tier1
    def test_run_io(self):
        """
        Test IO
        """
        self.pod_obj.run_io('fs', '1G')
        logging.info("Waiting for results")
        fio_result = self.pod_obj.get_fio_results()
        logging.info("IOPs after FIO:")
        logging.info(
            f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
        )
        logging.info(
            f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
        )
