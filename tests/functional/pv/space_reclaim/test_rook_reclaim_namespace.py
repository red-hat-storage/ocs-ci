import logging
import time

import pytest


from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.resources.pod import get_csi_provisioner_pod
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.helpers.helpers import (
    create_reclaim_space_job,
    verify_log_exist_in_pods_logs,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_ocs_version,
)

logger = logging.getLogger(__name__)


@green_squad
@tier2
@skipif_ocs_version("<4.13")
@pytest.mark.polarion_id("OCS-5424")
class TestRookReclaimNamespace(ManageTest):
    """
    Test Rook Reclaim Namespace

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            try:
                self.reclaim_job_after_pod_delete.delete()
                self.reclaim_job_before_pod_delete.delete()
            except Exception as e:
                logger.warning(f"Cleanup exception during teardown: {e}")

        request.addfinalizer(finalizer)

    def test_rook_reclaim_namespace(self, pvc_factory, pod_factory, teardown_factory):
        """
        Test Process:

        1.Create RBD PVC with filesystem mode
        2.Create Pod using that PVC
        3.Create reclaimspacejob CR to run on that PVC
        4.Verify reclaimspacejob successful completion
        5.Check logs of csi-rbdplugin-provisioner-xxx/csi-rbdplugin pods.
        6.Verify sparsify is skipped.
        7.Delete pod
        8.Delete reclaimspacejob CR
        9.Recreate reclaimspacejob CR
        10.Sleep 120 seconds so the logs in csi-rbdplugin-provisioner-xxx/csi-rbdplugin will be updated
        11.Verify logs does not show 'skipping sparsify operation' message.
        """
        logger.test_step("Create RBD PVC with filesystem mode and attach pod")
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_BOUND,
            size=2,
        )
        pod_dict_path = constants.NGINX_POD_YAML
        raw_block_pv = False

        logger.info(
            f"Creating pod for PVC {pvc_obj.name} with "
            f"volume_mode={constants.VOLUME_MODE_FILESYSTEM}"
        )
        pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=pod_dict_path,
            raw_block_pv=raw_block_pv,
        )

        logger.test_step(
            f"Create reclaimspacejob CR for PVC {pvc_obj.name} and verify sparsify is skipped"
        )
        self.reclaim_job_before_pod_delete = create_reclaim_space_job(
            pvc_name=pvc_obj.name
        )
        expected_log = "skipping sparsify operation"
        pod_names = get_csi_provisioner_pod(interface=constants.CEPHBLOCKPOOL)

        logger.info(
            f"Checking logs of csi-rbdplugin-provisioner pods {pod_names} for '{expected_log}'"
        )
        sample = TimeoutSampler(
            timeout=100,
            sleep=5,
            func=verify_log_exist_in_pods_logs,
            pod_names=pod_names,
            expected_log=expected_log,
        )
        if not sample.wait_for_func_status(result=True):
            raise ValueError(
                f"The expected log '{expected_log}' does not exist in {pod_names} pods"
            )

        logger.test_step(
            "Delete pod and reclaimspacejob CR, then recreate reclaimspacejob"
        )
        logger.info(f"Deleting pod {pod_obj.name}")
        pod_obj.delete()

        logger.info(
            f"Deleting reclaimspacejob CR {self.reclaim_job_before_pod_delete.name}"
        )
        self.reclaim_job_before_pod_delete.delete()

        logger.info("Recreating reclaimspacejob CR")
        self.reclaim_job_after_pod_delete = create_reclaim_space_job(
            pvc_name=pvc_obj.name
        )

        logger.info(f"Waiting 120 seconds for logs in {pod_names} to update")
        time.sleep(120)

        logger.test_step(
            "Verify logs do not show 'skipping sparsify operation' after pod deletion"
        )
        log_exist = verify_log_exist_in_pods_logs(
            pod_names=pod_names, expected_log=expected_log, since="120s"
        )
        if log_exist:
            raise ValueError(
                f"The expected log '{expected_log}' exist in {pod_names} pods after reclaimspacejob deletion"
            )
