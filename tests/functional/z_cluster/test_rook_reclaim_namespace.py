import logging
import pytest


from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.framework.pytest_customization.marks import brown_squad
from ocs_ci.helpers.helpers import (
    create_reclaim_space_job,
    verify_log_exist_in_pods_logs,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
    skipif_ocs_version,
    skipif_external_mode,
    ignore_leftovers,
)

log = logging.getLogger(__name__)


@brown_squad
@tier2
@ignore_leftovers
@bugzilla("2214838")
@skipif_external_mode
@skipif_ocs_version("<4.13")
@pytest.mark.polarion_id("OCS-XXXX")
class TestRookReclaimNamespace(ManageTest):
    """
    Test Rook Reclaim Namespace

    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        def finalizer():
            try:
                self.reclaim_space_job_obj.delete()
            except Exception as e:
                log.info(f"Exception: {e}")

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
        """
        pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            status=constants.STATUS_BOUND,
            size=2,
        )
        pod_dict_path = constants.NGINX_POD_YAML
        raw_block_pv = False
        log.info(
            f"Created new pod sc_name={constants.CEPHFILESYSTEM} size=10Gi, "
            f"access_mode={constants.ACCESS_MODE_RWX}, volume_mode={constants.VOLUME_MODE_FILESYSTEM}"
        )
        pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=pvc_obj,
            status=constants.STATUS_RUNNING,
            pod_dict_path=pod_dict_path,
            raw_block_pv=raw_block_pv,
        )
        self.reclaim_space_job_obj = create_reclaim_space_job(pvc_name=pvc_obj.name)
        expected_log = "skipping sparsify operation"
        pod_names = get_pod_name_by_pattern(pattern="csi-rbdplugin-provisioner")
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
