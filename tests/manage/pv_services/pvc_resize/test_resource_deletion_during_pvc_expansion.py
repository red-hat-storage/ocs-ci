import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier4, tier4b, ignore_leftover_label,
    skipif_upgraded_from
)
from ocs_ci.utility.utils import ceph_health_check
from tests import disruption_helpers

log = logging.getLogger(__name__)


@tier4
@tier4b
@skipif_ocs_version('<4.5')
@skipif_upgraded_from(['4.4'])
@ignore_leftover_label(constants.drain_canary_pod_label)
@pytest.mark.parametrize(
    argnames='resource_to_delete',
    argvalues=[
        pytest.param(
            'mgr', marks=pytest.mark.polarion_id('OCS-2224')
        ),
        pytest.param(
            'osd', marks=pytest.mark.polarion_id('OCS-2225')
        ),
        pytest.param(
            'rbdplugin', marks=pytest.mark.polarion_id('OCS-2226')
        ),
        pytest.param(
            'cephfsplugin', marks=pytest.mark.polarion_id('OCS-2227')
        ),
        pytest.param(
            'rbdplugin_provisioner', marks=pytest.mark.polarion_id('OCS-2228')
        ),
        pytest.param(
            'cephfsplugin_provisioner',
            marks=pytest.mark.polarion_id('OCS-2229')
        )
    ]
)
class TestResourceDeletionDuringPvcExpansion(ManageTest):
    """
    Tests to verify PVC expansion will be success even if rook-ceph, csi pods
    are re-spun during the expansion

    """
    @pytest.fixture(autouse=True)
    def setup(self, create_pvcs_and_pods):
        """
        Create PVCs and pods

        """
        self.pvcs, self.pods = create_pvcs_and_pods(
            pvc_size=10, pods_for_rwx=2
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Make sure ceph health is ok

        """

        def finalizer():
            assert ceph_health_check(), "Ceph cluster health is not OK"
            log.info("Ceph cluster health is OK")

        request.addfinalizer(finalizer)

    def test_resource_deletion_during_pvc_expansion(self, resource_to_delete):
        """
        Verify PVC expansion will succeed when rook-ceph, csi pods are re-spun
        during expansion

        """
        pvc_size_expanded = 30
        executor = ThreadPoolExecutor(max_workers=len(self.pvcs))
        disruption_ops = disruption_helpers.Disruptions()

        # Run IO to fill some data
        log.info(
            "Running IO on all pods to fill some data before PVC expansion."
        )
        for pod_obj in self.pods:
            storage_type = (
                'block' if pod_obj.pvc.volume_mode == 'Block' else 'fs'
            )
            pod_obj.run_io(
                storage_type=storage_type, size='4G', io_direction='write',
                runtime=30, rate='10M', fio_filename=f'{pod_obj.name}_f1'
            )

        log.info("Wait for IO to complete on pods")
        for pod_obj in self.pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get('jobs')[0].get('error')
            assert err_count == 0, (
                f"IO error on pod {pod_obj.name}. "
                f"FIO result: {fio_result}"
            )
            log.info(f"Verified IO on pod {pod_obj.name}.")
        log.info("IO is successful on all pods before PVC expansion.")

        # Select the pod to be deleted
        disruption_ops.set_resource(resource=resource_to_delete)

        log.info("Expanding all PVCs.")
        for pvc_obj in self.pvcs:
            log.info(
                f"Expanding size of PVC {pvc_obj.name} to {pvc_size_expanded}G"
            )
            pvc_obj.expand_proc = executor.submit(
                pvc_obj.resize_pvc, pvc_size_expanded, True
            )

        # Delete the pod 'resource_to_delete'
        disruption_ops.delete_resource()

        # Verify pvc expand status
        for pvc_obj in self.pvcs:
            assert pvc_obj.expand_proc.result(), (
                f"Expansion failed for PVC {pvc_obj.name}"
            )
        log.info("PVC expansion was successful on all PVCs")

        # Run IO to fill more data
        log.info("Write more data after PVC expansion.")
        for pod_obj in self.pods:
            storage_type = (
                'block' if pod_obj.pvc.volume_mode == 'Block' else 'fs'
            )
            pod_obj.run_io(
                storage_type=storage_type, size='10G', io_direction='write',
                runtime=30, rate='10M', fio_filename=f'{pod_obj.name}_f2'
            )

        log.info("Wait for IO to complete on all pods")
        for pod_obj in self.pods:
            fio_result = pod_obj.get_fio_results()
            err_count = fio_result.get('jobs')[0].get('error')
            assert err_count == 0, (
                f"IO error on pod {pod_obj.name}. "
                f"FIO result: {fio_result}"
            )
            log.info(f"Verified IO on pod {pod_obj.name}.")
        log.info("IO is successful on all pods after PVC expansion.")
