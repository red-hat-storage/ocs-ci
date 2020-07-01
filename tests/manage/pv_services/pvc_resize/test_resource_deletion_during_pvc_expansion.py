import logging
import pytest
from concurrent.futures import ThreadPoolExecutor

from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    skipif_ocs_version, ManageTest, tier4, tier4b, ignore_leftover_label
)
from tests import disruption_helpers

log = logging.getLogger(__name__)


@tier4
@tier4b
@skipif_ocs_version('<4.5')
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
    def setup(
        self, multi_pvc_factory, dc_pod_factory, service_account_factory
    ):
        """
        Create PVCs and pods

        """
        pvc_size = 10
        access_modes_cephfs = [
            constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX
        ]
        access_modes_rbd = [
            constants.ACCESS_MODE_RWO, f'{constants.ACCESS_MODE_RWO}-Block',
            f'{constants.ACCESS_MODE_RWX}-Block'
        ]

        pvcs_cephfs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM, size=pvc_size,
            access_modes=access_modes_cephfs, status=constants.STATUS_BOUND,
            num_of_pvc=2, timeout=90
        )

        pvcs_rbd = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=pvcs_cephfs[0].project, size=pvc_size,
            access_modes=access_modes_rbd,
            status=constants.STATUS_BOUND, num_of_pvc=3, timeout=90
        )
        self.pvcs = pvcs_cephfs + pvcs_rbd

        # Set volume mode on PVC objects
        for pvc_obj in self.pvcs:
            pvc_info = pvc_obj.get()
            setattr(pvc_obj, 'volume_mode', pvc_info['spec']['volumeMode'])

        sa_obj = service_account_factory(project=pvcs_cephfs[0].project)

        self.pods = []
        for pvc_obj in self.pvcs:
            if constants.CEPHFS_INTERFACE in pvc_obj.storageclass.name:
                interface = constants.CEPHFILESYSTEM
            else:
                interface = constants.CEPHBLOCKPOOL
            # Create pods. Create 2 pods if PVC access mode is RWX
            pod_objs = [
                dc_pod_factory(
                    interface=interface,
                    pvc=pvc_obj,
                    raw_block_pv=pvc_obj.volume_mode == 'Block',
                    sa_obj=sa_obj
                ) for _ in range(
                    int(pvc_obj.access_mode != constants.ACCESS_MODE_RWX), 2
                )
            ]
            self.pods.extend(pod_objs)

        log.info(
            f"Created {len(pvcs_cephfs)} cephfs PVCs and {len(pvcs_rbd)} rbd "
            f"PVCs. Created {len(self.pods)} pods. "
        )

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
                runtime=30, fio_filename=f'{pod_obj.name}_f1'
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

        # Run IO to fill some data
        log.info("Write more data after PVC expansion.")
        for pod_obj in self.pods:
            storage_type = (
                'block' if pod_obj.pvc.volume_mode == 'Block' else 'fs'
            )
            pod_obj.run_io(
                storage_type=storage_type, size='10G', io_direction='write',
                runtime=30, fio_filename=f'{pod_obj.name}_f2'
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
