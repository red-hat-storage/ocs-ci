"""
Test suite for memory stress testing with CSI addon PVC reconciler.

This test validates memory and CPU usage of the PVC reconciler pod during
encryption key rotation operations with a large number of encrypted PVCs.

Test Scenarios:
1. Create 100 encrypted PVCs with 100 deployments (1:1 mapping)
2. Monitor initial memory/CPU stats of reconciler pod
3. Annotate StorageClass for EKR with schedule: @weekly
4. Update SC annotation with schedule: * * * * *
5. Watch memory go up and ensure key rotation succeeds
6. Annotate SC with annotation enable: false
7. Wait for resources to be GCed
8. Monitor memory usage go down (should return to initial stats)
"""

import logging
import time
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    polarion_id,
    vault_kms_deployment_required,
)
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.ocs.resources.pod import (
    get_csi_addons_controller_manager_pod,
    get_pods_aggregated_metrics,
)
from ocs_ci.framework.testlib import skipif_disconnected_cluster

log = logging.getLogger(__name__)


@green_squad
@vault_kms_deployment_required
@skipif_disconnected_cluster
class TestMemoryStressWithCSIAddon:
    """
    Test memory stress with CSI addon PVC reconciler during key rotation operations.
    """

    @pytest.fixture(scope="function")
    def setup_test_resources(
        self,
        pv_encryption_kms_setup_factory,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
    ):
        """
        Setup test resources: KMS, project, encrypted StorageClass, PVCs, and deployments.

        Returns:
            dict: Dictionary containing all test resources
        """
        log.info("Setting up test resources...")

        # Setup KMS
        kms = pv_encryption_kms_setup_factory("v1", False)
        log.info(f"KMS setup completed: {kms.kmsid}")

        # Create project
        proj_obj = project_factory()
        log.info(f"Project created: {proj_obj.namespace}")

        # Create encrypted StorageClass
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=kms.kmsid,
        )
        log.info(f"Encrypted StorageClass created: {sc_obj.name}")

        # Create Vault CSI KMS token
        kms.vault_path_token = kms.generate_vault_token()
        kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)
        log.info("Vault CSI KMS token created")

        # Initialize PVKeyrotation helper and enable key rotation BEFORE creating PVCs
        # This ensures PVCs get keyrotation cronjobs immediately upon creation
        pv_keyrotation_obj = PVKeyrotation(sc_obj)
        pv_keyrotation_obj.set_keyrotation_state_by_annotation(enable=True)
        log.info("Key rotation enabled via StorageClass annotation")
        pv_keyrotation_obj.annotate_storageclass_key_rotation(schedule="@weekly")
        log.info("StorageClass annotated with @weekly schedule for key rotation")

        # Create 100 encrypted PVCs
        log.info("Creating 100 encrypted PVCs...")
        pvc_objs = multi_pvc_factory(
            size=1,
            num_of_pvc=100,
            storageclass=sc_obj,
            access_modes=[constants.ACCESS_MODE_RWO],
            wait_each=True,
            project=proj_obj,
        )
        log.info(f"Created {len(pvc_objs)} encrypted PVCs")

        # Create 100 deployments (1:1 mapping with PVCs)
        log.info("Creating 100 deployments (1:1 mapping with PVCs)...")
        pod_objs = []
        for i, pvc_obj in enumerate(pvc_objs):
            pod_obj = pod_factory(
                pvc=pvc_obj,
                interface=constants.CEPHBLOCKPOOL,
                status=constants.STATUS_RUNNING,
            )
            pod_objs.append(pod_obj)
            if (i + 1) % 10 == 0:
                log.info(f"Created {i + 1} deployments...")

        log.info(f"Created {len(pod_objs)} deployments")

        # Wait for key rotation cronjobs to be created for all PVCs
        log.info("Waiting for key rotation cronjobs to be created for all PVCs...")
        pv_keyrotation_obj.wait_for_keyrotation_cronjobs_recreation(pvc_objs)
        log.info("Key rotation cronjobs created successfully for all PVCs")

        return {
            "kms": kms,
            "project": proj_obj,
            "sc_obj": sc_obj,
            "pvc_objs": pvc_objs,
            "pod_objs": pod_objs,
            "pv_keyrotation_obj": pv_keyrotation_obj,
        }

    @polarion_id("RHSTOR-8091")
    def test_memory_stress_with_pvc_reconciler(self, setup_test_resources):
        """
        Test memory stress of PVC reconciler during key rotation operations.

        Steps:
        1. Monitor initial memory/CPU stats of reconciler pod
        2. Annotate StorageClass for EKR with schedule: @weekly
        3. Update SC annotation with schedule: * * * * *
        4. Watch memory go up and ensure key rotation succeeds
        5. Annotate SC with annotation enable: false
        6. Wait for resources to be GCed
        7. Monitor memory usage go down (should return to initial stats)
        """
        log.info("Starting memory stress test with PVC reconciler")

        # Get test resources
        pvc_objs = setup_test_resources["pvc_objs"]
        pv_keyrotation_obj = setup_test_resources["pv_keyrotation_obj"]

        # Get reconciler pods (CSI addons controller manager)
        reconciler_pods = get_csi_addons_controller_manager_pod()
        assert len(reconciler_pods) > 0, "No CSI addons controller manager pods found"
        log.info(f"Found {len(reconciler_pods)} reconciler pod(s)")

        # Step 1: Monitor initial memory/CPU stats
        log.info("=== Step 1: Monitoring initial memory/CPU stats ===")
        time.sleep(10)  # Wait for metrics to stabilize
        initial_metrics = get_pods_aggregated_metrics(reconciler_pods)
        log.info(
            f"Initial metrics - Memory: {initial_metrics['max_memory_mib']:.1f}Mi "
            f"(total: {initial_metrics['total_memory_mib']:.1f}Mi), "
            f"CPU: {initial_metrics['max_cpu_millicores']}m "
            f"(total: {initial_metrics['total_cpu_millicores']}m)"
        )

        # Step 2: Verify StorageClass annotation (already set in setup)
        log.info("=== Step 2: Verifying StorageClass EKR annotation (@weekly) ===")
        log.info("StorageClass already annotated with @weekly schedule from setup")

        # Step 3: Update SC annotation with schedule: * * * * *
        log.info("=== Step 3: Updating SC annotation with schedule: * * * * * ===")
        pv_keyrotation_obj.annotate_storageclass_key_rotation(schedule="* * * * *")
        log.info("StorageClass annotation updated to * * * * * schedule")

        # Step 4: Watch memory go up and ensure key rotation succeeds
        log.info("=== Step 4: Monitoring memory increase and key rotation ===")
        max_memory_during_kr = 0
        max_cpu_during_kr = 0

        # Monitor metrics during key rotation
        for i in range(20):  # Monitor for up to 10 minutes (30s intervals)
            time.sleep(30)
            current_metrics = get_pods_aggregated_metrics(reconciler_pods)
            max_memory_during_kr = max(
                max_memory_during_kr, current_metrics["max_memory_mib"]
            )
            max_cpu_during_kr = max(
                max_cpu_during_kr, current_metrics["max_cpu_millicores"]
            )
            log.info(
                f"Metrics check {i+1}/20 - Memory: {current_metrics['max_memory_mib']:.1f}Mi, "
                f"CPU: {current_metrics['max_cpu_millicores']}m"
            )

            # Check if memory has increased
            if (
                current_metrics["max_memory_mib"]
                > initial_metrics["max_memory_mib"] * 1.1
            ):
                log.info(
                    f"Memory increased from {initial_metrics['max_memory_mib']:.1f}Mi "
                    f"to {current_metrics['max_memory_mib']:.1f}Mi"
                )
                break

        # Verify key rotation succeeded
        log.info("Verifying key rotation succeeded...")
        pv_keyrotation_obj.wait_till_all_pv_keyrotation_on_vault_kms(pvc_objs)
        log.info("Key rotation completed successfully")

        log.info(
            f"Peak metrics during key rotation - Memory: {max_memory_during_kr:.1f}Mi, "
            f"CPU: {max_cpu_during_kr}m"
        )

        # Step 5: Annotate SC with annotation enable: false
        log.info("=== Step 5: Disabling key rotation via SC annotation ===")
        pv_keyrotation_obj.set_keyrotation_state_by_annotation(enable=False)
        log.info("Key rotation disabled via StorageClass annotation")

        # Step 6: Wait for resources to be GCed
        log.info("=== Step 6: Waiting for resources to be garbage collected ===")
        pv_keyrotation_obj.wait_for_keyrotation_cronjobs_deletion(pvc_objs)
        log.info("All key rotation cronjobs have been garbage collected")

        # Step 7: Monitor memory usage go down (should return to initial stats)
        log.info("=== Step 7: Monitoring memory decrease after GC ===")
        time.sleep(60)  # Wait for GC to complete and memory to stabilize

        final_metrics = get_pods_aggregated_metrics(reconciler_pods)
        log.info(
            f"Final metrics - Memory: {final_metrics['max_memory_mib']:.1f}Mi "
            f"(total: {final_metrics['total_memory_mib']:.1f}Mi), "
            f"CPU: {final_metrics['max_cpu_millicores']}m "
            f"(total: {final_metrics['total_cpu_millicores']}m)"
        )

        # Verify memory returned to initial levels (allow 20% variance)
        memory_increase_ratio = (
            final_metrics["max_memory_mib"] / initial_metrics["max_memory_mib"]
        )
        log.info(
            f"Memory ratio (final/initial): {memory_increase_ratio:.2f} "
            f"(initial: {initial_metrics['max_memory_mib']:.1f}Mi, "
            f"final: {final_metrics['max_memory_mib']:.1f}Mi)"
        )

        # Assert memory is close to initial (within 20% variance)
        assert memory_increase_ratio <= 1.2, (
            f"Memory did not return to initial levels. "
            f"Initial: {initial_metrics['max_memory_mib']:.1f}Mi, "
            f"Final: {final_metrics['max_memory_mib']:.1f}Mi, "
            f"Ratio: {memory_increase_ratio:.2f}"
        )

        log.info("✅ Test completed successfully!")
        log.info(
            f"Summary:\n"
            f"  Initial memory: {initial_metrics['max_memory_mib']:.1f}Mi, "
            f"CPU: {initial_metrics['max_cpu_millicores']}m\n"
            f"  Peak memory during KR: {max_memory_during_kr:.1f}Mi, "
            f"CPU: {max_cpu_during_kr}m\n"
            f"  Final memory: {final_metrics['max_memory_mib']:.1f}Mi, "
            f"CPU: {final_metrics['max_cpu_millicores']}m"
        )
