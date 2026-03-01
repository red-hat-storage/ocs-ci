import logging
import time
import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    polarion_id,
)
from ocs_ci.ocs.resources.pod import (
    get_csi_addons_controller_manager_pod,
    get_pods_aggregated_metrics,
)
from ocs_ci.framework.testlib import skipif_disconnected_cluster, tier1

log = logging.getLogger(__name__)


@green_squad
@skipif_disconnected_cluster
class TestMemoryStressWithCSIAddon:

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

        # Create encrypted PVCs
        log.info("Creating encrypted PVCs...")
        pvc_objs = multi_pvc_factory(
            size=1,
            num_of_pvc=100,
            storageclass=sc_obj,
            access_modes=[constants.ACCESS_MODE_RWO],
            wait_each=True,
            project=proj_obj,
        )
        log.info(f"Created {len(pvc_objs)} encrypted PVCs")

        # Create deployments (1:1 mapping with PVCs)
        log.info("Creating deployments (1:1 mapping with PVCs)...")
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

        return {
            "kms": kms,
            "project": proj_obj,
            "sc_obj": sc_obj,
            "pvc_objs": pvc_objs,
            "pod_objs": pod_objs,
        }

    @polarion_id("OCS-7447")
    @tier1
    def test_memory_stress_with_pvc_reconciler(self, setup_test_resources):
        """
        Verify PVC reconciler memory usage remains stable with 100 encrypted PVCs.

        Test Steps:
        1. Capture initial memory/CPU metrics of PVC reconciler pod
        2. Monitor memory/CPU metrics over time (20 iterations, 30s intervals)
        3. Capture final memory/CPU metrics and verify memory returned to initial levels

        """
        log.info("Starting memory stress test with PVC reconciler")

        # Get test resources
        # pvc_objs = setup_test_resources["pvc_objs"]

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

        # Step 2: Monitor memory/CPU stats over time
        log.info("=== Step 2: Monitoring memory/CPU stats over time ===")
        max_memory = 0
        max_cpu = 0

        # Monitor metrics over time
        for i in range(20):  # Monitor for up to 10 minutes (30s intervals)
            time.sleep(30)
            current_metrics = get_pods_aggregated_metrics(reconciler_pods)
            max_memory = max(max_memory, current_metrics["max_memory_mib"])
            max_cpu = max(max_cpu, current_metrics["max_cpu_millicores"])
            log.info(
                f"Metrics check {i+1}/20 - Memory: {current_metrics['max_memory_mib']:.1f}Mi, "
                f"CPU: {current_metrics['max_cpu_millicores']}m"
            )

        log.info(f"Peak metrics - Memory: {max_memory:.1f}Mi, " f"CPU: {max_cpu}m")

        # Step 3: Get final metrics
        log.info("=== Step 3: Getting final metrics ===")
        time.sleep(10)  # Wait for metrics to stabilize
        final_metrics = get_pods_aggregated_metrics(reconciler_pods)
        log.info(
            f"Final metrics - Memory: {final_metrics['max_memory_mib']:.1f}Mi "
            f"(total: {final_metrics['total_memory_mib']:.1f}Mi), "
            f"CPU: {final_metrics['max_cpu_millicores']}m "
            f"(total: {final_metrics['total_cpu_millicores']}m)"
        )

        # Verify memory returned to initial levels (allow 20% variance)
        # Check if initial memory is zero to avoid ZeroDivisionError
        if initial_metrics["max_memory_mib"] == 0:
            log.warning(
                f"Initial memory metrics were zero. Cannot calculate ratio. "
                f"Initial: {initial_metrics['max_memory_mib']:.1f}Mi, "
                f"Final: {final_metrics['max_memory_mib']:.1f}Mi"
            )
            # If initial was zero but final is not, memory increased
            if final_metrics["max_memory_mib"] > 0:
                pytest.fail(
                    f"Initial memory was zero but final memory is "
                    f"{final_metrics['max_memory_mib']:.1f}Mi. "
                    f"This suggests metrics collection issue."
                )
            # Both are zero, skip ratio check
            log.info(
                "Both initial and final memory metrics are zero. Skipping ratio check."
            )
        else:
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

        log.info("Test completed successfully!")
        log.info(
            f"Summary:\n"
            f"  Initial memory: {initial_metrics['max_memory_mib']:.1f}Mi, "
            f"CPU: {initial_metrics['max_cpu_millicores']}m\n"
            f"  Peak memory: {max_memory:.1f}Mi, "
            f"CPU: {max_cpu}m\n"
            f"  Final memory: {final_metrics['max_memory_mib']:.1f}Mi, "
            f"CPU: {final_metrics['max_cpu_millicores']}m"
        )
