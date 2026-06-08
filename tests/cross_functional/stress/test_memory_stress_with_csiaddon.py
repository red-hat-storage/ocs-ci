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

logger = logging.getLogger(__name__)


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
        logger.test_step("Set up KMS, project, encrypted StorageClass, and Vault token")

        # Setup KMS
        kms = pv_encryption_kms_setup_factory("v1", False)
        logger.info(f"KMS setup completed: {kms.kmsid}")

        # Create project
        proj_obj = project_factory()
        logger.info(f"Project created: {proj_obj.namespace}")

        # Create encrypted StorageClass
        sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=kms.kmsid,
        )
        logger.info(f"Encrypted StorageClass created: {sc_obj.name}")

        # Create Vault CSI KMS token
        kms.vault_path_token = kms.generate_vault_token()
        kms.create_vault_csi_kms_token(namespace=proj_obj.namespace)
        logger.info("Vault CSI KMS token created")

        # Create encrypted PVCs
        logger.test_step("Create 100 encrypted PVCs")
        pvc_objs = multi_pvc_factory(
            size=1,
            num_of_pvc=100,
            storageclass=sc_obj,
            access_modes=[constants.ACCESS_MODE_RWO],
            wait_each=True,
            project=proj_obj,
        )
        logger.info(f"Created {len(pvc_objs)} encrypted PVCs")

        # Create deployments (1:1 mapping with PVCs)
        logger.test_step(f"Create {len(pvc_objs)} deployments (1:1 mapping with PVCs)")
        pod_objs = []
        for i, pvc_obj in enumerate(pvc_objs):
            pod_obj = pod_factory(
                pvc=pvc_obj,
                interface=constants.CEPHBLOCKPOOL,
                status=constants.STATUS_RUNNING,
            )
            pod_objs.append(pod_obj)
            if (i + 1) % 10 == 0:
                logger.debug(f"Created {i + 1}/{len(pvc_objs)} deployments")

        logger.info(f"Created {len(pod_objs)} deployments")

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
        logger.test_step("Get CSI addons controller manager reconciler pods")

        # Get test resources
        # pvc_objs = setup_test_resources["pvc_objs"]

        # Get reconciler pods (CSI addons controller manager)
        reconciler_pods = get_csi_addons_controller_manager_pod()
        logger.assertion(
            f"Reconciler pods found: expected='>0', actual='{len(reconciler_pods)}'"
        )
        assert len(reconciler_pods) > 0, "No CSI addons controller manager pods found"
        logger.info(f"Found {len(reconciler_pods)} reconciler pod(s)")

        # Step 1: Monitor initial memory/CPU stats
        logger.test_step("Capture initial memory/CPU metrics of PVC reconciler pods")
        time.sleep(10)  # Wait for metrics to stabilize
        initial_metrics = get_pods_aggregated_metrics(reconciler_pods)
        logger.info(
            f"Initial metrics - Memory: {initial_metrics['max_memory_mib']:.1f}Mi "
            f"(total: {initial_metrics['total_memory_mib']:.1f}Mi), "
            f"CPU: {initial_metrics['max_cpu_millicores']}m "
            f"(total: {initial_metrics['total_cpu_millicores']}m)"
        )

        # Step 2: Monitor memory/CPU stats over time
        logger.test_step(
            "Monitor memory/CPU metrics over 20 iterations at 30s intervals"
        )
        max_memory = 0
        max_cpu = 0

        # Monitor metrics over time
        logger.info("Starting 20 metrics collection iterations (30s intervals)")
        for i in range(20):  # Monitor for up to 10 minutes (30s intervals)
            time.sleep(30)
            current_metrics = get_pods_aggregated_metrics(reconciler_pods)
            max_memory = max(max_memory, current_metrics["max_memory_mib"])
            max_cpu = max(max_cpu, current_metrics["max_cpu_millicores"])
            logger.debug(
                f"Metrics check {i+1}/20 - Memory: {current_metrics['max_memory_mib']:.1f}Mi, "
                f"CPU: {current_metrics['max_cpu_millicores']}m"
            )

        logger.info(
            f"Peak metrics after 20 iterations - Memory: {max_memory:.1f}Mi, CPU: {max_cpu}m"
        )

        # Step 3: Get final metrics
        logger.test_step("Capture final memory/CPU metrics and verify stability")
        time.sleep(10)  # Wait for metrics to stabilize
        final_metrics = get_pods_aggregated_metrics(reconciler_pods)
        logger.info(
            f"Final metrics - Memory: {final_metrics['max_memory_mib']:.1f}Mi "
            f"(total: {final_metrics['total_memory_mib']:.1f}Mi), "
            f"CPU: {final_metrics['max_cpu_millicores']}m "
            f"(total: {final_metrics['total_cpu_millicores']}m)"
        )

        # Verify memory returned to initial levels (allow 20% variance)
        # Check if initial memory is zero to avoid ZeroDivisionError
        if initial_metrics["max_memory_mib"] == 0:
            logger.warning(
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
            logger.info(
                "Both initial and final memory metrics are zero. Skipping ratio check."
            )
        else:
            memory_increase_ratio = (
                final_metrics["max_memory_mib"] / initial_metrics["max_memory_mib"]
            )
            logger.info(
                f"Memory ratio (final/initial): {memory_increase_ratio:.2f} "
                f"(initial: {initial_metrics['max_memory_mib']:.1f}Mi, "
                f"final: {final_metrics['max_memory_mib']:.1f}Mi)"
            )

            # Assert memory is close to initial (within 20% variance)
            logger.assertion(
                f"Memory increase ratio: expected='<=1.2', actual='{memory_increase_ratio:.2f}'"
            )
            assert memory_increase_ratio <= 1.2, (
                f"Memory did not return to initial levels. "
                f"Initial: {initial_metrics['max_memory_mib']:.1f}Mi, "
                f"Final: {final_metrics['max_memory_mib']:.1f}Mi, "
                f"Ratio: {memory_increase_ratio:.2f}"
            )

        logger.info("Test completed successfully")
        logger.info(
            f"Summary - "
            f"Initial memory: {initial_metrics['max_memory_mib']:.1f}Mi, "
            f"CPU: {initial_metrics['max_cpu_millicores']}m | "
            f"Peak memory: {max_memory:.1f}Mi, "
            f"CPU: {max_cpu}m | "
            f"Final memory: {final_metrics['max_memory_mib']:.1f}Mi, "
            f"CPU: {final_metrics['max_cpu_millicores']}m"
        )
