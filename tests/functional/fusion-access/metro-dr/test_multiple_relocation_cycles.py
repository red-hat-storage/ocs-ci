import logging

import pytest

from ocs_ci.deployment.cnv import CNVInstaller
from ocs_ci.framework.pytest_customization.marks import (
    fusion_access_required,
    mdr,
    tier4a,
    turquoise_squad,
)
from ocs_ci.helpers import fusionaccess_dr_helpers as fa_dr_helpers
from ocs_ci.ocs import constants

logger = logging.getLogger(__name__)

polarion_id_multiple_relocations = "OCS-XXXX"

# Number of relocation operations performed by the test. The workloads start on
# the source site, so odd numbered cycles move them to the recovery site and
# even numbered cycles move them back to the source site.
RELOCATION_CYCLES = 5


@mdr
@tier4a
@turquoise_squad
@fusion_access_required
class TestFusionAccessMultipleRelocationCycles:
    """
    Verify that repeated Relocate operations between the source and the
    recovery site of a Fusion Access Metro-DR setup keep the application data
    intact, the system stable and the RPO at zero.

    """

    @pytest.mark.parametrize(
        argnames=["vm_type"],
        argvalues=[
            pytest.param(
                constants.VM_VOLUME_PVC,
                marks=pytest.mark.polarion_id(polarion_id_multiple_relocations),
                id="vm-pvc",
            ),
            pytest.param(
                constants.VM_VOLUME_DVT,
                marks=pytest.mark.polarion_id(polarion_id_multiple_relocations),
                id="vm-dvt",
            ),
        ],
    )
    def test_multiple_relocation_cycles(self, vm_type, fusionaccess_dr_workloads):
        """
        Relocate every kind of DR protected application back and forth between
        the two Fusion Access managed clusters five times in a row.

        Test steps:
            1. Deploy an ApplicationSet based CNV VM, a Discovered Apps based
               CNV VM, a Recipe protected Discovered App and a busybox
               Deployment application.
            2. Start a continuously running FIO workload inside every VM.
            3. Capture the initial data baseline and its checksums.
            4. Relocate all applications to the peer site.
            5. Validate the data integrity of everything written so far.
            6. Write additional data, record the new checksums and relocate
               again. Repeat until five relocations have been performed.
            7. Run a final data integrity validation over every file written
               during the test.
            8. Verify system stability, the absence of resource leaks and that
               RPO=0 was preserved throughout.

        """
        logger.test_step("Download the virtctl binary used to reach the VMs")
        CNVInstaller().download_and_extract_virtctl_binary()

        logger.test_step(
            "Deploy ApplicationSet VM, Discovered Apps VM, Recipe protected "
            "Discovered App and busybox Deployment applications"
        )
        apps = fusionaccess_dr_workloads(vm_type=vm_type)
        source_site = fa_dr_helpers.get_app_primary_cluster(apps[0])
        recovery_site = fa_dr_helpers.get_app_secondary_cluster(apps[0])
        managed_clusters = [source_site, recovery_site]
        logger.info(
            f"Fusion Access Metro-DR sites: source={source_site}, "
            f"recovery={recovery_site}"
        )

        logger.test_step("Start the continuously running IO workload on every VM")
        fa_dr_helpers.start_continuous_vm_io(apps)

        logger.test_step("Verify RPO=0 is in place before the first relocation")
        fa_dr_helpers.verify_rpo_zero(apps, stage="workload deployment")

        logger.test_step("Capture the initial data baseline and its checksums")
        data_baseline = fa_dr_helpers.DataBaseline()
        data_baseline.add_data(apps, file_path="/dd_file_baseline.txt")

        logger.test_step(
            "Record the baseline resource counts used for the resource leak check"
        )
        baseline_resource_snapshot = fa_dr_helpers.collect_resource_snapshot(
            apps, managed_clusters
        )

        for cycle in range(1, RELOCATION_CYCLES + 1):
            logger.test_step(
                f"Relocation {cycle}/{RELOCATION_CYCLES}: move every application "
                f"to its peer site"
            )
            current_site = fa_dr_helpers.relocate_dr_apps(apps, cycle)

            logger.test_step(
                f"Relocation {cycle}/{RELOCATION_CYCLES}: verify IO is still "
                f"running on {current_site}"
            )
            fa_dr_helpers.verify_continuous_vm_io(apps, stage=f"relocation {cycle}")

            logger.test_step(
                f"Data integrity check {cycle}: validate every file written so far"
            )
            data_baseline.verify(apps, stage=f"relocation {cycle}")

            logger.test_step(
                f"Relocation {cycle}/{RELOCATION_CYCLES}: verify RPO=0 is preserved"
            )
            fa_dr_helpers.verify_rpo_zero(apps, stage=f"relocation {cycle}")

            if cycle < RELOCATION_CYCLES:
                logger.test_step(
                    f"Generate additional data and record the new checksums after "
                    f"relocation {cycle}"
                )
                data_baseline.add_data(
                    apps, file_path=f"/dd_file_after_relocation_{cycle}.txt"
                )

        logger.test_step(
            f"Final validation: verify the integrity of all "
            f"{len(data_baseline.tracked_files)} files written during the test"
        )
        data_baseline.verify(apps, stage=f"{RELOCATION_CYCLES} relocations")

        logger.test_step("Final validation: verify every application is healthy")
        fa_dr_helpers.verify_workloads_healthy(
            apps, stage=f"{RELOCATION_CYCLES} relocations"
        )

        logger.test_step("Final validation: verify RPO=0 survived every relocation")
        fa_dr_helpers.verify_rpo_zero(apps, stage=f"{RELOCATION_CYCLES} relocations")

        logger.test_step("Final validation: verify no resources were leaked")
        final_resource_snapshot = fa_dr_helpers.collect_resource_snapshot(
            apps, managed_clusters
        )
        fa_dr_helpers.verify_no_resource_leaks(
            baseline_resource_snapshot,
            final_resource_snapshot,
            stage=f"{RELOCATION_CYCLES} relocations",
        )

        logger.test_step("Final validation: verify both managed clusters are stable")
        fa_dr_helpers.verify_system_stability(
            managed_clusters, stage=f"{RELOCATION_CYCLES} relocations"
        )
