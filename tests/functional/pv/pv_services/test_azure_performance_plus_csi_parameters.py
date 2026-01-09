"""
Test to verify Azure Performance Plus CSI volume parameters.

This test verifies that when Azure Performance Plus is enabled,
OSD PVCs are created with the correct CSI volume parameters,
specifically that enablePerformancePlus=true is present in the
PV volume attributes.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    azure_platform_required,
    azure_performance_plus_required,
    polarion_id,
    green_squad,
    runs_on_provider,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs

log = logging.getLogger(__name__)

# Expected CSI volume attribute for Performance Plus
PERFPLUS_CSI_ATTRIBUTE = "enablePerformancePlus"
PERFPLUS_CSI_ATTRIBUTE_VALUE = "true"


@tier1
@green_squad
@azure_platform_required
@azure_performance_plus_required
class TestAzurePerformancePlusCSIParameters(ManageTest):
    """
    Test class to verify Azure Performance Plus CSI volume parameters.

    This test verifies that OSD PVCs created with Performance Plus
    storage class have the correct CSI volume attributes set,
    specifically enablePerformancePlus=true.
    """

    @polarion_id("OCS-7413")
    @runs_on_provider
    def test_verify_performance_plus_csi_volume_parameters(self):
        """
        Verify that OSD PVCs have enablePerformancePlus=true in PV CSI volume attributes.

        Steps:
            1. Get all OSD PVCs from the cluster
            2. For each OSD PVC:
               a. Verify the PVC is using the Performance Plus storage class
               b. Get the backed PV object
               c. Verify the PV has enablePerformancePlus=true in CSI volumeAttributes

        Expected:
            - All OSD PVCs should use the Performance Plus storage class
            - All OSD PVs should have enablePerformancePlus=true in volumeAttributes
        """
        log.info(
            "Starting verification of Azure Performance Plus CSI volume parameters"
        )

        # Get all OSD PVCs
        osd_pvc_objs = get_all_pvc_objs(
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=constants.OSD_PVC_GENERIC_LABEL,
        )

        assert osd_pvc_objs, "No OSD PVCs found in the cluster"

        log.info(f"Found {len(osd_pvc_objs)} OSD PVCs to verify")

        # Verify each OSD PVC
        for osd_pvc_obj in osd_pvc_objs:
            pvc_name = osd_pvc_obj.name
            log.info(f"Verifying OSD PVC: {pvc_name}")

            # Verify storage class
            pvc_storage_class = (
                osd_pvc_obj.get().get("spec", {}).get("storageClassName")
            )
            log.info(
                f"PVC {pvc_name} storage class: {pvc_storage_class}, "
                f"expected: {constants.AZURE_PERFORMANCE_PLUS_STORAGECLASS}"
            )

            assert pvc_storage_class == constants.AZURE_PERFORMANCE_PLUS_STORAGECLASS, (
                f"OSD PVC {pvc_name} is not using Performance Plus storage class. "
                f"Expected: {constants.AZURE_PERFORMANCE_PLUS_STORAGECLASS}, Actual: {pvc_storage_class}"
            )

            # Get the backed PV object
            pv_obj = osd_pvc_obj.backed_pv_obj
            pv_name = pv_obj.name
            log.info(f"Verifying PV: {pv_name} for PVC: {pvc_name}")

            # Get PV CSI volume attributes
            pv_data = pv_obj.get()
            csi_spec = pv_data.get("spec", {}).get("csi", {})
            volume_attributes = csi_spec.get("volumeAttributes", {})

            log.info(
                f"PV {pv_name} CSI volume attributes: {list(volume_attributes.keys())}"
            )

            # Verify enablePerformancePlus is present and set to true
            assert PERFPLUS_CSI_ATTRIBUTE in volume_attributes, (
                f"PV {pv_name} does not have {PERFPLUS_CSI_ATTRIBUTE} "
                f"in CSI volumeAttributes. Available attributes: {list(volume_attributes.keys())}"
            )

            perf_opt_value = volume_attributes.get(PERFPLUS_CSI_ATTRIBUTE)
            log.info(
                f"PV {pv_name} {PERFPLUS_CSI_ATTRIBUTE} value: {perf_opt_value}, "
                f"expected: {PERFPLUS_CSI_ATTRIBUTE_VALUE}"
            )

            assert perf_opt_value == PERFPLUS_CSI_ATTRIBUTE_VALUE, (
                f"PV {pv_name} has {PERFPLUS_CSI_ATTRIBUTE}={perf_opt_value}, "
                f"expected {PERFPLUS_CSI_ATTRIBUTE_VALUE}"
            )

            log.info(
                f"Successfully verified PV {pv_name} has "
                f"{PERFPLUS_CSI_ATTRIBUTE}={PERFPLUS_CSI_ATTRIBUTE_VALUE}"
            )

        log.info(
            f"Successfully verified all {len(osd_pvc_objs)} OSD PVCs have "
            f"Performance Plus CSI parameters configured correctly"
        )
