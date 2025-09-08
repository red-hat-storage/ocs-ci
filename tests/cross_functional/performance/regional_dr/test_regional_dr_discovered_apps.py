"""
Test to run PGSQL performance marker workload
"""

import logging
from time import sleep

import pytest
from ocs_ci.ocs.perftests import PASTest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.dr.dr_workload import BusyboxDiscoveredApps, CnvWorkloadDiscoveredApps
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.framework import config


log = logging.getLogger(__name__)


@grey_squad
@rdr
@performance
class RDRPerformance(PASTest):
    """
    RDR discovered apps performance test using simple-fio tool

    """

    @pytest.mark.parametrize(
        argnames=["storage_type", "server", "storage", "size"],
        argvalues=[
            pytest.param(
                *[constants.storage_type_block, "5", "200", "10G"],
                marks=pytest.mark.polarion_id("OCS-XXX"),
            ),
            pytest.param(
                *[constants.storage_type_cephfs, "5", "200", "10G"],
                marks=pytest.mark.polarion_id("OCS-XXX"),
            ),
        ],
    )
    def test_regional_dr_discovered_apps_performance(self, interface, server):
        """
        Test case to capture RDR discovered apps performance test using simple-fio tool

        """
        # We need to provide placemnt name for DR workload, which inturn requires drpc_name
        drpc_name = "simple-fio-drpc"
        # Deployment of simple-fio tool
        log.info("Deploying the simple-fio tool")

        primary_cluster_name = dr_helpers.get_current_primary_cluster_name(
            constants.simple_fio_namespace,
        )

        config.switch_to_cluster_by_name(primary_cluster_name)
        kubeconfig = config.RUN.get("kubeconfig")
        try:
            cmd = f"podman run --rm -e KUBECONFIG={kubeconfig} {constants.simple_fio_image} server={server} "
            exec_cmd(cmd, timeout=9000)

        except Exception as ex:
            log.error(f"Failed to deploy simple-fio tool : {ex}")

        log.info("Wait for the prefill to be completed")

        pvc_objs = pvc.get_all_pvc_objs(namespace=constants.simple_fio_namespace)

        # Label the PVCs with fio label
        for pvc_obj in pvc_objs:
            exec_cmd(
                f"oc label {pvc_obj.name} -n {constants.simple_fio_namespace} {constants.simple_fio_label_key}={constants.simple_fio_label_value}"
            )

        # Create dummy placement name
        discovered_apps_placement_name = drpc_name
        CnvWorkloadDiscoveredApps.create_placement(
            placement_name=discovered_apps_placement_name
        )

        # Create a DRPC
        BusyboxDiscoveredApps.create_drpc(
            drpc_name="simple-fio",
            placement_name=discovered_apps_placement_name,
            protected_namespaces=constants.simple_fio_namespace,
            pvc_selector_key=constants.simple_fio_label_key,
            pvc_selector_value=constants.simple_fio_label_value,
        )

        # Run the fio command
        cmd = f"podman run --rm -e KUBECONFIG={kubeconfig} {constants.simple_fio_image} ./02_run_tests.sh server={server} size={size} "
        exec_cmd(cmd, timeout=9000)

        # Wait for FIO to be completed
        self.wait_for_wl_to_finish(self, namespace=constants.simple_fio_namespace)

        # Extract the output if FIO is completed
        cmd = f"podman run --rm -e KUBECONFIG={kubeconfig} {constants.simple_fio_image} ./extract_data.sh server={server} "
        output = exec_cmd(cmd, timeout=9000)

        for line in output:
            # Split by '|' and strip whitespace
            out = [p.strip() for p in line.strip().split("|") if p.strip()]
            for part in out:
                if "randrw" in part:
                    operation = "randrw"
                    break
                elif "randread" in part:
                    operation = "randread"
                    break
                elif "randwrite" in part:
                    operation = "randwrite"
                    break
            if out[-1] and out[-2]:
                # Get last two values
                data = out[-2:]
            else:
                pass
            log.info(f"Latency and throughput for operation {operation}: {data}")
            log.info(f"Latency for  {operation}: {data[0]}")
            log.info(f"Throughput for  {operation}: {data[1]}")
