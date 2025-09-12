"""
Test to run PGSQL performance marker workload
"""

import logging

import pytest
import os

from pathlib import Path
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.pod import (
    wait_for_pods_to_be_in_statuses,
    get_pod_name_by_pattern,
)
from ocs_ci.framework.pytest_customization.marks import grey_squad, rdr, performance
from ocs_ci.ocs.resources import pvc, pod
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.utils import get_primary_cluster_index
from ocs_ci.ocs.dr.dr_workload import (
    BusyboxDiscoveredAppsPerformance,
)
from ocs_ci.ocs import constants
from ocs_ci.framework import config


log = logging.getLogger(__name__)


@grey_squad
@rdr
@performance
class TestRDRPerformance:
    @pytest.mark.parametrize(
        argnames=["server", "size"],
        argvalues=[
            pytest.param(
                *[constants.storage_type_block, "5", "200", "10G"],
                marks=pytest.mark.polarion_id("OCS-XXX"),
            ),
        ],
    )
    def test_regional_dr_discovered_apps_performance(self, server, size):
        """
        Test case to capture RDR discovered apps performance test using simple-fio tool

        """
        # We need to provide placemnt name for DR workload, which inturn requires drpc_name

        drpc_name = "simple-fio-drpc"
        pn_namespace = [constants.simple_fio_namespace]

        # Deployment of simple-fio tool
        log.info("Deploying the simple-fio tool")
        initial_cluster_index = get_primary_cluster_index()
        config.switch_ctx(initial_cluster_index)

        kubeconfig = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN.get("kubeconfig_location")
        )

        resolved_path = Path(kubeconfig).resolve()

        try:
            cmd = f'podman run -v "{resolved_path}:/tmp/kubeconfig:Z" -e KUBECONFIG=/tmp/kubeconfig --rm  {constants.simple_fio_image} ./01_prepare_setup.sh server={server}'
            exec_cmd(cmd, timeout=9000)

        except Exception as ex:
            log.error(f"Failed to deploy simple-fio tool : {ex}")

        log.info("Wait for the prefill to be completed")
        pod_name = get_pod_name_by_pattern("prefill", constants.simple_fio_namespace)

        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_COMPLETED,
            pod_names=pod_name,
            timeout=900,
            namespace=constants.simple_fio_namespace,
        )

        pvc_objs = pvc.get_all_pvc_objs(namespace=constants.simple_fio_namespace)

        all_pods = pod.get_all_pods()

        # Label the PVCs with fio label
        for pvc_obj in pvc_objs:
            exec_cmd(
                f"oc label pvc {pvc_obj.name} -n {constants.simple_fio_namespace} {constants.simple_fio_label_key}={constants.simple_fio_label_value} --overwrite"
            )
        for pod_obj in all_pods:
            exec_cmd(
                f"oc label pod {pod_obj.name} -n {constants.simple_fio_namespace} {constants.simple_fio_label_key}={constants.simple_fio_label_value} --overwrite"
            )

        # Create dummy placement name
        obj = BusyboxDiscoveredAppsPerformance()
        placement_name = drpc_name + "-placement-1"
        try:
            obj.create_placement(placement_name=placement_name)

            # Create a DRPC
            obj.create_drpc(
                drpc_name="simple-fio1",
                placement_name=placement_name,
                protected_namespaces=pn_namespace,
                pvc_selector_key=constants.simple_fio_label_key,
                pvc_selector_value=constants.simple_fio_label_value,
                pod_selector_key=constants.simple_fio_label_key,
                pod_selector_value=constants.simple_fio_label_value,
            )
        except CommandFailed:
            log.info("Placement and DRPC already exists")
        # Run the fio command
        cmd = f'podman run -v "{resolved_path}:/tmp/kubeconfig:Z" -e KUBECONFIG=/tmp/kubeconfig --rm {constants.simple_fio_image} ./02_run_tests.sh server={server} size={size}'
        exec_cmd(cmd, timeout=900)

        config.switch_ctx(initial_cluster_index)

        client_pod_names = ["fio-client"]

        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_COMPLETED,
            pod_names=client_pod_names,
            timeout=3600,
            namespace=constants.simple_fio_namespace,
        )

        # Extract the output if FIO is completed
        cmd = f'podman run -v "{resolved_path}:/tmp/kubeconfig:Z" -e KUBECONFIG=/tmp/kubeconfig --rm {constants.simple_fio_image} ./extract_data.sh server={server}'
        output = exec_cmd(cmd, timeout=9000)
        result = output.stdout.decode("utf-8", errors="replace")
        lines = result.splitlines()

        data = [
            line for line in lines if "|" in line and not line.startswith("start_time|")
        ]

        for row in data:
            values = [v.strip() for v in row.split("|")]
            filtered = [v for v in values if v not in (0, "", None)]
            x_vals = [float(v) for v in filtered[-3:]]
            x_avg = sum(x_vals) / len(x_vals)

            y_vals = [float(v) for v in filtered[-6:-3]]
            y_avg = sum(y_vals) / len(y_vals)

            log.info(x_avg)
            log.info(y_avg)
