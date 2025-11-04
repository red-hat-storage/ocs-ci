"""
Test to run PGSQL performance marker workload
"""

import logging
import time
import yaml
import re

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
#from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs import constants
from ocs_ci.framework import config
from ocs_ci.utility.utils import run_cmd

log = logging.getLogger(__name__)


@grey_squad
@rdr
@performance
class TestRDRPerformance:

    def parse_fio_line(line):
        parts = [p.strip() for p in line.split("|") if p.strip()]
        result = {}

        if len(parts) >= 6:
            result["start_time"] = parts[0]
            result["end_time"] = parts[1] if "UTC" in parts[1] else None
            result["server"] = (
                re.search(r"\d+", parts[2]).group() if "Server=" in parts[2] else None
            )
            result["workload"] = parts[3] if "rand" in parts[3] else None
            result["bs"] = parts[4]
            result["numjobs"] = (
                re.search(r"\d+", parts[5]).group() if "Numjobs=" in parts[5] else None
            )
            result["iodepth"] = (
                re.search(r"\d+", parts[6]).group() if "IOdepth=" in parts[6] else None
            )

        # Optional IOPS and Latency
        if len(parts) >= 8:
            result["iops"] = parts[7].strip()
        if len(parts) >= 9:
            result["latency"] = parts[8].strip()

        return result

    @pytest.mark.parametrize(
        argnames=["server", "size"],
        argvalues=[
            pytest.param(
                *[constants.storage_type_block, "5"],
                marks=pytest.mark.polarion_id("OCS-XXX"),
            ),
        ],
    )
    def test_regional_dr_discovered_apps_performance(self, server, size):
        """
        Test case to capture RDR discovered apps performance test using simple-fio tool

        """
        # We need to provide placemnt name for DR workload, which in turn requires drpc_name

        drpc_name = "simple-fio-drpc"
        pn_namespace = [constants.simple_fio_namespace]

        server = 2
        total_duration = 900
        run_duration = 300
        last_group_sync_time = []
        last_group_sync_duration = []

        run_nos = total_duration / run_duration

        # Deployment of simple-fio tool
        log.info("Deploying the simple-fio tool")
        initial_cluster_index = get_primary_cluster_index()
        config.switch_ctx(initial_cluster_index)

        kubeconfig = os.path.join(
            config.ENV_DATA["cluster_path"], config.RUN.get("kubeconfig_location")
        )

        resolved_path = Path(kubeconfig).resolve()

        pvc_objs = pvc.get_all_pvc_objs(namespace=constants.simple_fio_namespace)

        all_pods = pod.get_all_pods(namespace=constants.simple_fio_namespace)

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
            drpc_obj = obj.create_drpc(
                drpc_name="simple-fio1",
                placement_name=placement_name,
                protected_namespaces=pn_namespace,
                pvc_selector_key=constants.simple_fio_label_key,
                pvc_selector_value=constants.simple_fio_label_value,
                pod_selector_key=constants.simple_fio_label_key,
                pod_selector_value=constants.simple_fio_label_value,
            )
            log.info(f"ddd{drpc_obj}")
        except CommandFailed:
            log.info("Placement and DRPC already exists")

        # Run the fio command
        cmd = (
            f'podman run -v "{resolved_path}:/tmp/kubeconfig:Z" -e KUBECONFIG=/tmp/kubeconfig --rm '
            f'{constants.simple_fio_image} ./02_run_tests.sh server={server} size={size}'
        )
        exec_cmd(cmd, timeout=1500)

        data = yaml.safe_load(
            run_cmd("oc get drpc -o wide -A --all-namespaces -o yaml")
        )
        log.info(data)
        name, pol_name = (
            data["items"][0]["metadata"]["name"],
            data["items"][0]["spec"]["drPolicyRef"]["name"],
        )
        log.info(name)
        drpc_pol_data = yaml.safe_load(
            run_cmd(
                f"oc --kubeconfig ../hub/auth/kubeconfig -n openshift-dr-ops get DRPolicy {pol_name} -o yaml"
            )
        )
        scheduling_interval = drpc_pol_data["spec"]["schedulingInterval"]
        log.info(scheduling_interval)

        config.switch_ctx(initial_cluster_index)

        client_pod_name = get_pod_name_by_pattern(
            "fio-client", constants.simple_fio_namespace
        )
        time.sleep(300)
        wait_for_pods_to_be_in_statuses(
            expected_statuses=constants.STATUS_COMPLETED,
            pod_names=client_pod_name,
            timeout=1800,
            namespace=constants.simple_fio_namespace,
        )

        # Extract the output if FIO is completed
        cmd = (
            f'podman run -v "{resolved_path}:/tmp/kubeconfig:Z" -e KUBECONFIG=/tmp/kubeconfig --rm '
            f'{constants.simple_fio_image} ./extract_data.sh server={server}'
        )
        with open("extract_data.txt", "w") as f:
            f.write(cmd + "\n")

        # Read from file
        with open("extract_data.txt") as f:
            lines = f.readlines()

        parsed_data = []
        for line in lines:
            if "UTC" in line and "Server=" in line:
                parsed_data.append(self.parse_fio_line(line))

        for iteration in {1, run_nos}:
            cmd = (
                f'podman run -v "{resolved_path}:/tmp/kubeconfig:Z" -e KUBECONFIG=/tmp/kubeconfig --rm '
                f'{constants.simple_fio_image} ./02_run_tests.sh server={server} size={size}'
            )
            exec_cmd(cmd, timeout=300)

            drpc_cd = run_cmd(
                "oc get drpc -n openshift-dr-ops -o yaml |grep -i lastGroupSyncTime"
            )
            data = yaml.safe_load(drpc_cd)
            last_group_sync_duration.append(
                data["items"][0]["status"]["lastGroupSyncDuration"]
            )
            last_group_sync_time.append(data["items"][0]["status"]["lastGroupSyncTime"])

            log.info(f"ddddd{last_group_sync_time}")
            log.info(f"ddddd{last_group_sync_duration}")
            wait_for_pods_to_be_in_statuses(
                expected_statuses=constants.STATUS_COMPLETED,
                pod_names=client_pod_name,
                timeout=3600,
                namespace=constants.simple_fio_namespace,
            )

            cmd = (
                f'podman run -v "{resolved_path}:/tmp/kubeconfig:Z" -e KUBECONFIG=/tmp/kubeconfig --rm '
                f'{constants.simple_fio_image} ./extract_data.sh server={server}'
            )
            with open(f"extract_data{iteration}.txt", "w") as f:
                f.write(cmd + "\n")

            with open(f"extract_data{iteration}.txt") as f:
                lines = f.readlines()

            parsed_data = []
            for line in lines:
                if "UTC" in line and "Server=" in line:
                    parsed_data.append(self.parse_fio_line(line))
