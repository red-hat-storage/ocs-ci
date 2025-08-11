"""
Test to run PGSQL performance marker workload
"""
import logging
from time import sleep

import pytest

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import grey_squad
from ocs_ci.utility.utils import exec_cmd
from ocs_ci.ocs.resources.pvc import get_pvc_objs
from ocs_ci.ocs.dr.dr_workload import BusyboxDiscoveredApps
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants


log = logging.getLogger(__name__)



@grey_squad
@rdr
@performance
class RDRPerformance:
    """
    RDR discovered apps performance test using simple-fio tool

    """

    @pytest.mark.parametrize(
        argnames=["interface", "server"],
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 5],
                marks=pytest.mark.polarion_id("OCS-844"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 5],
                marks=pytest.mark.polarion_id("OCS-845"),
            ),
            pytest.param(
                *[constants.CEPHBLOCKPOOL, 10],
                marks=pytest.mark.polarion_id("OCS-846"),
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM, 10],
                marks=pytest.mark.polarion_id("OCS-847"),
            ),
        ],
    )
    def test_regional_dr_discovered_apps_performance(self, interface, server):
        """
        Test case to capture RDR discovered apps performance test using simple-fio tool

        """
        # Deployment of simple-fio tool
        log.info("Deploying the simple-fio tool")
        simple_fio_image = "quay.io/ocsci/simple-fio:latest"
        drpc_name = "fio_25"
        kubeconfig = config.RUN.get("kubeconfig")
        namespace = "simple-fio"
        label_key = "app"
        label_value = "fio"
        try:
            cmd = (
                f"podman run --rm -e KUBECONFIG={kubeconfig} {simple_fio_image} server={} "
            )
            exec_cmd(cmd, timeout=9000)

        except Exception as ex:
            log.error(f"Failed to deploy simple-fio tool : {ex}")

        log.info("Wait for the prefill to be completed")

        pvc_objs = get_pvc_objs(pvc_names=pvc_list, namespace="simple-fio")
        for pvc_obj in pvc_objs:
            run_cmd(
                f"oc label {pvc_obj.name} -n {namespace} {label_key}={label_value}"
            )
        BusyboxDiscoveredApps.create_drpc(
            drpc_name=drpc_name,
            placement_name=placement_name,
            protected_namespaces=namespace,
            pvc_selector_key=label_key,
            pvc_selector_value=label_value,
        )
        cmd = (
            f"podman run --rm -e KUBECONFIG={kubeconfig} {simple_fio_image} ./02_run_tests.sh server={} "
        )
        exec_cmd(cmd, timeout=9000)









