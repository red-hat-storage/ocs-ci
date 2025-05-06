import pytest
import logging

from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.constants import CSI_RBDPLUGIN_LABEL_419
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    green_squad,
)
from ocs_ci.framework.pytest_customization.marks import polarion_id, skipif_ocs_version

log = logging


@tier1
@green_squad
@polarion_id("OCS-6807")
@skipif_ocs_version("<4.19")
class TestCSIAddonPodSecurity:
    """This class contains tests to Validate if CSI Addon pod enforces security
    by allowing HTTPS and rejecting HTTP connections.
    """

    def test_csi_addon_pod_security(self):
        """
        Validate that the CSI Addon pods are compliant with the Pod Security Standards.

        Test Steps:
        1. Fetch a pod with label 'app=openshift-storage.rbd.csi.ceph.com-nodeplugin' (CSI RBD Nodeplugin).
        2. Retrieve container information for the container named 'csi-addons'.
        3. Assert that the 'csi-addons' container exists in the pod.
        4. Extract the port used by the 'csi-addons' container.
        5. Execute a HTTPS (secure) curl command inside the 'csi-addons' container on localhost:{port}/healthz.
           - Verify that the pod responds correctly over HTTPS (secure connection should succeed).
        6. Execute a HTTP (insecure) curl command inside the 'csi-addons' container on localhost:{port}/healthz.
           - Verify that the insecure connection fails as expected (CommandFailed exception raised).
        7. Assert that the CSI Addon pod does not allow connections without TLS (insecure HTTP).

        Expected Result:
        - The pod should be reachable securely over HTTPS.
        - The pod should reject insecure HTTP (non-TLS) connections.
        """

        log.info("Validating CSI Addon pod security standards")

        pod_obj = Pod(**get_pods_having_label(CSI_RBDPLUGIN_LABEL_419)[0])

        csi_addon_container = pod_obj.get_container_data("csi-addons")

        assert csi_addon_container, "No CSI Addon container found in the pod"

        port_used_by_csi_addon = csi_addon_container[0]["ports"][0]["containerPort"]

        # Querying to the container port with HTTPS
        try:
            pod_obj.exec_cmd_on_pod(
                command=f"curl -k -s https://localhost:{port_used_by_csi_addon}/healthz",
                container_name="csi-addons",
                out_yaml_format=False,
            )
            log.info(
                f"CSI Addon pod is reachable securely on port {port_used_by_csi_addon}"
            )
        except CommandFailed as e:
            log.error(
                f"CSI Addon pod is not reachable securely on port {port_used_by_csi_addon}: {str(e)}"
            )
            pytest.fail(f"CSI Addon pod HTTPS connection failed: {str(e)}")

        # Now check if the pod is rejecting insecure HTTP (without TLS)
        with pytest.raises(CommandFailed) as exc_info:
            pod_obj.exec_cmd_on_pod(
                command=f"curl -s http://localhost:{port_used_by_csi_addon}/healthz",
                container_name="csi-addons",
                out_yaml_format=False,
            )

        assert "command terminated" in str(
            exc_info.value
        ), "CSI Addon pod should not allow connection without TLS"
        log.info("CSI Addon pod correctly refused HTTP (insecure) connection")
