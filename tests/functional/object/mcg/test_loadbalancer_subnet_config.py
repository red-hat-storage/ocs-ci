import json
import logging
from time import sleep

import pytest
import requests

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    aws_platform_required,
    mcg,
    polarion_id,
    red_squad,
    tier2,
)
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility.utils import exec_cmd

logger = logging.getLogger(__name__)


@mcg
@red_squad
@aws_platform_required
class TestLBSubnetConfig(MCGTest):

    @tier2
    @polarion_id("OCS-4716")
    def test_lb_subnet_config(self, awscli_pod):
        """
        Test whether MCG's load balancer subnet config allows connections
        to the load balancer only from the specified subnets:

        1. Test local connection to the load balancer
        2. Test connectivity from a pod to the load balancer
        3. Patch the local IP as the only subnet in noobaa/noobaa.spec.loadBalancerSourceSubnets
        4. Verify that the pod is no longer able to connect to the load balancer
        5. Verify that local connection to the load balancer is still available
        6. Revert the patch
        7. Verify that the pod is able to connect to the load balancer
        8. Verify that local connection to the load balancer is still available

        This test uses the awscli pod as a second subnet to test, but it can be
        any pod that can connect to the load balancer.
        """
        noobaa_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        local_ip = exec_cmd("curl -s https://ifconfig.me").stdout.decode().split(" ")[0]
        lb_host_name = OCP(
            kind="service",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="s3",
        ).get()["status"]["loadBalancer"]["ingress"][0]["hostname"]
        port = 443  # This is the default port for the load balancer

        # 1. Test local connection to the load balancer
        local_can_connect = self.test_connectivity(lb_host_name, port)
        assert local_can_connect, "Local host failed to connect to the load balancer"
        logger.info("Local host can connect to the load balancer")

        # 2. Test connectivity from the awscli pod to the load balancer
        pod_can_connect = self.test_connectivity(lb_host_name, port, pod_obj=awscli_pod)
        assert pod_can_connect, "Pod failed to connect to the load balancer"
        logger.info("Pod can connect to the load balancer")

        # 3. Patch the local IP as the only subnet in noobaa/noobaa.spec.loadBalancerSourceSubnets
        source_subnet = local_ip + "/32"  # /32 limits the subnet to only the given IP
        patch_params = {"spec": {"loadBalancerSourceSubnets": {"s3": [source_subnet]}}}
        noobaa_obj.patch(
            resource_name="noobaa", params=json.dumps(patch_params), format_type="merge"
        )
        noobaa_obj.reload_data()
        assert noobaa_obj.get(resource_name="noobaa")["spec"].get(
            "loadBalancerSourceSubnets"
        ).get("s3") == [
            source_subnet
        ], "Patch to load balancer subnet config was reconciled"
        sleep(10)  # Wait a bit for the change to take effect

        # 4. Verify that the pod is no longer able to connect to the load balancer
        pod_can_connect = self.test_connectivity(lb_host_name, port, pod_obj=awscli_pod)
        assert (
            not pod_can_connect
        ), "Pod should no longer be able to connect to the load balancer"
        logger.info("Pod couldn't connect to the load balancer as expected")

        # 5. Verify that local connection to the load balancer is still available
        local_can_connect = self.test_connectivity(lb_host_name, port)
        assert local_can_connect, "Local host failed to connect to the load balancer"
        logger.info("Local host can still connect to the load balancer")

        # 6. Revert the patch
        self.revert_lb_subnet_config()

        # 7. Verify that the pod is able to connect to the load balancer
        pod_can_connect = self.test_connectivity(lb_host_name, port, pod_obj=awscli_pod)
        assert pod_can_connect, "Pod failed to connect to the load balancer"
        logger.info("Pod can connect again to the load balancer after revert")

        # 8. Verify that local connection to the load balancer is still available
        local_can_connect = self.test_connectivity(lb_host_name, port)
        assert local_can_connect, "Local host failed to connect to the load balancer"
        logger.info("Local host can still connect to the load balancer after revert")

    def test_connectivity(self, host, port, pod_obj=None):
        """
        Test connectivity to a given host and port.

        Args:
            pod_obj (Pod|None): Pod object to execute the command on.
            If None, the command will be executed on the local host.

            host (str): Host to test.
            port (int): Port to test.

        Returns:
            bool: True if connectivity is successful, False otherwise.
        """
        timeout = 10  # setting timeouts prevents unsuccessful connections from hanging for too long
        try:
            if pod_obj:  # Test pod connectivity via netcat
                pod_obj.exec_cmd_on_pod(f"timeout 10 nc -z {host} {port}")

            else:  # Test local host connectivity via an HTTPS request
                requests.get(f"https://{host}:{port}", verify=False, timeout=timeout)

            # If we get here, no exception was raised, so a connection was established
            return True
        except (CommandFailed, requests.exceptions.Timeout):
            return False

    def revert_lb_subnet_config(self):
        """
        Remove the load balancer subnet config from the noobaa CRD:w
        """
        noobaa_obj = OCP(
            kind="noobaa",
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        patch_params = {"spec": {"loadBalancerSourceSubnets": None}}
        noobaa_obj.patch(
            resource_name="noobaa",
            params=json.dumps(patch_params),
            format_type="merge",
        )
        noobaa_obj.reload_data()
        assert (
            noobaa_obj.get(resource_name="noobaa")["spec"].get(
                "loadBalancerSourceSubnets"
            )
            is None
        ), "Patch to load balancer subnet config was reverted"
        sleep(10)  # Wait a bit for the change to take effect

    @pytest.fixture(scope="class", autouse=True)
    def cleanup(self, request):
        """
        Make sure that any change to the load balancer subnet config is reverted
        """
        request.addfinalizer(self.revert_lb_subnet_config)
