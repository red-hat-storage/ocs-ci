import logging
import requests
import pytest

from time import sleep
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import get_noobaa_metrics_token_from_secret
from ocs_ci.ocs.resources.pod import (
    get_noobaa_endpoint_pods,
    get_pod_logs,
    wait_for_pods_to_be_running,
    get_noobaa_core_pod,
)
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    red_squad,
    mcg,
    post_upgrade,
)

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def reset_configmap(request):
    """
    resets noobaa metrics configmap values to original values
    """
    noobaa_configmap = OCP(
        kind="configmap",
        namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        resource_name=constants.NOOBAA_CONFIGMAP,
    )

    def reset_flag():
        params = '{"data": {"NOOBAA_VERSION_AUTH_ENABLED": "true", "NOOBAA_METRICS_AUTH_ENABLED": "true"}}'
        noobaa_configmap.patch(params=params, format_type="merge")

    request.addfinalizer(reset_flag)


@mcg
@red_squad
class TestNoobaaRouteAuthentication:
    """
    Tests noobaa management route and noobaa endpoints with JWT authentication
    """

    def set_nb_config_flag(self, flag_param, value):
        """
        Sets the value for a given config flag in noobaa-config.


            Args:
                flag_param (str): The config flag key to modify.
                value (str): Desired value for the flag.
        """
        noobaa_configmap = OCP(
            kind="configmap",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.NOOBAA_CONFIGMAP,
        )
        params = f'{{"data": {{"{flag_param}": "{value}"}}}}'
        noobaa_configmap.patch(params=params, format_type="merge")

    def get_endpoint_host_ip_port(self):
        """
        returns endpoint host ip and node port associated with it
        """
        noobaa_endpoint_pod = get_noobaa_endpoint_pods()[0]
        endpoint_host_ip = noobaa_endpoint_pod.get().get("status")["hostIP"]
        logger.info(endpoint_host_ip)
        s3_service_data = OCP(
            kind="service",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name="s3",
        ).get()
        all_port_info = s3_service_data["spec"]["ports"]
        port = None
        for port_info in all_port_info:
            if port_info.get("name") == "metrics":
                port = port_info.get("nodePort")
        assert port is not None, "Metrics Port info not found in s3 service"
        return f"http://{endpoint_host_ip}:{port}"

    def get_mgmt_route_address(self):
        """
        returns noobaa management route address
        """
        s3_route_data = OCP(
            kind="route",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name="noobaa-mgmt",
        ).get()
        return f"https://{s3_route_data['spec']['host']}/version"

    def access_url(self, url, header=None, verify=False):
        """
        Executes curl request to the given url with or without header.

            Args:
                url (str): The target URL to request
                header (dict, optional): A dictionary of HTTP headers to include in the request.
                    Defaults to None.
                verify (bool, optional): Whether to verify the server's TLS/SSL certificate.
                    Defaults to False.

            Returns:
                status code (int) : The HTTP status code.
        """
        response = requests.get(url, headers=header, verify=verify)
        logger.info(response)
        return response.status_code

    @post_upgrade
    @tier2
    def test_mgmt_and_endpoint_route_authentication(self):
        """
        This test covers validation of the below steps
            1. Check noobaa-metrics-auth-secret created or not
            2. Validate below values are set to true in noobaa-config configmap
                 i. NOOBAA_VERSION_AUTH_ENABLED: True
                ii. NOOBAA_METRICS_AUTH_ENABLED: True
            3. Validate Noobaa Mgmt route version using auth token
            4. Validate Noobaa Mgmt route version without using auth token
            5. Validate Noobaa Mgmt route version with incorrect auth token
            6. Validate noobaa endpoint host address + port using auth token
            7. Validate noobaa endpoint host address + port without using auth token
            8. Validate noobaa endpoint host address + port with incorrect auth token
        """

        json_web_token = get_noobaa_metrics_token_from_secret()
        valid_headers = {"Authorization": f"Bearer {json_web_token}"}
        invalid_headers = {"Authorization": f"Bearer {json_web_token}_incorrect"}

        # Check noobaa-metrics-auth-secret created or not
        ocp_secret_obj = OCP(
            kind="secret", namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
        )
        assert ocp_secret_obj.is_exist(
            resource_name=constants.NOOBAA_METRICS_AUTH_SECRET
        ), f"{constants.NOOBAA_METRICS_AUTH_SECRET} not found..."

        # Validate below values are set to true in noobaa-config configmap
        noobaa_configmap = OCP(
            kind="configmap",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=constants.NOOBAA_CONFIGMAP,
        )
        cm_data = noobaa_configmap.get()["data"]
        for key in ["NOOBAA_METRICS_AUTH_ENABLED", "NOOBAA_VERSION_AUTH_ENABLED"]:
            assert (
                cm_data.get(key) == "true"
            ), f"Incorrect default value of {key}. Received value {cm_data.get(key)}"

        # Validate Noobaa Mgmt route version using auth token
        mgmt_address = self.get_mgmt_route_address()
        response_code = self.access_url(
            mgmt_address, header=valid_headers, verify=False
        )
        assert (
            response_code == 200
        ), f"Expected response code 200. Received {response_code}"

        # Validate Noobaa Mgmt route version without using auth token
        response_code = self.access_url(mgmt_address, verify=False)
        assert (
            response_code == 401
        ), f"Expected response code 401. Received {response_code}"

        # Validate Noobaa Mgmt route version with incorrect auth token
        response_code = self.access_url(
            mgmt_address, header=invalid_headers, verify=False
        )
        assert (
            response_code == 403
        ), f"Expected response code 403. Received {response_code}"

        # Validate noobaa endpoint host address + port using auth token
        ep_url = self.get_endpoint_host_ip_port()
        response_code = self.access_url(ep_url, header=valid_headers, verify=False)
        assert (
            response_code == 200
        ), f"Expected response code 200. Received {response_code}"

        # Validate noobaa endpoint host address + port without using auth token
        response_code = self.access_url(ep_url, verify=False)
        assert (
            response_code == 401
        ), f"Expected response code 401. Received {response_code}"

        # Validate noobaa endpoint host address + port with incorrect auth token
        response_code = self.access_url(ep_url, header=invalid_headers, verify=False)
        assert (
            response_code == 403
        ), f"Expected response code 403. Received {response_code}"

    @pytest.mark.parametrize(
        argnames="flag",
        argvalues=[
            pytest.param("NOOBAA_VERSION_AUTH_ENABLED"),
            pytest.param("NOOBAA_METRICS_AUTH_ENABLED"),
        ],
        ids=[
            "NOOBAA_VERSION_AUTH_ENABLED",
            "NOOBAA_METRICS_AUTH_ENABLED",
        ],
    )
    @tier2
    def test_noobaa_configmap_flag(self, reset_configmap, flag):
        """
        This test covers validation of the below steps
            1. Validate below values are set to true in noobaa-config configmap
                 i. NOOBAA_VERSION_AUTH_ENABLED: True
                ii. NOOBAA_METRICS_AUTH_ENABLED: True
            2. Validate respective routes using auth token
            3. Change flag value to "False"
            4. Validate respective routes without using auth token
            5. revert value to original
        """

        json_web_token = get_noobaa_metrics_token_from_secret()
        auth_headers = {"Authorization": f"Bearer {json_web_token}"}
        if flag == "NOOBAA_VERSION_AUTH_ENABLED":
            test_url = self.get_mgmt_route_address()
        else:
            test_url = self.get_endpoint_host_ip_port()

        # Validate given flag value is set to true in noobaa-config configmap
        noobaa_configmap = OCP(
            kind="configmap",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name=constants.NOOBAA_CONFIGMAP,
        )
        cm_data = noobaa_configmap.get()["data"]
        assert (
            cm_data.get(flag) == "true"
        ), f"Incorrect default value of {flag}. Received value {cm_data.get(flag)}"

        # Validate respective routes using auth token
        response_code = self.access_url(test_url, header=auth_headers, verify=False)
        assert (
            response_code == 200
        ), f"Expected response code 200. Received {response_code}"

        # Change flag value to "False"
        self.set_nb_config_flag(flag, "False")

        # Validate respective routes without using auth token
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.access_url,
            url=test_url,
            verify=False,
        )
        sample.wait_for_func_value(200)

    @tier2
    def test_invalid_access_entries_in_core_log(self):
        """
        This test covers validation of the below steps
            1. Access mgmt route without using auth token
            2. Validate invalid access is recorded in core pod log
        """
        # Delete existing core pod to clear logs
        noobaa_core_pod = get_noobaa_core_pod()
        noobaa_core_pod.delete(force=True)
        wait_for_pods_to_be_running(pod_names=[noobaa_core_pod.name])

        # Trigger invalid access request
        json_web_token = get_noobaa_metrics_token_from_secret()
        invalid_headers = {"Authorization": f"Bearer {json_web_token}_incorrect"}
        mgmt_address = self.get_mgmt_route_address()
        sample = TimeoutSampler(
            timeout=120,
            sleep=10,
            func=self.access_url,
            header=invalid_headers,
            url=mgmt_address,
            verify=False,
        )
        sample.wait_for_func_value(403)

        # Wait for 30 seconds before checking logs
        sleep(30)

        # verify invalid access is recorded in core pod.
        pod_logs = get_pod_logs(pod_name=noobaa_core_pod.name)
        assert (
            "JWT VERIFY FAILED" in pod_logs
        ), "Invalid access is not recorded in core pod logs"
