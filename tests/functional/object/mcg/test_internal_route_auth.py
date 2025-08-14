import logging
import requests

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import get_noobaa_metrics_token_from_secret
from ocs_ci.ocs.resources.pod import get_noobaa_endpoint_pods
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    red_squad,
    mcg,
    post_upgrade,
)

logger = logging.getLogger(__name__)


@mcg
@red_squad
class TestNoobaaRouteAuthentication:
    """
    Tests noobaa management route and noobaa endpoints with JWT authentication
    """

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
        s3_route_data = OCP(
            kind="route",
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            resource_name="noobaa-mgmt",
        ).get()
        mgmt_address = f'{s3_route_data["spec"]["host"]}'

        response = requests.get(
            f"https://{mgmt_address}/version", headers=valid_headers, verify=False
        )
        logger.info(response)
        assert (
            response.status_code == 200
        ), f"Expected response code 200. Received {response.status_code}"

        # Validate Noobaa Mgmt route version without using auth token
        response = requests.get(f"https://{mgmt_address}/version", verify=False)
        logger.info(response)
        assert (
            response.status_code == 401
        ), f"Expected response code 401. Received {response.status_code}"

        # Validate Noobaa Mgmt route version with incorrect auth token
        response = requests.get(
            f"https://{mgmt_address}/version",
            headers=invalid_headers,
            verify=False,
        )
        logger.info(response)
        assert (
            response.status_code == 403
        ), f"Expected response code 403. Received {response.status_code}"

        # Validate noobaa endpoint host address + port using auth token
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
        response = requests.get(
            f"http://{endpoint_host_ip}:{port}",
            headers=valid_headers,
            verify=False,
        )
        logger.info(response.status_code)
        assert (
            response.status_code == 200
        ), f"Expected response code 200. Received {response.status_code}"

        # Validate noobaa endpoint host address + port without using auth token
        response = requests.get(f"http://{endpoint_host_ip}:{port}", verify=False)
        logger.info(response.status_code)
        assert (
            response.status_code == 401
        ), f"Expected response code 401. Received {response.status_code}"

        # Validate noobaa endpoint host address + port with incorrect auth token
        response = requests.get(
            f"http://{endpoint_host_ip}:{port}",
            headers=invalid_headers,
            verify=False,
        )
        logger.info(response.status_code)
        assert (
            response.status_code == 403
        ), f"Expected response code 403. Received {response.status_code}"
