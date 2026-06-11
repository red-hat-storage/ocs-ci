import logging
import requests
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    on_prem_platform_required,
    post_upgrade,
    red_squad,
    skipif_disconnected_cluster,
    skipif_external_mode,
    skipif_ocs_version,
    skipif_proxy_cluster,
    tier2,
    rgw,
    runs_on_provider,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


@rgw
@red_squad
@skipif_disconnected_cluster
@skipif_proxy_cluster
@runs_on_provider
@on_prem_platform_required
class TestRGWHTTPEndpoint:
    """
    Test the RGW HTTP endpoint disable functionality

    """

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, request):
        """
        Setup and teardown fixture to restore original state after test

        """
        self.storage_cluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        self.route_obj = ocp.OCP(
            kind=constants.ROUTE, namespace=config.ENV_DATA["cluster_namespace"]
        )

        # Get storage cluster name with fallback
        self.storage_cluster_name = (
            self.storage_cluster_obj.resource_name or "ocs-storagecluster"
        )

        # Get initial storage cluster state
        self.storage_cluster_data = self.storage_cluster_obj.get(
            resource_name=self.storage_cluster_name
        )

        # Check if disableHttp parameter exists
        ceph_object_stores = (
            self.storage_cluster_data.get("spec", {})
            .get("managedResources", {})
            .get("cephObjectStores", {})
        )
        self.disable_http_exists = "disableHttp" in ceph_object_stores
        self.original_disable_http = ceph_object_stores.get("disableHttp", False)

        def teardown():
            """Teardown: Restore original state"""
            if self.disable_http_exists:
                # Restore original value if it existed
                log.info(
                    f"Restoring original disableHttp value: {self.original_disable_http}"
                )
                self.set_disable_http_value(
                    self.storage_cluster_name, self.original_disable_http
                )
            else:
                # Remove the parameter if it didn't exist originally
                log.info("Removing disableHttp parameter (it didn't exist originally)")
                params = '[{"op": "remove", "path": "/spec/managedResources/cephObjectStores/disableHttp"}]'
                try:
                    self.storage_cluster_obj.patch(
                        resource_name=self.storage_cluster_name,
                        params=params,
                        format_type="json",
                    )
                except CommandFailed:
                    # Parameter might have already been removed or never existed
                    log.info("disableHttp parameter already absent, nothing to remove")
            # Wait for RGW pods to be ready after restoration
            self.wait_for_rgw_pods_ready()

        request.addfinalizer(teardown)

    def set_disable_http_value(self, storage_cluster_name, value):
        """
        Set the disableHttp value in the StorageCluster

        Args:
            storage_cluster_name (str): Name of the StorageCluster resource
            value (bool): Value to set for disableHttp

        """
        params = (
            f'{{"spec":{{"managedResources":{{"cephObjectStores":'
            f'{{"disableHttp":{str(value).lower()}}}}}}}}}'
        )
        log.info(f"Setting disableHttp to {value}")
        assert self.storage_cluster_obj.patch(
            resource_name=storage_cluster_name, params=params, format_type="merge"
        ), f"Failed to set disableHttp={value}"

    def wait_for_rgw_pods_ready(self, timeout=300):
        """
        Wait for RGW pods to be in Ready state

        Args:
            timeout (int): Timeout in seconds

        """
        log.info("Waiting for RGW pods to be ready")
        pod_names = get_pod_name_by_pattern(
            "rgw", namespace=config.ENV_DATA["cluster_namespace"]
        )
        assert wait_for_pods_to_be_running(
            pod_names=pod_names,
            namespace=config.ENV_DATA["cluster_namespace"],
            timeout=timeout,
            sleep=10,
        ), f"RGW pods did not reach Running state within {timeout} seconds"
        log.info("All RGW pods are ready")

    def get_route(self, route_name):
        """
        Get route data by name

        Args:
            route_name (str): Name of the route

        Returns:
            dict or None: Route data if exists, None otherwise

        """
        try:
            route_data = self.route_obj.get(resource_name=route_name)
            return route_data
        except CommandFailed:
            return None

    def wait_for_route_to_exist(self, route_name, timeout=180):
        """
        Wait for a route to exist

        Args:
            route_name (str): Name of the route
            timeout (int): Timeout in seconds

        Returns:
            dict: Route data

        Raises:
            AssertionError: If route doesn't exist within timeout

        """
        try:
            log.info(f"Waiting for route {route_name} to exist")
            for sample in TimeoutSampler(
                timeout=timeout, sleep=10, func=self.get_route, route_name=route_name
            ):
                if sample:
                    log.info(f"Route {route_name} exists")
                    return sample
        except TimeoutExpiredError:
            raise AssertionError(
                f"Route {route_name} did not get created within {timeout} seconds"
            )

    @skipif_external_mode
    @tier2
    @post_upgrade
    @skipif_ocs_version(">4.22")
    def test_rgw_disable_http_endpoint(self):
        """
        Test the RGW HTTP endpoint disable functionality

        Test Steps:
        1. Verify the default value present for disableHttp parameter
        2. Validate unsecured RGW route is present in route info
        3. Change disableHttp value to true
        4. Validate unsecured RGW route is not present and HTTPS route is functional
        5. Try to curl unsecured address from local machine
        6. Re-enable HTTP endpoint and verify route is recreated

        """

        # Step 1: Verify the default value present for disableHttp parameter
        log.info("Step 1: Verifying default value of disableHttp parameter")
        if not self.disable_http_exists:
            log.info("disableHttp parameter does not exist (fresh cluster)")
        else:
            log.info(
                f"disableHttp parameter exists with value: {self.original_disable_http}"
            )
            # This test expects disableHttp to be False initially (HTTP enabled)
            assert self.original_disable_http is False, (
                f"Test expects disableHttp to be False initially, but found: {self.original_disable_http}. "
                "Please reset the cluster or manually set disableHttp to false before running this test."
            )

        # Step 2: Validate unsecured RGW route is present in route info
        log.info("Step 2: Validating unsecured RGW route is present")
        http_route_data = self.wait_for_route_to_exist(
            constants.RGW_ROUTE_INTERNAL_MODE, timeout=180
        )
        log.info("HTTP route exists as expected when disableHttp is False")

        # Save the HTTP endpoint URL for later testing in Step 5
        http_host = http_route_data.get("spec", {}).get("host")
        assert (
            http_host
        ), "HTTP route host is missing; cannot validate endpoint accessibility"
        http_endpoint_url = f"http://{http_host}"
        log.info(f"Saved HTTP endpoint URL: {http_endpoint_url}")

        # Verify HTTPS route exists
        self.wait_for_route_to_exist(
            constants.RGW_ROUTE_INTERNAL_MODE_SECURE, timeout=180
        )
        # Step 3: Change disableHttp value to true
        log.info("Step 3: Changing disableHttp value to true")
        self.set_disable_http_value(self.storage_cluster_name, True)

        # Wait for RGW pods to be ready
        self.wait_for_rgw_pods_ready()

        # Step 4: Validate unsecured RGW route is not present in route info
        log.info("Step 4: Validating unsecured RGW route is not present")
        assert self.route_obj.wait_for_delete(
            resource_name=constants.RGW_ROUTE_INTERNAL_MODE, timeout=180
        ), (
            f"Route {constants.RGW_ROUTE_INTERNAL_MODE} should be deleted "
            "when disableHttp is set to true"
        )

        # Verify HTTPS route still exists and is functional
        https_route_data = self.wait_for_route_to_exist(
            constants.RGW_ROUTE_INTERNAL_MODE_SECURE, timeout=60
        )
        log.info("HTTPS route exists, now verifying it is functional")

        # Verify HTTPS endpoint is accessible
        https_host = https_route_data.get("spec", {}).get("host")
        assert (
            https_host
        ), "HTTPS route host is missing; cannot validate endpoint accessibility"
        https_endpoint_url = f"https://{https_host}"
        log.info(f"Verifying HTTPS endpoint is functional: {https_endpoint_url}")

        try:
            response = requests.get(https_endpoint_url, timeout=10, verify=False)
            # HTTPS should be functional - accept success or auth-related codes
            assert response.status_code in [200, 403, 401, 404], (
                f"HTTPS endpoint should be functional after disabling HTTP, "
                f"but got status code {response.status_code}"
            )
            log.info(
                f"HTTPS endpoint is functional with status code {response.status_code}"
            )
        except requests.exceptions.RequestException as e:
            pytest.fail(f"HTTPS endpoint is not accessible after disabling HTTP: {e}")

        # Step 5: Try to curl unsecured address
        log.info("Step 5: Attempting to access HTTP endpoint (should fail)")
        log.info(
            f"Trying to access previously saved HTTP endpoint: {http_endpoint_url}"
        )

        # Try to access the HTTP endpoint - acceptable outcomes when disableHttp=true:
        # 1. Connection error (route deleted) - ideal
        # 2. 503 Service Unavailable (route being deleted/service unavailable)
        # 3. 404 Not Found (route points to nothing)
        # 4. 502 Bad Gateway (backend unavailable)
        try:
            response = requests.get(http_endpoint_url, timeout=10, verify=False)
            # Check if we got an error status indicating service is not accessible
            if response.status_code in [503, 404, 502, 500]:
                log.info(
                    f"HTTP endpoint returned error status {response.status_code}, "
                    "which indicates the endpoint is effectively inaccessible"
                )
                log.info("Step 5 passed: HTTP endpoint is not accessible")
            else:
                log.error(
                    f"HTTP endpoint returned success status {response.status_code} "
                    "even though disableHttp is true - this is unexpected!"
                )
                assert False, (
                    "HTTP endpoint should not be accessible when disableHttp is true, "
                    f"but got response with status code {response.status_code}"
                )
        except requests.exceptions.RequestException as e:
            # Connection error - endpoint is truly inaccessible (ideal case)
            log.info(f"HTTP endpoint correctly not accessible: {e}")
            log.info("Step 5 passed: HTTP endpoint is not accessible as expected")

        # Step 6: Re-enable HTTP endpoint and verify route is recreated
        log.info("Step 6: Re-enabling HTTP endpoint")
        self.set_disable_http_value(self.storage_cluster_name, False)
        self.wait_for_rgw_pods_ready()

        # Verify HTTP route is recreated (wait for it with timeout)
        log.info("Waiting for HTTP route to be recreated after re-enabling")
        self.wait_for_route_to_exist(constants.RGW_ROUTE_INTERNAL_MODE, timeout=180)

        # Verify the HTTP endpoint is actually accessible by making a request
        # Retry logic to handle RGW service initialization delay
        log.info("Verifying HTTP endpoint is accessible after re-enabling")

        def check_http_accessibility():
            """Check if HTTP endpoint is accessible"""
            try:
                response = requests.get(http_endpoint_url, timeout=10, verify=False)
                if response.status_code == 200:
                    log.info(
                        f"HTTP endpoint accessible with status {response.status_code}"
                    )
                    return True
                else:
                    log.info(f"Got status {response.status_code}, retrying...")
                    return False
            except requests.exceptions.RequestException as e:
                log.info(f"Connection failed: {e}, retrying...")
                return False

        # Wait for endpoint to become accessible (up to 2 minutes)
        for sample in TimeoutSampler(
            timeout=300, sleep=10, func=check_http_accessibility
        ):
            if sample:
                log.info("Step 6 passed: HTTP endpoint is accessible again")
                break

        log.info("Test completed successfully")
