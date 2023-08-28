import logging

import boto3

from ocs_ci.ocs import ocp, constants
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.exceptions import CommandFailed, UnavailableResourceException
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    skipif_external_mode,
)
from ocs_ci.ocs.bucket_utils import (
    s3_put_object,
    s3_get_object,
    s3_delete_object,
)

log = logging.getLogger(__name__)


class TestRGWRoutes:
    """
    Test the RGW routes in an ODF cluster

    """

    @skipif_external_mode
    @bugzilla("2139037")
    def test_rgw_routes(self, rgw_bucket_factory):
        """
        Test the availability of RGW routes in an ODF cluster

        1. Assert that RGW's service is exposed by both http and https routes
        2. Assert that both endpoints use a TLS termination policy
        3. Test basic I.O functionality using both endpoints

        """
        route_obj = ocp.OCP(kind="Route", namespace=self.namespace)

        log.info("Getting RGW's HTTP and HTTPS routes")
        try:
            http_route = route_obj.get(resource_name=constants.RGW_ROUTE_INTERNAL_MODE)
        except CommandFailed as ex:
            log.error("Failed to get RGW's HTTP route!")
            raise UnavailableResourceException(ex)

        try:
            https_route = route_obj.get(
                resource_name=constants.RGW_ROUTE_INTERNAL_MODE_SECURE
            )
        except CommandFailed as ex:
            log.error("Failed to get RGW's HTTPS route!")
            raise UnavailableResourceException(ex)

        log.info(
            "Asserting that RGW's service is exposed by both http and https routes"
        )
        assert http_route["spec"]["to"]["name"] == constants.RGW_SERVICE_INTERNAL_MODE
        assert (
            https_route[["spec"]["to"]["name"]] == constants.RGW_SERVICE_INTERNAL_MODE
        )
        assert http_route["spec"]["port"]["targetPort"] == "http"
        assert https_route["spec"]["port"]["targetPort"] == "https"

        log.info("Asserting that both endpoints use a TLS termination policy")
        assert http_route["spec"]["tls"]["insecureEdgeTerminationPolicy"] is not None
        assert https_route["spec"]["tls"]["insecureEdgeTerminationPolicy"] is not None

        log.info("Testing basic I.O functionality using both endpoints")
        rgw_bucket_name = rgw_bucket_factory(amount=1, interface="RGW-OC")[0].name
        rgw_obc = OBC(rgw_bucket_name)

        for route in http_route, https_route:
            # Apply the current route to the OBC s3_client
            route_name = route["metadata"]["name"]
            url_prefix = route["spec"]["port"]["targetPort"]
            current_route_endpoint_url = f"{url_prefix}://{route['spec']['host']}"
            rgw_obc.s3_resource = boto3.resource(
                "s3",
                endpoint_url=current_route_endpoint_url,
                aws_access_key_id=rgw_obc.access_key_id,
                aws_secret_access_key=rgw_obc.access_key,
            )
            rgw_obc.s3_client = rgw_obc.s3_resource.meta.client

            # Test basic I.O functionality
            assert s3_put_object(
                s3_obj=rgw_obc,
                bucketname=rgw_bucket_name,
                object_key=f"test-route-{route_name}",
                object_data="A simple test object string",
                content_type="text/html",
            ), f"s3_put_object failed via route {route_name}!"

            assert s3_get_object(
                s3_obj=rgw_obc,
                bucketname=rgw_bucket_name,
                object_key=f"test-route-{route_name}",
            ), f"s3_get_object failed via route {route_name}!"

            assert s3_delete_object(
                s3_obj=rgw_obc,
                bucketname=rgw_bucket_name,
                object_key=f"test-route-{route_name}",
            ), f"s3_delete_object failed via route {route_name}!"
