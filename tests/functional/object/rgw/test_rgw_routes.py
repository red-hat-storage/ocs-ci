import logging

import boto3
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    on_prem_platform_required,
    red_squad,
    skipif_external_mode,
    tier1,
    rgw,
    runs_on_provider,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.bucket_utils import (
    s3_delete_object,
    s3_get_object,
    s3_put_object,
)
from ocs_ci.ocs.exceptions import CommandFailed, UnavailableResourceException
from ocs_ci.ocs.resources.objectbucket import OBC

log = logging.getLogger(__name__)


@rgw
@red_squad
@runs_on_provider
@on_prem_platform_required
class TestRGWRoutes:
    """
    Test the RGW routes in an ODF cluster

    """

    @skipif_external_mode
    @bugzilla("2139037")
    @tier1
    @pytest.mark.parametrize(
        argnames="route_data",
        argvalues=[
            pytest.param(
                (constants.RGW_ROUTE_INTERNAL_MODE, "http"),
                marks=pytest.mark.polarion_id("OCS-5168"),
            ),
            pytest.param(
                (constants.RGW_ROUTE_INTERNAL_MODE_SECURE, "https"),
                marks=pytest.mark.polarion_id("OCS-5169"),
            ),
        ],
        ids=[
            "HTTP",
            "HTTPS",
        ],
    )
    def test_rgw_route(self, rgw_bucket_factory, route_data):
        """
        Test the availability of RGW routes in an ODF cluster

        1. Assert that RGW's service is exposed by the route
        2. Assert that the route uses a TLS termination policy
        3. Test basic I.O functionality using the endpoint

        """
        route_name, url_prefix = route_data
        route_obj = ocp.OCP(
            kind="Route", namespace=config.ENV_DATA["cluster_namespace"]
        )

        try:
            route_dict = route_obj.get(resource_name=route_name)
        except CommandFailed as ex:
            log.error(f"Failed to get {route_name}")
            raise UnavailableResourceException(ex)

        log.info(
            "Asserting that RGW's service is exposed by both http and https routes"
        )
        assert route_dict["spec"]["to"]["name"] == constants.RGW_SERVICE_INTERNAL_MODE
        assert route_dict["spec"]["port"]["targetPort"] == url_prefix

        log.info("Asserting that the endpoint uses a TLS termination policy")
        assert route_dict["spec"]["tls"]["insecureEdgeTerminationPolicy"] is not None

        log.info("Testing basic I.O functionality using the endpoint")
        rgw_bucket_name = rgw_bucket_factory(amount=1, interface="RGW-OC")[0].name
        rgw_obc = OBC(rgw_bucket_name)

        # Apply the current route to the OBC s3_client
        current_route_endpoint_url = f"{url_prefix}://{route_dict['spec']['host']}"
        rgw_obc.s3_resource = boto3.resource(
            "s3",
            verify=False,
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
            data="A simple test object string",
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
