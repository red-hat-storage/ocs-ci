import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    skipif_ocs_version,
    tier3,
    skipif_external_mode,
)
from ocs_ci.ocs import defaults, constants, ocp
from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster

logger = logging.getLogger(__name__)

RECONCILE_WAIT = 60


class TestS3Routes:

    """
    Tests related to ODF S3 routes
    """

    @pytest.fixture(scope="function", autouse=True)
    def revert_routes(self, request):
        """
        Teardown function which reverts the routes and storage cluster option back to original.
        """

        def finalizer():
            nb_s3_route_obj = ocp.OCP(
                kind=constants.ROUTE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                resource_name="s3",
            )
            if (
                nb_s3_route_obj.data["spec"]["tls"]["insecureEdgeTerminationPolicy"]
                == "Redirect"
            ):
                param = '{"spec":{"tls":{"insecureEdgeTerminationPolicy":"Allow","termination":"reencrypt"}}}'
                nb_s3_route_obj.patch(params=param, format_type="merge")

            if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
                storage_cluster_obj = get_storage_cluster()
                try:
                    if storage_cluster_obj.data["items"][0]["spec"]["managedResources"][
                        "cephObjectStores"
                    ]["disableRoute"]:
                        n_param = '[{"op": "remove", "path": "/spec/managedResources/cephObjectStores/disableRoute"}]'
                        storage_cluster_obj.patch(
                            resource_name="ocs-storagecluster",
                            params=n_param,
                            format_type="json",
                        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"
                        sleep(RECONCILE_WAIT)
                except KeyError:
                    logger.info(
                        "disableRoute does not exist in storage cluster, no need to revert"
                    )
                rgw_route_obj = ocp.OCP(
                    kind=constants.ROUTE,
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )
                assert rgw_route_obj.is_exist(
                    resource_name=constants.RGW_ROUTE_INTERNAL_MODE,
                )

        request.addfinalizer(finalizer)

    @tier3
    @bugzilla("")
    @bugzilla("")
    @skipif_external_mode
    @pytest.mark.polarion_id("")
    @skipif_ocs_version("<4.11")
    def test_s3_routes_reconcile(self):
        """
        Tests:
            1. Validates S3 route is not reconciled after changing insecureEdgeTerminationPolicy.
            2. Validates rgw route is not recreated after changing disableRoute in the storage cluster crd.
        """
        # S3 route
        nb_s3_route_obj = ocp.OCP(
            kind=constants.ROUTE,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name="s3",
        )
        param = '{"spec":{"tls":{"insecureEdgeTerminationPolicy":"Redirect","termination":"reencrypt"}}}'
        nb_s3_route_obj.patch(params=param, format_type="merge")
        sleep(RECONCILE_WAIT)
        nb_s3_route_obj.reload_data()
        logger.info("Validating updated s3 route persists and does not get reconciled")
        assert (
            nb_s3_route_obj.data["spec"]["tls"]["insecureEdgeTerminationPolicy"]
            == "Redirect"
        ), "Failed, s3 route is not updated"

        # RGW route
        if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
            rgw_route_obj = ocp.OCP(
                kind=constants.ROUTE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            )
            storage_cluster_obj = get_storage_cluster()
            n_param = '{"spec":{"managedResources":{"cephObjectStores":{"disableRoute":true}}}}'
            assert storage_cluster_obj.patch(
                resource_name="ocs-storagecluster",
                params=n_param,
                format_type="merge",
            ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"
            rgw_route_obj.delete(resource_name=constants.RGW_ROUTE_INTERNAL_MODE)
            sleep(RECONCILE_WAIT)
            logger.info("Validating whether rgw route does not get recreated")
            assert not rgw_route_obj.is_exist(
                resource_name=constants.RGW_ROUTE_INTERNAL_MODE
            ), "Failed: RGW route exist"
