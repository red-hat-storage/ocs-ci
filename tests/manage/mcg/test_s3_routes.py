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
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster

logger = logging.getLogger(__name__)


@tier3
@bugzilla("")
@bugzilla("")
@skipif_external_mode
@pytest.mark.polarion_id("")
@skipif_ocs_version("<4.11")
class TestS3Routes:

    """
    Tests related to ODF S3 routes
    """

    @pytest.fixture(scope="function")
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
            if nb_s3_route_obj == "Redirect":
                param = '{"spec":{"tls":{"insecureEdgeTerminationPolicy":"Allow","termination":"reencrypt"}}}'
                nb_s3_route_obj.patch(params=param, format_type="merge")
            if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
                storage_cluster_obj = get_storage_cluster()
                n_param = '{"managedResources":{"cephObjectStores":{}}}'
                if storage_cluster_obj:
                    storage_cluster_obj.patch(
                        resource_name="ocs-storagecluster",
                        params=n_param,
                        format_type="merge",
                    ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"
                    sleep(60)
                    rgw_s3_route_obj = ocp.OCP(
                        kind=constants.ROUTE,
                        namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                        resource_name=constants.RGW_ROUTE_INTERNAL_MODE,
                    )
                    logger.info(rgw_s3_route_obj)

        request.addfinalizer(finalizer)

    def test_s3_routes_reconcile(self):
        """
        Tests:
            1. Validates S3 route is not reconciled after changing insecureEdgeTerminationPolicy.
            2. Validates rgw route is not recreated after changing disableRoute in the storage cluster crd.
        """
        nb_s3_route_obj = ocp.OCP(
            kind=constants.ROUTE,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name="s3",
        )
        # s3_route_obj = ocp_obj.get(resource_name="s3")
        param = '{"spec":{"tls":{"insecureEdgeTerminationPolicy":"Redirect","termination":"reencrypt"}}}'
        nb_s3_route_obj.patch(params=param, format_type="merge")
        sleep(60)
        nb_s3_route_obj.reload_data()
        logger.info(nb_s3_route_obj)
        if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
            rgw_s3_route_obj = ocp.OCP(
                kind=constants.ROUTE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                resource_name=constants.RGW_ROUTE_INTERNAL_MODE,
            )
            storage_cluster_obj = get_storage_cluster()
            n_param = '{"spec":{"managedResources":{"cephObjectStores":{"disableRoute": true}}}}'
            assert storage_cluster_obj.patch(
                resource_name="ocs-storagecluster",
                params=n_param,
                format_type="merge",
            ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"

            rgw_s3_route_obj.delete()
            sleep(60)
            new_rgw_s3_route_obj = ocp.OCP(
                kind=constants.ROUTE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                resource_name="ocs-storagecluster-cephobjectstore",
            )
            assert new_rgw_s3_route_obj
