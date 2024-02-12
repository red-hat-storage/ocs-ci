import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    skipif_ocs_version,
    tier3,
    skipif_external_mode,
    skipif_ibm_cloud,
    skipif_managed_service,
    red_squad,
    mcg,
)
from ocs_ci.ocs import defaults, constants, ocp
from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster
from ocs_ci.utility import version

logger = logging.getLogger(__name__)

RECONCILE_WAIT = 60


@mcg
@red_squad
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
            # Revert S3 route
            nb_s3_route_obj = ocp.OCP(
                kind=constants.ROUTE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                resource_name="s3",
            )
            if (
                nb_s3_route_obj.data["spec"]["tls"]["insecureEdgeTerminationPolicy"]
                == "Redirect"
            ):
                s3_route_param = '{"spec":{"tls":{"insecureEdgeTerminationPolicy":"Allow","termination":"reencrypt"}}}'
                nb_s3_route_obj.patch(params=s3_route_param, format_type="merge")

            # Revert disableRoute param
            if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
                storage_cluster_obj = get_storage_cluster()
                try:
                    if storage_cluster_obj.data["items"][0]["spec"]["managedResources"][
                        "cephObjectStores"
                    ]["disableRoute"]:
                        sc_param = '[{"op": "remove", "path": "/spec/managedResources/cephObjectStores/disableRoute"}]'
                        storage_cluster_obj.patch(
                            resource_name="ocs-storagecluster",
                            params=sc_param,
                            format_type="json",
                        ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"
                except KeyError:
                    logger.info(
                        "disableRoute does not exist in storage cluster, no need to revert"
                    )

            # Validate both routes
            sleep(RECONCILE_WAIT)
            nb_s3_route_obj.reload_data()
            assert (
                nb_s3_route_obj.data["spec"]["tls"]["insecureEdgeTerminationPolicy"]
                == "Allow"
            ), "Failed, Nb s3 route is not reverted."
            if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
                rgw_route_obj = ocp.OCP(
                    kind=constants.ROUTE,
                    namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                )
                assert rgw_route_obj.is_exist(
                    resource_name=constants.RGW_ROUTE_INTERNAL_MODE,
                ), "Failed, rgw route does not exist."

        request.addfinalizer(finalizer)

    @tier3
    @bugzilla("2067079")
    @bugzilla("2063691")
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-4648")
    @skipif_ocs_version("<4.11")
    def test_s3_routes_reconcile(self, revert_routes):
        """
        Tests:
            1. Validates S3 route is not reconciled after changing insecureEdgeTerminationPolicy.
            2. Validates rgw route is not recreated after enabling disableRoute in the storage cluster.
        """
        # S3 route
        nb_s3_route_obj = ocp.OCP(
            kind=constants.ROUTE,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name="s3",
        )
        s3_route_param = '{"spec":{"tls":{"insecureEdgeTerminationPolicy":"Redirect","termination":"reencrypt"}}}'
        nb_s3_route_obj.patch(params=s3_route_param, format_type="merge")
        sleep(RECONCILE_WAIT)
        nb_s3_route_obj.reload_data()
        logger.info("Validating updated s3 route persists")
        assert (
            nb_s3_route_obj.data["spec"]["tls"]["insecureEdgeTerminationPolicy"]
            == "Redirect"
        ), "Failed, s3 route is not updated, it has been reverted back to original"

        # RGW route
        if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
            rgw_route_obj = ocp.OCP(
                kind=constants.ROUTE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            )
            storage_cluster_obj = get_storage_cluster()
            sc_param = '{"spec":{"managedResources":{"cephObjectStores":{"disableRoute":true}}}}'
            assert storage_cluster_obj.patch(
                resource_name="ocs-storagecluster",
                params=sc_param,
                format_type="merge",
            ), "storagecluster.ocs.openshift.io/ocs-storagecluster not patched"
            rgw_route_obj.delete(resource_name=constants.RGW_ROUTE_INTERNAL_MODE)
            sleep(RECONCILE_WAIT)
            logger.info("Validating whether rgw route does not get recreated")
            assert not rgw_route_obj.is_exist(
                resource_name=constants.RGW_ROUTE_INTERNAL_MODE
            ), "Failed: RGW route exist, it has been recreated by the system"

    @pytest.fixture(scope="function")
    def revert_lb_service(self, request):
        """
        Teardown function to revert back the enabled disableLoadBalancerService
        """

        def finalizer():
            nb_obj = ocp.OCP(
                kind=constants.NOOBAA_RESOURCE_NAME,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                resource_name=constants.NOOBAA_RESOURCE_NAME,
            )
            try:
                if nb_obj.data["spec"]["disableLoadBalancerService"]:
                    lb_param = (
                        '[{"op": "remove", "path": "/spec/disableLoadBalancerService"}]'
                    )
                    logger.info("Revert disableLoadBalancerService")
                    nb_obj.patch(params=lb_param, format_type="json")
            except KeyError:
                logger.info(
                    "disableLoadBalancerService param does not exist, no need to revert"
                )
            nb_mgmt_svc_obj = ocp.OCP(
                kind=constants.SERVICE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                resource_name="noobaa-mgmt",
            )
            nb_s3_svc_obj = ocp.OCP(
                kind=constants.SERVICE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
                resource_name="s3",
            )
            sleep(10)
            logger.info("Validating service type reverted to LoadBalancer")
            if version.get_semantic_ocs_version_from_config() < version.VERSION_4_12:
                assert (
                    nb_mgmt_svc_obj.data["spec"]["type"]
                    and nb_s3_svc_obj.data["spec"]["type"] == "LoadBalancer"
                ), (
                    f'Failed, noobaa-mgmt type: {nb_mgmt_svc_obj.data["spec"]["type"]}, '
                    f's3 type: {nb_s3_svc_obj.data["spec"]["type"]}'
                )
            else:
                assert (
                    nb_s3_svc_obj.data["spec"]["type"] == "LoadBalancer"
                ), f'Failed, s3 type: {nb_s3_svc_obj.data["spec"]["type"]}'
            nb_route_obj = ocp.OCP(
                kind=constants.ROUTE,
                namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            )
            assert nb_route_obj.is_exist(resource_name="s3") and nb_route_obj.is_exist(
                resource_name="noobaa-mgmt"
            ), "Failed: Nb routes missing"

        request.addfinalizer(finalizer)

    @tier3
    @skipif_external_mode
    @skipif_ibm_cloud
    @skipif_managed_service
    @bugzilla("1954708")
    @pytest.mark.polarion_id("OCS-4653")
    @skipif_ocs_version("<4.10")
    def test_disable_nb_lb(self, revert_lb_service):
        """
        Validates the functionality of disableLoadBalancerService param
        """
        nb_obj = ocp.OCP(
            kind=constants.NOOBAA_RESOURCE_NAME,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name=constants.NOOBAA_RESOURCE_NAME,
        )
        lb_param = (
            '[{"op": "add", "path": "/spec/disableLoadBalancerService", "value": true}]'
        )
        logger.info("Patching noobaa resource to enable disableLoadBalancerService")
        nb_obj.patch(params=lb_param, format_type="json")

        nb_mgmt_svc_obj = ocp.OCP(
            kind=constants.SERVICE,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name="noobaa-mgmt",
        )
        nb_s3_svc_obj = ocp.OCP(
            kind=constants.SERVICE,
            namespace=defaults.ROOK_CLUSTER_NAMESPACE,
            resource_name="s3",
        )
        sleep(10)
        logger.info("Validating service type changed to ClusterIP")
        if version.get_semantic_ocs_version_from_config() < version.VERSION_4_12:
            assert (
                nb_mgmt_svc_obj.data["spec"]["type"]
                and nb_s3_svc_obj.data["spec"]["type"] == "ClusterIP"
            ), (
                f'Failed: noobaa-mgmt type: {nb_mgmt_svc_obj.data["spec"]["type"]}, '
                f's3 type {nb_s3_svc_obj.data["spec"]["type"]}'
            )
        else:
            assert (
                nb_s3_svc_obj.data["spec"]["type"] == "ClusterIP"
            ), f'Failed: s3 type {nb_s3_svc_obj.data["spec"]["type"]}'
