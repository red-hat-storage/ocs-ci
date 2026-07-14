import logging
from time import sleep

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_ocs_version,
    tier3,
    skipif_external_mode,
    skipif_ibm_cloud,
    skipif_managed_service,
    skipif_mcg_only,
    red_squad,
    runs_on_provider,
    mcg,
)
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.resources.storage_cluster import get_storage_cluster
from ocs_ci.utility import version

logger = logging.getLogger(__name__)

RECONCILE_WAIT = 60


@mcg
@red_squad
@runs_on_provider
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
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name="s3",
            )
            if (
                nb_s3_route_obj.data["spec"]["tls"]["insecureEdgeTerminationPolicy"]
                == "None"
            ):
                # Set spec.multiCloudGateway.denyHTTP to true on ocs-storagecluster
                storagecluster_obj = ocp.OCP(
                    kind=constants.STORAGECLUSTER,
                    namespace=config.ENV_DATA["cluster_namespace"],
                    resource_name=constants.DEFAULT_CLUSTERNAME,
                )
                lb_param = '[{"op": "replace", "path": "/spec/multiCloudGateway/denyHTTP", "value": false}]'
                logger.info(
                    "Patching noobaa resource to disable disableLoadBalancerService"
                )
                storagecluster_obj.patch(params=lb_param, format_type="json")

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
                    namespace=config.ENV_DATA["cluster_namespace"],
                )
                assert rgw_route_obj.is_exist(
                    resource_name=constants.RGW_ROUTE_INTERNAL_MODE,
                ), "Failed, rgw route does not exist."

        request.addfinalizer(finalizer)

    @tier3
    @skipif_external_mode
    @pytest.mark.polarion_id("OCS-4648")
    @skipif_ocs_version("<4.11")
    @skipif_mcg_only
    def test_s3_routes_reconcile(self, revert_routes):
        """
        Tests:
            1. Validates S3 route is reconciled after setting denyHTTP to true in the storage cluster.
            2. Validates rgw route is not recreated after enabling disableRoute in the storage cluster.
        """

        # Set spec.multiCloudGateway.denyHTTP to true on ocs-storagecluster
        storagecluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_CLUSTERNAME,
        )
        lb_param = (
            '[{"op": "add", "path": "/spec/multiCloudGateway/denyHTTP", "value": true}]'
        )
        logger.info("Patching noobaa resource to enable disableLoadBalancerService")
        storagecluster_obj.patch(params=lb_param, format_type="json")

        sleep(RECONCILE_WAIT)

        # Check that the s3 route has been reconciled
        nb_s3_route_obj = ocp.OCP(
            kind=constants.ROUTE,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="s3",
        )
        logger.info("Validating the s3 route has been reconciled as expected")
        assert (
            nb_s3_route_obj.data["spec"]["tls"]["insecureEdgeTerminationPolicy"]
            == "None"
        ), "Failed, s3 route is not updated, it has been reverted back to original"

        # RGW route
        if config.ENV_DATA.get("platform") in constants.ON_PREM_PLATFORMS:
            rgw_route_obj = ocp.OCP(
                kind=constants.ROUTE,
                namespace=config.ENV_DATA["cluster_namespace"],
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
            storagecluster_obj = ocp.OCP(
                kind=constants.STORAGECLUSTER,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name=constants.DEFAULT_CLUSTERNAME,
            )
            try:
                if storagecluster_obj.data["spec"]["multiCloudGateway"][
                    "disableLoadBalancerService"
                ]:
                    lb_param = '[{"op": "remove", "path": "/spec/multiCloudGateway/disableLoadBalancerService"}]'
                    logger.info("Revert disableLoadBalancerService")
                    storagecluster_obj.patch(params=lb_param, format_type="json")
            except KeyError:
                logger.info(
                    "disableLoadBalancerService param does not exist, no need to revert"
                )
            nb_mgmt_svc_obj = ocp.OCP(
                kind=constants.SERVICE,
                namespace=config.ENV_DATA["cluster_namespace"],
                resource_name="noobaa-mgmt",
            )
            nb_s3_svc_obj = ocp.OCP(
                kind=constants.SERVICE,
                namespace=config.ENV_DATA["cluster_namespace"],
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
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            assert nb_route_obj.is_exist(resource_name="s3") and nb_route_obj.is_exist(
                resource_name="noobaa-mgmt"
            ), "Failed: Nb routes missing"

        request.addfinalizer(finalizer)

    @tier3
    @skipif_external_mode
    @skipif_ibm_cloud
    @skipif_managed_service
    @pytest.mark.polarion_id("OCS-4653")
    @skipif_ocs_version("<4.10")
    def test_disable_nb_lb(self, revert_lb_service):
        """
        Validates the functionality of disableLoadBalancerService param
        """
        storagecluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=constants.DEFAULT_CLUSTERNAME,
        )
        lb_param = '[{"op": "add", "path": "/spec/multiCloudGateway/disableLoadBalancerService", "value": true}]'
        logger.info("Patching noobaa resource to enable disableLoadBalancerService")
        storagecluster_obj.patch(params=lb_param, format_type="json")

        nb_mgmt_svc_obj = ocp.OCP(
            kind=constants.SERVICE,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="noobaa-mgmt",
        )
        nb_s3_svc_obj = ocp.OCP(
            kind=constants.SERVICE,
            namespace=config.ENV_DATA["cluster_namespace"],
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
