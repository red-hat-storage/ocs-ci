import logging
from json import dumps

import pytest

from time import sleep

from ocs_ci.framework.testlib import MCGTest, red_squad, mcg, tier2
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def save_original_state(request):
    """
    Save the ODF route names and original cluster object and noobaa configurations and store them
    in the class members
    """

    # Save original multiCloudGateway section of storagecluster object configuration
    storagecluster_obj = ocp.OCP(
        resource_name=constants.DEFAULT_CLUSTERNAME,
        namespace=config.ENV_DATA["cluster_namespace"],
        kind=constants.STORAGECLUSTER,
    )

    sc_dict = storagecluster_obj.get()
    logger.info(f"Initial storagecluster configuration is {sc_dict}")
    request.cls.sc_multiCloudGateway_orig_val = sc_dict.get("spec", {}).get(
        "multiCloudGateway", None
    )
    logger.info(
        f"Initial sc_dict multiCloudGateway  is {request.cls.sc_multiCloudGateway_orig_val}"
    )

    # Save original disableRoutes flag in noobaa object configuration
    noobaa_obj = ocp.OCP(
        resource_name=constants.NOOBAA_RESOURCE_NAME,
        namespace=config.ENV_DATA["cluster_namespace"],
        kind=constants.NOOBAA_RESOURCE_NAME,
    )

    noobaa_dict = noobaa_obj.get()
    logger.info(f"Initial noobaa configuration is {noobaa_dict}")
    request.cls.noobaa_disableRoutes_orig_val = noobaa_dict.get("spec", {}).get(
        "disableRoutes", None
    )
    logger.info(
        f"Initial noobaa disableRoutes  is {request.cls.noobaa_disableRoutes_orig_val}"
    )

    # Save the original routes
    ocp_routes_obj = ocp.OCP(
        kind=constants.ROUTE, namespace=config.ENV_DATA["cluster_namespace"]
    )
    route_items = ocp_routes_obj.get().get("items")
    request.cls.original_route_names = set(
        [item["metadata"]["name"] for item in route_items]
    )
    logger.info(f"The existing routes are {request.cls.original_route_names}")

    # Get noobaa's routes by using the app=noobaa label:
    ocp_routes_obj = ocp.OCP(
        kind=constants.ROUTE,
        namespace=config.ENV_DATA["cluster_namespace"],
        selector=constants.NOOBAA_APP_LABEL,
    )
    route_items = ocp_routes_obj.get().get("items")
    request.cls.original_noobaa_route_names = set(
        [item["metadata"]["name"] for item in route_items]
    )
    logger.info(
        f"The existing noobaa routes are {request.cls.original_noobaa_route_names}"
    )


@tier2
@red_squad
@mcg
@pytest.mark.usefixtures("save_original_state")
class TestBlockExternalAccess(MCGTest):
    @pytest.fixture(scope="class", autouse=True)
    def cleanup(self, request):
        def finalizer():
            """
            This method restores the original settings of storagecluster and noobaa configurations that
             may have be changed by the tests
            """
            # Restore storagecluster configuration
            storagecluster_obj = ocp.OCP(
                resource_name=constants.DEFAULT_CLUSTERNAME,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.STORAGECLUSTER,
            )

            sc_patch_params = [
                {
                    "op": "replace",
                    "path": "/spec/multiCloudGateway",
                    "value": self.sc_multiCloudGateway_orig_val,
                }
            ]
            storagecluster_obj.patch(
                params=dumps(sc_patch_params),
                format_type="json",
            )

            # Restore noobaa configuration
            noobaa_obj = ocp.OCP(
                resource_name=constants.NOOBAA_RESOURCE_NAME,
                namespace=config.ENV_DATA["cluster_namespace"],
                kind=constants.NOOBAA_RESOURCE_NAME,
            )

            noobaa_patch_params = [
                {
                    "op": "replace",
                    "path": "/spec/disableRoutes",
                    "value": self.noobaa_disableRoutes_orig_val,
                }
            ]
            noobaa_obj.patch(
                params=dumps(noobaa_patch_params),
                format_type="json",
            )

        request.addfinalizer(finalizer)

    def test_block_access_from_storagecluster(
        self,
    ):
        """
        This method validates that
         - if disableRoutes flag in storagecluster yaml is set to False then all routes are recreated after deletion
         - if disableRoutes flag in storagecluster yaml is set to True then noobaa-mgmt, s3 and sts routes are not
        recreated after deletion
        """
        storagecluster_obj = ocp.OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )

        # validate that initial value of disableRoutes is false or missing (and is false by default)
        sc_dict = storagecluster_obj.get()
        logger.info(f"Initial storagecluster configuration is {sc_dict}")

        disable_routes_status = (
            sc_dict.get("spec", {}).get("multiCloudGateway", {}).get("disableRoutes")
        )
        assert (
            not disable_routes_status
        ), "Disable Routes should be not defined or false when test starts"

        self.check_disable_routes(False)

        self.set_disable_routes_value(storagecluster_obj, True, True)
        self.check_disable_routes(True)

        self.set_disable_routes_value(storagecluster_obj, True, False)
        self.check_disable_routes(False)

    def check_disable_routes(self, disable_routes_val):
        """
        This function tests that the routes are deleted are recreated after deletion correctly according to the value
        of disable_routes_val parameter. If the parameter value is false, all routes should be recreated after deletion.
        If its value is false, only non_deletable_routes should be recreated and the deletable ones
        should be deleted completely.
        Args:
            disable_routes_val (bool) Value of the 'disableRoutes' parameter
        """
        timeout = 60

        ocp_routes_obj = ocp.OCP(
            kind=constants.ROUTE, namespace=config.ENV_DATA["cluster_namespace"]
        )
        route_items = ocp_routes_obj.get().get("items")
        original_route_names = [item["metadata"]["name"] for item in route_items]

        logger.info(f"Existing routes {original_route_names}")

        for route in original_route_names:
            ocp_routes_obj.delete(resource_name=route)

        sleep(timeout)

        ocp_routes_obj.reload_data()
        route_items = ocp_routes_obj.data.get("items")
        route_names_after_deletion = set(
            [item["metadata"]["name"] for item in route_items]
        )
        logger.info(f"Routes after deletion {route_names_after_deletion}")

        if not disable_routes_val:
            assert (
                self.original_route_names == route_names_after_deletion
            ), "Some of the routes don't exist"
        else:
            non_deletable_route_names = (
                self.original_route_names - self.original_noobaa_route_names
            )
            assert non_deletable_route_names.issubset(
                route_names_after_deletion
            ), "Some of the predefined routes were deleted"
            assert self.original_noobaa_route_names.isdisjoint(
                route_names_after_deletion
            ), "Some routes were not deleted"

    def set_disable_routes_value(self, ocp_obj, is_storage_cluster, val):
        """
        This method sets the value of 'disableRoutes' flag of the storagecluster or noobaa object  to val
        Args:
            ocp_obj (obj): Storage cluster or noobaa object on which the value should be set
            is_storage_cluster (bool): True if the ocp_obj is storagecluster and False if it is noobaa object
            val (bool) Value to be set to 'disableRoutes' parameter
        """
        if not isinstance(val, bool):
            raise TypeError("val argument should be boolean")

        param_value = str(val).lower()
        disable_routes_param = (
            (
                f'{{"spec": {{"multiCloudGateway": {{"disableRoutes": {param_value} }}}}}}'
            )
            if is_storage_cluster
            else f'{{"spec": {{"disableRoutes": {param_value} }}}}'
        )

        ocp_obj.patch(
            params=disable_routes_param,
            format_type="merge",
        )

        # validate that the configuration really changed
        obj_dict = ocp_obj.get()
        logger.info(f"Updated configuration is {obj_dict}")

        disable_routes = (
            (obj_dict.get("spec", {}).get("multiCloudGateway", {}).get("disableRoutes"))
            if is_storage_cluster
            else obj_dict.get("spec", {}).get("disableRoutes")
        )

        assert (
            disable_routes == val
        ), f"Disable routes is expected to be {val}, is {disable_routes}"

    def delete_multiCloudGateway_section_from_storagecluster(self):
        """
        This method removes 'multiCloudGateway' section from storagecluster configuration if this section exists there
        """
        storagecluster_obj = ocp.OCP(
            resource_name=constants.DEFAULT_CLUSTERNAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.STORAGECLUSTER,
        )

        sc_dict = storagecluster_obj.get()
        logger.info(f"Initial storagecluster configuration is {sc_dict}")

        multi_cloud_gateway = sc_dict.get("spec", {}).get("multiCloudGateway")
        if multi_cloud_gateway is not None:
            logger.info(
                f"multiCloudGateway section with value {multi_cloud_gateway} found , trying to delete"
            )
            storagecluster_obj.patch(
                params=[{"op": "remove", "path": "/spec/multiCloudGateway"}],
                format_type="json",
            )
            sc_dict = storagecluster_obj.get()
            logger.info(f"Storagecluster configuration after patch is {sc_dict}")

    def test_block_access_from_noobaa(
        self,
    ):
        """
        This method validates that
         - if disableRoutes flag in noobaa yaml is set to False then all routes are recreated after deletion
         - if disableRoutes flag in noobaa yaml is set to True then noobaa-mgmt, s3 and sts routes are not
        recreated after deletion
        """

        # The test is meaningful only when there is no 'disableRoutes' flag in storagecluster configuration
        self.delete_multiCloudGateway_section_from_storagecluster()

        noobaa_obj = ocp.OCP(
            resource_name=constants.NOOBAA_RESOURCE_NAME,
            namespace=config.ENV_DATA["cluster_namespace"],
            kind=constants.NOOBAA_RESOURCE_NAME,
        )

        # validate that initial value of disableRoutes is false or missing (and is false by default)
        noobaa_dict = noobaa_obj.get()
        logger.info(f"Initial noobaa configuration is {noobaa_dict}")

        disable_routes_status = noobaa_dict.get("spec", {}).get("disableRoutes")
        assert (
            not disable_routes_status
        ), "Disable Routes should be not defined or false when test starts"

        self.check_disable_routes(False)

        self.set_disable_routes_value(noobaa_obj, False, True)
        self.check_disable_routes(True)

        self.set_disable_routes_value(noobaa_obj, False, False)
        self.check_disable_routes(False)
