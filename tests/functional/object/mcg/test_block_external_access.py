import logging
from time import sleep

from ocs_ci.framework.testlib import MCGTest, red_squad, mcg, tier2
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


@tier2
@red_squad
@mcg
class TestBlockExternalAccess(MCGTest):
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

        self.set_storagecluster_disable_routes_value(storagecluster_obj, True)
        self.check_disable_routes(True)

        self.set_storagecluster_disable_routes_value(storagecluster_obj, False)
        self.check_disable_routes(False)

    def check_disable_routes(self, disable_routes_val):
        """
        This function tests that the routes are deleted are recreated after deletion correctly according to the value
        of disable_routes_val parameter. If the parameter value is false, all routes should be recreated after deletion
        If its value is false, only non_deletable_routes should be recreated and the deletable ones to be deleted completely
        Args:
            disable_routes_val (bool) Value of the 'disableRoutes' parameter
        """
        non_deletable_routes = [
            "ocs-storagecluster-cephobjectstore",
            "ocs-storagecluster-cephobjectstore-secure",
        ]
        deletable_routes = ["noobaa-mgmt", "s3", "sts"]
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
        route_names_after_deletion = [item["metadata"]["name"] for item in route_items]
        logger.info(f"Routes after deletion {route_names_after_deletion}")

        if not disable_routes_val:
            assert set(original_route_names) == set(
                route_names_after_deletion
            ), "Some of the routes don't exist"
        else:
            assert set(non_deletable_routes).issubset(
                set(route_names_after_deletion)
            ), "Some of the predefined routes were deleted"
            for route in non_deletable_routes:
                if route in original_route_names:
                    assert (
                        route in route_names_after_deletion
                    ), f"Predefined route {route} was deleted"
            assert set(deletable_routes).isdisjoint(
                set(route_names_after_deletion)
            ), "Some routes were not deleted"

    def set_storagecluster_disable_routes_value(self, storagecluster_obj, val):
        """
        This method sets the value of 'disableRoutes' flag of storagecluster_obj to val
        Args:
            storagecluster_obj (obj): Storage cluster on which the value should be set
            val (bool) Value to be set to 'disableRoutes' parameter
        """
        if not isinstance(val, bool):
            raise TypeError("val argument should be boolean")

        param_value = str(val).lower()
        disable_routes_param = (
            f'{{"spec": {{"multiCloudGateway": {{"disableRoutes": {param_value} }}}}}}'
        )

        storagecluster_obj.patch(
            params=disable_routes_param,
            format_type="merge",
        )

        # validate that the configuration really changed
        sc_dict = storagecluster_obj.get()
        logger.info(f"Updated configuration is {sc_dict}")

        disable_routes = (
            sc_dict.get("spec", {}).get("multiCloudGateway", {}).get("disableRoutes")
        )

        assert (
            disable_routes == val
        ), f"Disable routes is expected to be {val}, is {disable_routes}"

    def set_noobaa_disable_routes_value(self, noobaa_obj, val):
        """
        This method sets the value of 'disableRoutes' flag of storagecluster_obj to val
        Args:
            noobaa_obj (obj): Noobaa object on which the value should be set
            val (bool) Value to be set to 'disableRoutes' parameter
        """
        if not isinstance(val, bool):
            raise TypeError("val argument should be boolean")

        param_value = str(val).lower()
        disable_routes_param = f'{{"spec": {{"disableRoutes": {param_value} }}}}'

        noobaa_obj.patch(
            params=disable_routes_param,
            format_type="merge",
        )

        # validate that the configuration really changed
        noobaa_dict = noobaa_obj.get()
        logger.info(f"Updated configuration is {noobaa_dict}")

        disable_routes = noobaa_dict.get("spec", {}).get("disableRoutes")

        assert (
            disable_routes == val
        ), f"Disable routes is expected to be {val}, is {disable_routes}"

    def delete_multiCLoudGateway_from_storagecluster(self):
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
        self.delete_multiCLoudGateway_from_storagecluster()

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

        self.set_noobaa_disable_routes_value(noobaa_obj, True)
        self.check_disable_routes(True)

        self.set_noobaa_disable_routes_value(noobaa_obj, False)
        self.check_disable_routes(False)
