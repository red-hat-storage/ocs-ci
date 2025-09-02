import logging
import time

import yaml

from ocs_ci.framework.testlib import MCGTest, red_squad
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config


logger = logging.getLogger(__name__)


def check_disable_routes(disable_routes_val):
    """
    Args:
        disable_routes_val (bool) Value to be set to 'disableRoutes' parameter
    """
    non_deletable_routes = [
        "ocs-storagecluster-cephobjectstore",
        "ocs-storagecluster-cephobjectstore-secure",
    ]
    deletable_routes = ["noobaa-mgmt", "s3", "sts"]
    all_routes = non_deletable_routes + deletable_routes
    timeout = 60

    ocp_obj = ocp.OCP()
    result = ocp_obj.exec_oc_cmd(command="get routes -o yaml", out_yaml_format=False)
    data = yaml.safe_load(result)
    route_names = [item["metadata"]["name"] for item in data["items"]]
    logger.info(f"Existing routes {route_names}")
    assert set(all_routes).issubset(
        route_names
    ), "Some of the predefined routes don't exist"

    for route in all_routes:
        ocp_obj.exec_oc_cmd(command=f"delete route {route}")

    time.sleep(timeout)

    result = ocp_obj.exec_oc_cmd(command="get routes -o yaml", out_yaml_format=False)
    data = yaml.safe_load(result)
    route_names = [item["metadata"]["name"] for item in data["items"]]
    logger.info(f"Routes after deletion {route_names}")

    if not disable_routes_val:
        assert set(all_routes).issubset(
            set(route_names)
        ), "Some of the predefined routes don't exist"
    else:
        assert set(non_deletable_routes).issubset(
            set(route_names)
        ), "Some of the predefined routes were deleted"
        assert set(deletable_routes).isdisjoint(
            set(route_names)
        ), "Some routes were not deleted"


def set_disable_routes_value(storagecluster_obj, val):
    """
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


@red_squad
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
            not disable_routes_status or disable_routes_status is False
        ), "Disable Routes should be not defined or false when test starts"

        check_disable_routes(False)

        set_disable_routes_value(storagecluster_obj, True)
        check_disable_routes(True)

        set_disable_routes_value(storagecluster_obj, False)
        check_disable_routes(False)
