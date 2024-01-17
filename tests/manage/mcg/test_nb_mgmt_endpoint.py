import json
import requests
import logging

from ocs_ci.framework.pytest_customization.marks import mcg
from ocs_ci.framework.testlib import tier1

from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework.testlib import skipif_ocs_version

from ocs_ci.ocs.bucket_utils import retrieve_verification_mode

logger = logging.getLogger(name=__file__)


@mcg
@tier1
@skipif_ocs_version(">4.13")
class TestNoobaaMgmtEndpoint(MCGTest):
    """
    Test the noobaa mgmt route functionality
    """

    def test_noobaa_mgmt_endpoint(self, mcg_obj_session):
        """
        Test the noobaa mgmt route via an RPC
        """
        rpc_response = send_rpc_request_to_mgmt_endpoint(
            mcg_obj_session, "system_api", "read_system"
        )

        assert (
            rpc_response.ok
        ), f"RPC to {mcg_obj_session.mgmt_endpoint} failed with {rpc_response.status_code} status code"

        json_response = rpc_response.json()

        assert (
            "error" not in json_response
        ), f"RPC failed with message: {json_response['error']['message']}"

        logger.info("RPC to the noobaa-mgmt endpoint was successful")


def send_rpc_request_to_mgmt_endpoint(mcg_obj, api, method, params={}):
    """
    Send an RPC request to the noobaa mgmt route
    """

    logger.info(
        f"Sending MCG RPC query to the noobaa-mgmt endpoint:\n{api} {method} {params}"
    )

    payload = {
        "api": api,
        "method": method,
        "params": params,
        "auth_token": mcg_obj.noobaa_token,
    }

    return requests.post(
        url=mcg_obj.mgmt_endpoint,
        data=json.dumps(payload),
        verify=retrieve_verification_mode(),
    )
