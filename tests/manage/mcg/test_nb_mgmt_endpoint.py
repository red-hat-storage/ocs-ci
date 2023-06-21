import json
import requests
import logging

from ocs_ci.framework.testlib import tier1

from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework.testlib import skipif_ocs_version

logger = logging.getLogger(name=__file__)


@tier1
@skipif_ocs_version("<4.14")
class TestNoobaaMgmtEndpoint(MCGTest):
    """
    Test the noobaa mgmt route functionality
    """

    def test_noobaa_mgmt_route(self, mcg_obj_session):
        """
        Test the noobaa mgmt route via an RPC call
        """
        rpc_response = send_rpc_request_to_mgmt_endpoint(
            mcg_obj_session, "system_api", "read_system"
        )
        assert rpc_response


def send_rpc_request_to_mgmt_endpoint(mcg_obj, api, method, params):
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
        verify=mcg_obj.retrieve_verification_mode(),
    ).json()
