import base64
import logging
import pytest
import requests

from ocs_ci.deployment.azure import AZUREIPI
from ocs_ci.framework.pytest_customization.marks import (
    bugzilla,
    tier1,
    skipif_ocs_version,
)
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


@tier1
@bugzilla("1970123")
@pytest.mark.polarion_id("")
@skipif_ocs_version("<4.10")
class TestNoobaaStorageAccount:

    """
    Test azure Noobaa SA
    """

    def test_tls_version(
        self,
    ):
        """"""
        azure_depl = AZUREIPI()
        token = azure_depl.azure_util.credentials.token["access_token"]
        secret_ocp_obj = OCP(kind="secret", namespace="openshift-storage")
        creds_secret_obj = secret_ocp_obj.get("noobaa-azure-container-creds")
        resource_group_name = base64.b64decode(
            creds_secret_obj.get("data").get("azure_resourcegroup")
        ).decode("utf-8")
        account_name = base64.b64decode(
            creds_secret_obj.get("data").get("AccountName")
        ).decode("utf-8")
        subscription_id = base64.b64decode(
            creds_secret_obj.get("data").get("azure_subscription_id")
        ).decode("utf-8")
        headers = {"Authorization": f"Bearer {token}"}
        url = (
            f"https://management.azure.com/subscriptions/{subscription_id}/"
            f"resourceGroups/{resource_group_name}/providers/Microsoft.Storage/"
            f"storageAccounts/{account_name}?api-version=2021-09-01"
        )
        res = requests.get(
            url=url,
            headers=headers,
        ).json()
        assert res["properties"]["minimumTlsVersion"] == "TLS1_2"
        assert not res["properties"]["supportsHttpsTrafficOnly"]
