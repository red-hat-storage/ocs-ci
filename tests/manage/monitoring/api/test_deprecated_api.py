import logging
import pytest

from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework.testlib import (
    ManageTest,
    tier3,
    skipif_ocs_version,
    bugzilla,
)
from ocs_ci.framework import config as ocsci_config

log = logging.getLogger(__name__)


class TestVerifyDeprecatedApi(ManageTest):
    @tier3
    @bugzilla("1975581")
    @skipif_ocs_version("<4.8")
    @pytest.mark.polarion_id("OCS-4834")
    def test_verify_deprecated_api_calls_in_ocs(self):
        """
        Test Process:
        1.query through oc get command for all api calls
        2.Grep for v1beta1 in the query output
        3.Verify there is no v1beta1 related string
        """
        cluster_namespace = ocsci_config.ENV_DATA["cluster_namespace"]
        log.info("Checking if deprecated apis are used in OCS")
        command = "get rolebindings,clusterrolebindings,roles,CSIDriver -o custom-columns=KIND:kind,api:apiVersion"
        log.info(f"Output:{command}")
        ocp_obj = OCP(namespace=cluster_namespace)
        output = ocp_obj.exec_oc_cmd(command)
        assert "v1beta1" not in output, "Deprecated API's in use"
        log.info("No depractaed API's are in use")
