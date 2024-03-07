import logging
import pytest

import yaml

from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    polarion_id,
    tier1,
    skipif_ocs_version,
)
from ocs_ci.helpers import helpers
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.utils import run_cmd

log = logging.getLogger(__name__)


@green_squad
@skipif_ocs_version("<4.15")
class TestRwopPvc(ManageTest):
    """
    Tests ReadWriteOncePod RBD PVC
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, pvc_factory, pod_factory):
        """
        Create PVC and pods

        """
        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            access_mode=constants.ACCESS_MODE_RWOP,
            size=10,
        )
        self.pod_obj = pod_factory(
            pvc=self.pvc_obj,
        )

    @polarion_id("OCS-5456")
    @tier1
    def test_pod_with_same_priority(self, pod_factory):
        """
        Test RBD Block volume mode RWOP PVC

        """
        # Verify that PVCs are reusable by creating new pods
        log.info(f"PVC obj {self.pvc_obj}")
        new_pod_obj = helpers.create_pods(
            [self.pvc_obj],
            pod_factory,
            constants.CEPHBLOCKPOOL,
        )

        yaml_output = run_cmd(
            "oc get pod " + str(new_pod_obj[0].name) + " -o yaml", timeout=60
        )
        log.info(f"yaml_output of the pod {self.pod_obj.name} - {yaml_output}")

        # Validating the pod status
        results = yaml.safe_load(yaml_output)
        log.info(f"Status of the Pod : {results['status']['phase']}")
        if results["status"]["phase"] != "Pending":
            raise UnexpectedBehaviour(
                f"Pod {self.pod_obj.name} using RWOP pvc {self.pvc_obj.name} is not in Pending state"
            )
