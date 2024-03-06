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
    Tests RWOP RBD PVC
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory, create_pvcs_and_pods):
        """
        Create PVC and pods

        """
        pvc_size_gi = 10
        self.pvc, self.pod = create_pvcs_and_pods(
           pvc_size=pvc_size_gi,
           access_modes_rbd=[constants.ACCESS_MODE_RWOP],
           num_of_rbd_pvc=1,
           num_of_cephfs_pvc=0,
        )


    @polarion_id("OCS-5456")
    @tier1
    def test_pod_with_same_priority(self, pod_factory):
        """
        Test RBD Block volume mode RWOP PVC

        """
        # Verify that PVCs are reusable by creating new pods
        interface = constants.CEPHBLOCKPOOL
        new_pod_obj = helpers.create_pods(
            self.pvc,
            pod_factory,
            interface,
        )

        for pod_obj in new_pod_obj:
            self.pod_obj = pod_obj
            yaml_output = run_cmd(f"oc get pod " +str(pod_obj.name)+ " -o yaml", timeout=60)
            log.info(f"yaml_output of the pod {self.pod_obj.name} - {yaml_output}")

        # Validating the pod status
        results = yaml.safe_load(yaml_output)
        log.info(f"results {results['status']['phase']}")
        if results ["status"]["phase"] != "Pending":
           raise UnexpectedBehaviour(
            f"Pod {self.pod_obj.name} using RWOP pvc {self.pvc} is not in Pending state"
        )





