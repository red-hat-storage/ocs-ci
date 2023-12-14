import logging
import pytest
import time

from ocs_ci.helpers.sanity_helpers import Sanity
from ocs_ci.ocs import constants, ocp
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pod import (
    get_mgr_pods,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.resources.pod import get_deployments_having_label
from ocs_ci.framework.pytest_customization.marks import (
    skipif_external_mode,
    brown_squad,
)
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    bugzilla,
    polarion_id,
)

log = logging.getLogger(__name__)


@brown_squad
@tier2
@bugzilla("1990031")
@polarion_id("OCS-2696")
@skipif_external_mode
class TestRestartMgrWhileTwoMonsDown(ManageTest):
    """
    Restart mgr pod while two mon pods are down

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        """
        Verify all pods on openshift-storage project on Running state

        """

        def finalizer():
            for mon_scale in self.mons_scale:
                self.oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mon_scale}")
            wait_for_pods_to_be_running(timeout=600)

        request.addfinalizer(finalizer)

    def test_restart_mgr_while_two_mons_down(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test Procedure:
        1.Scaling down two mons:
        oc scale --replicas=0 deploy/rook-ceph-mon-a
        oc scale --replicas=0 deploy/rook-ceph-mon-b

        2.Restarting mgr
        oc delete pod -l app=rook-ceph-mgr

        3.sleep 5 seconds

        4.Scaling mons back up
        oc scale --replicas=1 deploy/rook-ceph-mon-a
        oc scale --replicas=1 deploy/rook-ceph-mon-b

        5.sleep 10

        6.Waiting for mgr pod move to running state:
        oc get pod -l app=rook-ceph-mgr

        """
        self.oc = ocp.OCP(
            kind=constants.DEPLOYMENT, namespace=config.ENV_DATA["cluster_namespace"]
        )
        mons = [
            mon["metadata"]["name"]
            for mon in get_deployments_having_label(
                constants.MON_APP_LABEL, config.ENV_DATA["cluster_namespace"]
            )
        ]
        self.mons_scale = mons[0:2]
        tries = 11
        for index in range(1, tries):
            log.info(f"Scaling down two mons {self.mons_scale}, index={index}")
            for mon_scale in self.mons_scale:
                self.oc.exec_oc_cmd(f"scale --replicas=0 deployment/{mon_scale}")

            log.info(f"Restarting mgr pod, index={index}")
            mgr_pod = get_mgr_pods()
            mgr_pod[0].delete(wait=True)

            time.sleep(5)

            log.info(f"Scaling up two mons {self.mons_scale}, index={index}")
            for mon_scale in self.mons_scale:
                self.oc.exec_oc_cmd(f"scale --replicas=1 deployment/{mon_scale}")

            time.sleep(10)

            log.info(f"Waiting for mgr pod move to Running state, index={index}")
            mgr_pod_obj = ocp.OCP(
                kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
            )
            assert mgr_pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=constants.MGR_APP_LABEL,
                resource_count=1,
                timeout=100,
            ), f"Mgr pod did'nt move to Running state after 100 seconds, index={index}"

        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        log.info("Deleting Resources using sanity helpers")
        self.sanity_helpers.delete_resources()
