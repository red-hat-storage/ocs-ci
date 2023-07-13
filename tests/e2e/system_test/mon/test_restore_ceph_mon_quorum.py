import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    system_test,
    polarion_id,
    skipif_ocs_version,
    ignore_leftovers,
    skipif_external_mode,
)
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError, ResourceWrongStatusException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    wait_for_storage_pods,
    get_ceph_tools_pod,
    get_mon_pods,
)
from ocs_ci.helpers.helpers import (
    induce_mon_quorum_loss,
    recover_mon_quorum,
    modify_deployment_replica_count,
)
from ocs_ci.helpers.sanity_helpers import Sanity


log = logging.getLogger(__name__)


@ignore_leftovers
@system_test
@skipif_ocs_version("<4.9")
@skipif_external_mode
@polarion_id("OCS-2720")
class TestRestoreCephMonQuorum(E2ETest):
    """
    The objective of this test case is to verify that
    mons can be brought to quorum successfully by
    following the steps mentioned in
    https://access.redhat.com/solutions/5898541, when
    mon(s) are out of quorum on a cluster

    """

    @pytest.fixture(autouse=True)
    def init_sanity(self):
        """
        Initialize Sanity instance

        """
        self.sanity_helpers = Sanity()

    @pytest.fixture(autouse=True)
    def base_set_up(self):
        """
        Preconditions:

        a) Have a cluster with FIPS and Hugepages enabled
        b) Create some resources s3 objects, buckets and write data's to it.
        c) Create RGW kafka notification & see the objects are notified to kafka
        d) Perform mcg bucket replication (bidirectional) and see the objects are synced.
        e) Perform noobaa caching
        f) there are snapshots and clones for few PVCs
        g) Background IOs (fio pods) running
        """

        # ToDo: Create objects and write data's to it

        # ToDo: Create RGW kafka notification

        # ToDo: Perform mcg bucket replication

        # Todo: Noobaa caching

        # ToDo: Create PVC, snapshot and restore into new PVC, clone PVC

        # ToDo: Run background IOs

    @pytest.fixture(autouse=True)
    def rook_operator_teardown(self, request):
        def finalizer():
            op_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=config.ENV_DATA["cluster_namespace"],
            )
            pod_obj = OCP(
                kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"]
            )
            operator_obj = op_obj.get(resource_name=constants.ROOK_CEPH_OPERATOR)
            if operator_obj.get("spec").get("replicas") != 1:
                modify_deployment_replica_count(
                    deployment_name=constants.ROOK_CEPH_OPERATOR, replica_count=1
                ), "Failed to scale up rook-ceph-operator to 1"

            log.info("Validate all mons are up and running")
            try:
                pod_obj.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=constants.MON_APP_LABEL,
                    resource_count=3,
                    timeout=60,
                    sleep=5,
                )
            except (TimeoutExpiredError, ResourceWrongStatusException) as ex:
                log.warning(ex)
                op_obj.delete(resource_name=constants.ROOK_CEPH_OPERATOR)
                for pod in get_mon_pods():
                    pod.delete()
                pod_obj.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=constants.MON_APP_LABEL,
                    resource_count=3,
                    timeout=360,
                    sleep=5,
                )
            log.info("All mons are up and running")

        request.addfinalizer(finalizer)

    def test_restore_ceph_mon_quorum(
        self, pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
    ):
        """
        Test Procedure:

        1) With all the preconditions met, take mons out of quorum
        2) Follow the procedure mentioned in
           https://access.redhat.com/solutions/5898541 to recover the quorum
        3) Check for data access and integrity of block,
           object and file based data - No DU/DL/DC
        4) Make sure basic functionality working fine

        """

        # Take mons out of the quorum and confirm it
        (
            self.mon_pod_obj_list,
            mon_pod_running,
            ceph_mon_daemon_id,
        ) = induce_mon_quorum_loss()

        # Recover mon quorum
        recover_mon_quorum(self.mon_pod_obj_list, mon_pod_running, ceph_mon_daemon_id)

        # Validate storage pods are running
        wait_for_storage_pods()

        # Remove crash list from ceph health
        log.info("Silence the ceph warnings by “archiving” the crash")
        tool_pod = get_ceph_tools_pod()
        tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)
        log.info("Removed ceph crash warnings. Check for ceph and cluster health")

        # Validate cluster health
        self.sanity_helpers.health_check(tries=40)

        # ToDo: Common system test case validation: Check for data integrity and corruption after mon recovery

        # Creating Resources
        log.info("Creating Resources using sanity helpers")
        self.sanity_helpers.create_resources(
            pvc_factory, pod_factory, bucket_factory, rgw_bucket_factory
        )
        # Deleting Resources
        self.sanity_helpers.delete_resources()
