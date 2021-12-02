import logging

import pytest

from ocs_ci.framework.testlib import E2ETest, ignore_leftovers
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import wait_for_storage_pods, get_ceph_tools_pod
from ocs_ci.helpers.helpers import (
    mon_quorom_lost,
    recover_mon_quorum,
    modify_deployment_replica_count,
)
from ocs_ci.helpers.sanity_helpers import Sanity


log = logging.getLogger(__name__)


@ignore_leftovers
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
        # ToDo:Validate FIPS, hugepages enabled in cluster

        # ToDo: Create objects and write data's to it

        # ToDo: Create RGW kafka notification

        # ToDo: Perform mcg bucket replication

        # Todo: Noobaa caching

        # ToDo: Create PVC, snapshot and restore into new PVC, clone PVC

        # ToDo: Run background IOs

    @pytest.fixture(autouse=True)
    def base_teardown(self, request):
        def finalizer():
            op_obj = OCP(
                kind=constants.DEPLOYMENT,
                namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
            )
            op_obj = op_obj.get(resource_name="rook-ceph-operator")
            if op_obj.get("spec").get("replicas") != 1:
                modify_deployment_replica_count(
                    deployment_name="rook-ceph-operator", replica_count=1
                ), "Failed to scale up rook-ceph-operator to 1"

                log.info("Validate all mons are up and running")
                pod_obj = OCP(
                    kind=constants.POD, amespace=constants.OPENSHIFT_STORAGE_NAMESPACE
                )
                pod_obj.wait_for_resource(
                    condition=constants.STATUS_RUNNING,
                    selector=constants.MON_APP_LABEL,
                    resource_count=3,
                    timeout=1800,
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
        mon_pod_obj_list, mon_pod_running, ceph_mon_daemon_id = mon_quorom_lost()

        # Recover mon quorum
        recover_mon_quorum(mon_pod_obj_list, mon_pod_running, ceph_mon_daemon_id)

        # Remove crash list from ceph health
        log.info("Silence the ceph warnings by “archiving” the crash")
        tool_pod = get_ceph_tools_pod()
        tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)
        log.info("Removed ceph crash warnings. Check for ceph and cluster health")

        # Validate storage pods are running
        wait_for_storage_pods()

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
