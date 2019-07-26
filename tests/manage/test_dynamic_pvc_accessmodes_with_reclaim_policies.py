import logging
import pytest

from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from tests import helpers
from tests.fixtures import (
    create_ceph_block_pool, create_rbd_secret, create_cephfs_secret,
    create_project
)

logger = logging.getLogger(__name__)


class BaseDynamicPvc(ManageTest):
    """
    Base class for Dynamic PVC creation tests
    """
    access_mode = None
    storage_type = None
    expected_pod_failure = None
    expected_pvc_failure = None

    pvc_size = '10Gi'
    io_size = '512M'

    def dynamic_pvc_base(self, interface_type, reclaim_policy):
        """
        Base function for Dynamic PVC creation tests
        Fetches the worker nodes name list, creates StorageClass and PVC
        """
        self.interface_type = interface_type
        self.reclaim_policy = reclaim_policy
        self.worker_nodes_list = helpers.get_worker_nodes()

        if self.interface_type == constants.CEPHBLOCKPOOL:
            self.interface_name = self.cbp_obj.name
            self.secret_name = self.rbd_secret_obj.name

        elif self.interface_type == constants.CEPHFILESYSTEM:
            self.interface_name = helpers.get_cephfs_data_pool_name()
            self.secret_name = self.cephfs_secret_obj.name

        logger.info(
            f"Creating Storage Class with reclaimPolicy: {self.reclaim_policy}"
        )
        self.sc_obj = helpers.create_storage_class(
            interface_type=self.interface_type,
            interface_name=self.interface_name,
            secret_name=self.secret_name,
            reclaim_policy=self.reclaim_policy
        )

        logger.info(f"Creating PVC with accessModes: {self.access_mode}")
        self.pvc_obj = helpers.create_pvc(
            sc_name=self.sc_obj.name, namespace=self.namespace,
            size=self.pvc_size, wait=True, access_mode=self.access_mode
        )

    @retry(UnexpectedBehaviour, tries=10, delay=5, backoff=1)
    def verify_expected_failure_event(self, ocs_obj, failure_str):
        """
        Checks for the expected failure event message in oc describe command
        """
        if failure_str in ocs_obj.describe():
            logger.info(
                f"Failure string {failure_str} is present in oc describe"
                f" command"
            )
            return True
        else:
            raise UnexpectedBehaviour(
                f"Failure string {failure_str} is not found in oc describe"
                f" command"
            )

    def cleanup(self):
        """
        Removes resources created during test execution and verifies
        the reclaim policy is honored
        """

        pod_objs = pod.get_all_pods(namespace=self.namespace)
        if len(pod_objs) > 0:
            for pod_obj in pod_objs:
                pod_obj.delete()
                pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name)

        if hasattr(self, 'pvc_obj'):
            pv_obj = self.pvc_obj.backed_pv_obj
            self.pvc_obj.delete()

            try:
                assert helpers.validate_pv_delete(pv_obj.name)

            except AssertionError:
                if self.reclaim_policy == constants.RECLAIM_POLICY_RETAIN:
                    assert helpers.wait_for_resource_state(
                        pv_obj, constants.STATUS_RELEASED
                    )
                    # TODO: deletion of ceph rbd image, blocked by BZ#1723656
                    pv_obj.delete()

                else:
                    raise UnexpectedBehaviour(
                        f"PV {pv_obj.name} is not deleted after deleting PVC"
                    )

        if hasattr(self, 'sc_obj'):
            self.sc_obj.delete()


@tier1
@pytest.mark.usefixtures(
    create_ceph_block_pool.__name__,
    create_rbd_secret.__name__,
    create_cephfs_secret.__name__,
    create_project.__name__
)
@pytest.mark.parametrize(
    argnames=["interface_type", "reclaim_policy"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, constants.RECLAIM_POLICY_RETAIN],
            marks=pytest.mark.polarion_id("OCS-530")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, constants.RECLAIM_POLICY_DELETE],
            marks=pytest.mark.polarion_id("OCS-533")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, constants.RECLAIM_POLICY_RETAIN],
            marks=pytest.mark.polarion_id("OCS-525")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, constants.RECLAIM_POLICY_DELETE],
            marks=pytest.mark.polarion_id("OCS-526")
        )
    ]
)
class TestRWODynamicPvc(BaseDynamicPvc):
    """
    Automates the following test cases
    OCS-530 - RBD Based RWO Dynamic PVC creation with Reclaim policy set
    to Retain
    OCS-533 - RBD Based RWO Dynamic PVC creation with Reclaim policy set
    to Delete
    OCS-525 - CephFS Based RWO Dynamic PVC creation with Reclaim policy set
    to Retain
    OCS-526 - CephFS Based RWO Dynamic PVC creation with Reclaim policy set
    to Delete
    """

    access_mode = constants.ACCESS_MODE_RWO
    storage_type = 'fs'
    expected_pod_failure = 'Multi-Attach error for volume'

    @pytest.fixture()
    def setup_base(self, request, interface_type, reclaim_policy):

        def finalizer():
            self.cleanup()
        request.addfinalizer(finalizer)

        self.dynamic_pvc_base(interface_type, reclaim_policy)

    def test_rwo_dynamic_pvc(self, setup_base):
        """
        RWO Dynamic PVC creation tests with Reclaim policy set to Delete/Retain
        """
        logger.info(f"Creating two pods using same PVC {self.pvc_obj.name}")
        logger.info(f"Creating first pod on node: {self.worker_nodes_list[0]}")
        pod_obj1 = helpers.create_pod(
            interface_type=self.interface_type, pvc_name=self.pvc_obj.name,
            desired_status=constants.STATUS_RUNNING, wait=True,
            namespace=self.namespace, node_name=self.worker_nodes_list[0],
            pod_dict_path=constants.NGINX_POD_YAML
        )
        node_pod1 = pod_obj1.get().get('spec').get('nodeName')

        logger.info(
            f"Creating second pod on node: {self.worker_nodes_list[1]}"
        )

        pod_obj2 = helpers.create_pod(
            interface_type=self.interface_type, pvc_name=self.pvc_obj.name,
            wait=False, namespace=self.namespace,
            node_name=self.worker_nodes_list[1],
            pod_dict_path=constants.NGINX_POD_YAML
        )
        node_pod2 = pod_obj2.get().get('spec').get('nodeName')

        assert node_pod1 != node_pod2, 'Both pods are on the same node'

        logger.info(f"Running IO on pod {pod_obj1.name}")
        file_name = pod_obj1.name
        pod_obj1.run_io(
            storage_type=self.storage_type, size=self.io_size, runtime=30,
            fio_filename=file_name
        )
        pod.get_fio_rw_iops(pod_obj1)
        md5sum_pod1_data = pod.cal_md5sum(
            pod_obj=pod_obj1, file_name=file_name
        )

        # Verify that second pod is still in Pending state and not able to
        # attain Running state due to expected failure
        assert helpers.wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_PENDING
        )
        self.verify_expected_failure_event(
            ocs_obj=pod_obj2, failure_str=self.expected_pod_failure
        )

        pod_obj1.delete()
        pod_obj1.ocp.wait_for_delete(resource_name=pod_obj1.name)

        # Wait for second pod to be in Running state
        assert helpers.wait_for_resource_state(
            resource=pod_obj2, state=constants.STATUS_RUNNING, timeout=240
        )

        assert pod.verify_data_integrity(
            pod_obj=pod_obj2, file_name=file_name,
            original_md5sum=md5sum_pod1_data
        )

        pod_obj2.run_io(
            storage_type=self.storage_type, size=self.io_size, runtime=30,
            fio_filename=pod_obj2.name
        )
        pod.get_fio_rw_iops(pod_obj2)

        # Again verify data integrity
        assert pod.verify_data_integrity(
            pod_obj=pod_obj2, file_name=file_name,
            original_md5sum=md5sum_pod1_data
        )

        pod_obj2.delete()
        pod_obj2.ocp.wait_for_delete(resource_name=pod_obj2.name)


@tier1
@pytest.mark.usefixtures(
    create_ceph_block_pool.__name__,
    create_rbd_secret.__name__,
    create_cephfs_secret.__name__,
    create_project.__name__
)
@pytest.mark.parametrize(
    argnames=["interface_type", "reclaim_policy"],
    argvalues=[
        pytest.param(
            *[constants.CEPHBLOCKPOOL, constants.RECLAIM_POLICY_RETAIN],
            marks=pytest.mark.polarion_id("OCS-547")
        ),
        pytest.param(
            *[constants.CEPHBLOCKPOOL, constants.RECLAIM_POLICY_DELETE],
            marks=pytest.mark.polarion_id("OCS-538")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, constants.RECLAIM_POLICY_RETAIN],
            marks=pytest.mark.polarion_id("OCS-542")
        ),
        pytest.param(
            *[constants.CEPHFILESYSTEM, constants.RECLAIM_POLICY_DELETE],
            marks=pytest.mark.polarion_id("OCS-529")
        )
    ]
)
class TestRWXDynamicPvc(BaseDynamicPvc):
    """
    Automates the following test cases
    OCS-547 - RBD Based RWX Dynamic PVC creation with Reclaim policy set
    to Retain
    OCS-538 - RBD Based RWX Dynamic PVC creation with Reclaim policy set
    to Delete
    OCS-542 - CephFS Based RWX Dynamic PVC creation with Reclaim policy set
    to Retain
    OCS-529 - CephFS Based RWX Dynamic PVC creation with Reclaim policy set
    to Delete
    """
    access_mode = constants.ACCESS_MODE_RWX

    @pytest.fixture()
    def setup_base(self, request, interface_type, reclaim_policy):

        def finalizer():
            self.cleanup()
        request.addfinalizer(finalizer)

        self.dynamic_pvc_base(interface_type, reclaim_policy)

    def rwx_dynamic_pvc(self, setup_base):
        logger.info('RWX Test')
        # TODO


# ROX Dynamic PVC creation tests not supported in 4.2
# BZ 1727004
