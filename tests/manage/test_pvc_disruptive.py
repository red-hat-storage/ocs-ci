import pytest
import logging
from ocs_ci.ocs import constants
from tests import helpers, disruption_helpers
from ocs_ci.framework.testlib import ManageTest, tier4
from tests.fixtures import (
    create_rbd_storageclass, create_ceph_block_pool, create_cephfs_storageclass,
    create_rbd_secret, create_cephfs_secret, create_project
)

logger = logging.getLogger(__name__)

DISRUPTION_OPS = disruption_helpers.Disruptions()


@pytest.mark.usefixtures(create_project.__name__)
class BaseDisruption(ManageTest):
    """
    Base class for PVC related disruption tests
    """
    pod_obj = None
    pvc_obj = None
    storage_type = None

    def disruptive_base(self, operation_to_disrupt, resource_to_delete):
        """
        Base function for PVC disruptive tests
        """
        DISRUPTION_OPS.set_resource(resource=resource_to_delete)
        self.pvc_obj = helpers.create_pvc(
            sc_name=self.sc_obj.name, namespace=self.namespace, wait=False
        )
        if operation_to_disrupt == 'create_pvc':
            DISRUPTION_OPS.delete_resource()
        self.pvc_obj.reload()
        assert self.pvc_obj.ocp.wait_for_resource(
            condition=constants.STATUS_BOUND, resource_name=self.pvc_obj.name, timeout=120
        )

        self.pod_obj = helpers.create_pod(
            interface_type=constants.CEPHBLOCKPOOL, pvc_name=self.pvc_obj.name, wait=False,
            namespace=self.namespace
        )
        if operation_to_disrupt == 'create_pod':
            DISRUPTION_OPS.delete_resource()
        self.pod_obj.reload()

        assert self.pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_RUNNING, resource_name=self.pod_obj.name, timeout=120
        )
        self.pod_obj.run_io(storage_type=self.storage_type, size='1G')
        if operation_to_disrupt == 'run_io':
            DISRUPTION_OPS.delete_resource()

        self.pod_obj.delete()
        self.pvc_obj.delete()


@pytest.mark.usefixtures(
    create_rbd_secret.__name__,
    create_ceph_block_pool.__name__,
    create_rbd_storageclass.__name__,
)
@tier4
class TestRBDDisruption(BaseDisruption):
    """
    RBD PVC related disruption tests class
    """
    storage_type = 'block'

    @pytest.mark.parametrize(
        argnames=["operation_to_disrupt", "resource_to_delete"],
        argvalues=[
            pytest.param(
                *['create_pvc', 'mgr'], marks=pytest.mark.polarion_id("OCS-568")
            ),
            pytest.param(
                *['create_pod', 'mgr'], marks=pytest.mark.polarion_id("OCS-569")
            ),
            pytest.param(
                *['run_io', 'mgr'], marks=pytest.mark.polarion_id("OCS-570")
            ),
            pytest.param(
                *['create_pvc', 'mon'], marks=pytest.mark.polarion_id("OCS-561")
            ),
            pytest.param(
                *['create_pod', 'mon'], marks=pytest.mark.polarion_id("OCS-562")
            ),
            pytest.param(
                *['run_io', 'mon'], marks=pytest.mark.polarion_id("OCS-563")
            ),
            pytest.param(
                *['create_pvc', 'osd'], marks=pytest.mark.polarion_id("OCS-565")
            ),
            pytest.param(
                *['create_pod', 'osd'], marks=pytest.mark.polarion_id("OCS-554")
            ),
            pytest.param(
                *['run_io', 'osd'], marks=pytest.mark.polarion_id("OCS-566")
            ),

        ]
    )
    def test_disruptive_block(self, operation_to_disrupt, resource_to_delete):
        """
        RBD PVC related disruption tests class method
        """
        self.disruptive_base(operation_to_disrupt, resource_to_delete)


@pytest.mark.usefixtures(
    create_cephfs_secret.__name__,
    create_cephfs_storageclass.__name__,
)
@tier4
class TestFSDisruption(BaseDisruption):
    """
    CephFS PVC related disruption tests class
    """
    storage_type = 'fs'

    @pytest.mark.parametrize(
        argnames=["operation_to_disrupt", "resource_to_delete"],
        argvalues=[
            pytest.param(
                *['create_pvc', 'mgr'], marks=pytest.mark.polarion_id("OCS-555")
            ),
            pytest.param(
                *['create_pod', 'mgr'], marks=pytest.mark.polarion_id("OCS-558")
            ),
            pytest.param(
                *['run_io', 'mgr'], marks=pytest.mark.polarion_id("OCS-559")
            ),
            pytest.param(
                *['create_pvc', 'mon'], marks=pytest.mark.polarion_id("OCS-560")
            ),
            pytest.param(
                *['create_pod', 'mon'], marks=pytest.mark.polarion_id("OCS-550")
            ),
            pytest.param(
                *['run_io', 'mon'], marks=pytest.mark.polarion_id("OCS-551")
            ),
            pytest.param(
                *['create_pvc', 'osd'], marks=pytest.mark.polarion_id("OCS-552")
            ),
            pytest.param(
                *['create_pod', 'osd'], marks=pytest.mark.polarion_id("OCS-553")
            ),
            pytest.param(
                *['run_io', 'osd'], marks=pytest.mark.polarion_id("OCS-549")
            ),
            pytest.param(
                *['create_pvc', 'mds'], marks=pytest.mark.polarion_id("OCS-564")
            ),
            pytest.param(
                *['create_pod', 'mds'], marks=pytest.mark.polarion_id("OCS-567")
            ),
            pytest.param(
                *['run_io', 'mds'], marks=pytest.mark.polarion_id("OCS-556")
            ),
        ]
    )
    def test_disruptive_file(self, operation_to_disrupt, resource_to_delete):
        """
        CephFS PVC related disruption tests class method
        """
        self.disruptive_base(operation_to_disrupt, resource_to_delete)
