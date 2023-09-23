import pytest
import logging
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.ocs.exceptions import ResourceLeftoversException
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2, skipif_external_mode
from tests.fixtures import (
    create_ceph_block_pool,
    create_rbd_secret,
    create_cephfs_secret,
    create_project,
)

log = logging.getLogger(__name__)


@pytest.fixture()
def resources(request):
    """
    Delete the resources created during the test
    Returns:
        tuple: empty lists of resources
    """
    pods, pvcs, storageclasses = ([] for i in range(3))

    def finalizer():
        """
        Delete the resources created during the test
        """
        failed_to_delete = []
        pvs = [pvc.backed_pv_obj for pvc in pvcs]
        for resource_type in pods, pvcs, storageclasses:
            for resource in resource_type:
                resource.delete()
                try:
                    resource.ocp.wait_for_delete(resource.name)
                except TimeoutError:
                    failed_to_delete.append(resource)
            if resource.kind == constants.PVC:
                helpers.wait_for_pv_delete(pvs)

        if failed_to_delete:
            raise ResourceLeftoversException(
                f"Failed to delete resources: {failed_to_delete}"
            )

    request.addfinalizer(finalizer)

    return pods, pvcs, storageclasses


@green_squad
@skipif_external_mode
@tier2
@pytest.mark.usefixtures(
    create_project.__name__,
    create_rbd_secret.__name__,
    create_cephfs_secret.__name__,
    create_ceph_block_pool.__name__,
)
class TestCreateMultipleScWithSamePoolName(ManageTest):
    """
    Create Multiple Storage Class with same pool name
    """

    @pytest.mark.parametrize(
        argnames="interface_type",
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-622")
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-623")
            ),
        ],
    )
    def test_create_multiple_sc_with_same_pool_name(self, interface_type, resources):
        """
        This test function does below,
        *. Creates multiple Storage Classes with same pool name
        *. Creates PVCs using each Storage Class
        *. Mount each PVC to an app pod
        *. Run IO on each app pod
        """
        # Unpack resources
        pods, pvcs, storageclasses = resources

        # Create 3 Storage Classes with same pool name
        if interface_type == constants.CEPHBLOCKPOOL:
            secret = self.rbd_secret_obj.name
            interface_name = self.cbp_obj.name
        else:
            interface_type = constants.CEPHFILESYSTEM
            secret = self.cephfs_secret_obj.name
            interface_name = helpers.get_cephfs_data_pool_name()
        for i in range(3):
            log.info(f"Creating a {interface_type} storage class")
            storageclasses.append(
                helpers.create_storage_class(
                    interface_type=interface_type,
                    interface_name=interface_name,
                    secret_name=secret,
                )
            )
            log.info(
                f"{interface_type}StorageClass: {storageclasses[i].name} "
                f"created successfully"
            )

        # Create PVCs using each SC
        for i in range(3):
            log.info(f"Creating a PVC using {storageclasses[i].name}")
            pvcs.append(helpers.create_pvc(storageclasses[i].name))
        for pvc in pvcs:
            helpers.wait_for_resource_state(pvc, constants.STATUS_BOUND)
            pvc.reload()

        # Create app pod and mount each PVC
        for i in range(3):
            log.info(f"Creating an app pod and mount {pvcs[i].name}")
            pods.append(
                helpers.create_pod(
                    interface_type=interface_type,
                    pvc_name=pvcs[i].name,
                    namespace=config.ENV_DATA["cluster_namespace"],
                )
            )
            for pod in pods:
                helpers.wait_for_resource_state(pod, constants.STATUS_RUNNING)
                pod.reload()
            log.info(
                f"{pods[i].name} created successfully and " f"mounted {pvcs[i].name}"
            )

        # Run IO on each app pod for sometime
        for pod in pods:
            log.info(f"Running FIO on {pod.name}")
            pod.run_io("fs", size="2G")

        for pod in pods:
            get_fio_rw_iops(pod)
