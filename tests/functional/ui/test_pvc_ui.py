import logging
import pytest

from ocs_ci.framework.testlib import tier1, skipif_ui_not_support, ui
from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.framework.testlib import skipif_ocs_version
from ocs_ci.framework.pytest_customization.marks import (
    green_squad,
    runs_on_provider,
)
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs, get_pvc_objs
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import wait_for_resource_state, create_unique_resource_name
from ocs_ci.ocs.ui.views import locators_for_current_ocp_version
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from ocs_ci.framework import config

logger = logging.getLogger(__name__)


@ui
@runs_on_provider
@skipif_ocs_version("<4.6")
@skipif_ui_not_support("pvc")
@green_squad
class TestPvcUserInterface(object):
    """
    Test PVC User Interface

    """

    @tier1
    @pytest.mark.parametrize(
        argnames=["sc_name", "access_mode", "pvc_size", "vol_mode"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfs",
                "ReadWriteMany",
                "2",
                "Filesystem",
                marks=pytest.mark.polarion_id("OCS-5210"),
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "ReadWriteMany",
                "3",
                "Block",
                marks=pytest.mark.polarion_id("OCS-5211"),
            ),
            pytest.param(
                "ocs-storagecluster-cephfs",
                "ReadWriteOnce",
                "10",
                "Filesystem",
                marks=pytest.mark.polarion_id("OCS-5212"),
            ),
            pytest.param(
                *["ocs-storagecluster-ceph-rbd", "ReadWriteOnce", "11", "Block"],
                marks=[skipif_ocs_version("<4.7"), pytest.mark.polarion_id("OCS-5206")],
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "ReadWriteOnce",
                "13",
                "Filesystem",
                marks=pytest.mark.polarion_id("OCS-5207"),
            ),
        ],
    )
    def test_create_resize_delete_pvc(
        self,
        project_factory,
        teardown_factory,
        setup_ui_class_factory,
        sc_name,
        access_mode,
        pvc_size,
        vol_mode,
    ):
        """
        Test create, resize and delete pvc via UI

        """

        setup_ui_class_factory()

        # Creating a test project via CLI
        pro_obj = project_factory()
        project_name = pro_obj.namespace

        pvc_ui_obj = PvcUI()

        logger.test_step(
            f"Create PVC via UI with sc={sc_name}, access_mode={access_mode}, "
            f"size={pvc_size}, vol_mode={vol_mode}"
        )
        pvc_name = create_unique_resource_name("test", "pvc")

        if config.DEPLOYMENT["external_mode"]:
            if sc_name == constants.CEPHFILESYSTEM_SC:
                sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
            elif sc_name == constants.CEPHBLOCKPOOL_SC:
                sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD

        pvc_ui_obj.create_pvc_ui(
            project_name, sc_name, pvc_name, access_mode, pvc_size, vol_mode
        )

        OCP(kind=constants.PVC, namespace=project_name).wait_for_resource(
            condition=constants.STATUS_BOUND, resource_name=pvc_name, timeout=120
        )

        logger.test_step("Verify PVC properties match expected values")
        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvc = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]

        logger.assertion(f"PVC size: expected={pvc_size}, actual={pvc[0].size}")
        assert pvc[0].size == int(pvc_size), (
            f"size error| expected size:{pvc_size} \n "
            f"actual size:{str(pvc[0].size)}"
        )

        logger.assertion(
            f"PVC access mode: expected='{access_mode}', actual='{pvc[0].get_pvc_access_mode}'"
        )
        assert pvc[0].get_pvc_access_mode == access_mode, (
            f"access mode error| expected access mode:{access_mode} "
            f"\n actual access mode:{pvc[0].get_pvc_access_mode}"
        )

        logger.assertion(
            f"PVC storage class: expected='{sc_name}', actual='{pvc[0].backed_sc}'"
        )
        assert pvc[0].backed_sc == sc_name, (
            f"storage class error| expected storage class:{sc_name} "
            f"\n actual storage class:{pvc[0].backed_sc}"
        )

        logger.assertion(
            f"PVC volume mode: expected='{vol_mode}', actual='{pvc[0].get_pvc_vol_mode}'"
        )
        assert pvc[0].get_pvc_vol_mode == vol_mode, (
            f"volume mode error| expected volume mode:{vol_mode} "
            f"\n actual volume mode:{pvc[0].get_pvc_vol_mode}"
        )

        logger.test_step("Verify PVC details via UI")
        pvc_ui_obj.verify_pvc_ui(
            pvc_size=pvc_size,
            access_mode=access_mode,
            vol_mode=vol_mode,
            sc_name=sc_name,
            pvc_name=pvc_name,
            project_name=project_name,
        )
        logger.info("PVC details verified via UI")

        logger.test_step("Create pod and wait for Running state")
        if sc_name in constants.DEFAULT_STORAGECLASS_RBD:
            interface_type = constants.CEPHBLOCKPOOL
        elif sc_name in constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD:
            interface_type = constants.CEPHBLOCKPOOL
        else:
            interface_type = constants.CEPHFILESYSTEM

        new_pod = helpers.create_pod(
            interface_type=interface_type,
            pvc_name=pvc_name,
            namespace=project_name,
            raw_block_pv=vol_mode == constants.VOLUME_MODE_BLOCK,
        )

        logger.info(
            f"Waiting for pod {new_pod.name} to reach {constants.STATUS_RUNNING} state"
        )
        wait_for_resource_state(
            resource=new_pod, state=constants.STATUS_RUNNING, timeout=120
        )

        # Calling the Teardown Factory Method to make sure Pod is deleted
        teardown_factory(new_pod)

        logger.test_step(f"Resize PVC from {pvc_size} to {int(pvc_size) + 3} GiB")
        new_size = int(pvc_size) + 3
        pvc_ui_obj.pvc_resize_ui(
            pvc_name=pvc_name, new_size=new_size, project_name=project_name
        )

        logger.assertion(
            f"PVC new size > old size: expected={new_size} > {pvc_size}, actual={new_size > int(pvc_size)}"
        )
        assert new_size > int(
            pvc_size
        ), f"New size of the PVC cannot be less than existing size: new size is {new_size})"

        self.pvc_loc = locators_for_current_ocp_version()["pvc"]

        logger.info("Verifying PVC resize via UI")
        expected_capacity = f"{new_size} GiB"
        pvc_resize = pvc_ui_obj.verify_pvc_resize_ui(
            project_name=project_name,
            pvc_name=pvc_name,
            expected_capacity=expected_capacity,
        )

        logger.assertion(f"PVC resize result: expected=True, actual={pvc_resize}")
        assert pvc_resize, "PVC resize failed"
        logger.info(f"PVC resize verified. New capacity: {expected_capacity}")

        logger.test_step("Run FIO on pod")
        if vol_mode == constants.VOLUME_MODE_BLOCK:
            storage_type = constants.WORKLOAD_STORAGE_TYPE_BLOCK
        else:
            storage_type = constants.WORKLOAD_STORAGE_TYPE_FS

        new_pod.run_io(
            storage_type,
            size=(new_size - 1),
            invalidate=0,
            direct=int(storage_type == "block"),
        )

        get_fio_rw_iops(new_pod)
        logger.info("FIO execution on pod completed successfully")

        logger.test_step("Delete pod and PVC via UI")
        new_pod.delete(wait=True)
        new_pod.ocp.wait_for_delete(resource_name=new_pod.name)

        logger.info(f"Deleting PVC {pvc_name} via UI")
        pvc_ui_obj.delete_pvc_ui(pvc_name, project_name)

        pvc[0].ocp.wait_for_delete(pvc_name, timeout=120)

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"

    @tier1
    @pytest.mark.parametrize(
        argnames=["sc_name", "access_mode", "clone_access_mode"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                constants.ACCESS_MODE_RWO,
                constants.ACCESS_MODE_RWO,
                marks=pytest.mark.polarion_id("OCS-5208"),
            ),
            pytest.param(
                "ocs-storagecluster-cephfs",
                constants.ACCESS_MODE_RWX,
                constants.ACCESS_MODE_RWO,
                marks=pytest.mark.polarion_id("OCS-5209"),
            ),
        ],
    )
    def test_clone_pvc(
        self,
        project_factory,
        teardown_factory,
        setup_ui_class_factory,
        sc_name,
        access_mode,
        clone_access_mode,
    ):
        """
        Test to verify PVC clone from UI

        """

        setup_ui_class_factory()

        pvc_size = "1"
        vol_mode = constants.VOLUME_MODE_FILESYSTEM

        # Creating a project from CLI
        pro_obj = project_factory()
        project_name = pro_obj.namespace

        pvc_ui_obj = PvcUI()

        if config.DEPLOYMENT["external_mode"]:
            if sc_name == constants.CEPHFILESYSTEM_SC:
                sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_CEPHFS
            elif sc_name == constants.CEPHBLOCKPOOL_SC:
                sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_RBD

        pvc_name = create_unique_resource_name("test", "pvc")
        logger.test_step(f"Create PVC '{pvc_name}' via UI with sc={sc_name}")
        pvc_ui_obj.create_pvc_ui(
            project_name, sc_name, pvc_name, access_mode, pvc_size, vol_mode
        )

        teardown_factory(get_pvc_objs(pvc_names=[pvc_name], namespace=project_name)[0])

        logger.test_step("Verify PVC details in UI")
        pvc_ui_obj.verify_pvc_ui(
            pvc_size=pvc_size,
            access_mode=access_mode,
            vol_mode=vol_mode,
            sc_name=sc_name,
            pvc_name=pvc_name,
            project_name=project_name,
        )
        logger.info(f"PVC {pvc_name} details verified in UI")

        logger.test_step("Clone PVC from UI")
        clone_pvc_name = f"{pvc_name}-clone"
        pvc_ui_obj.pvc_clone_ui(
            project_name=project_name,
            pvc_name=pvc_name,
            cloned_pvc_access_mode=clone_access_mode,
            cloned_pvc_name=clone_pvc_name,
        )

        teardown_factory(
            get_pvc_objs(pvc_names=[clone_pvc_name], namespace=project_name)[0]
        )

        logger.test_step("Verify cloned PVC details in UI")
        pvc_ui_obj.verify_pvc_ui(
            pvc_size=pvc_size,
            access_mode=clone_access_mode,
            vol_mode=vol_mode,
            sc_name=sc_name,
            pvc_name=clone_pvc_name,
            project_name=project_name,
        )
        logger.info(f"Cloned PVC {clone_pvc_name} details verified in UI")
