import logging
import pytest

from ocs_ci.framework.testlib import tier1, skipif_ui_not_support, ignore_data_rebalance
from ocs_ci.ocs.ui.pvc_ui import PvcUI
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    skipif_ocp_version,
)
from ocs_ci.ocs.resources.pvc import get_all_pvc_objs
from ocs_ci.ocs import constants
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import wait_for_resource_state, create_unique_resource_name
from ocs_ci.utility.utils import get_ocp_version
from ocs_ci.ocs.ui.views import locators
from ocs_ci.ocs.resources.pod import get_fio_rw_iops

logger = logging.getLogger(__name__)


@ignore_data_rebalance
class TestPvcUserInterface(object):
    """
    Test PVC User Interface

    """

    @tier1
    @skipif_ocs_version("<4.6")
    @skipif_ui_not_support("pvc")
    @pytest.mark.parametrize(
        argnames=["sc_name", "access_mode", "pvc_size", "vol_mode"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfs",
                "ReadWriteMany",
                "2",
                "Filesystem",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "ReadWriteMany",
                "3",
                "Block",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd-thick",
                "ReadWriteMany",
                "4",
                "Block",
                marks=[skipif_ocp_version("<4.9")],
            ),
            pytest.param(
                "ocs-storagecluster-cephfs",
                "ReadWriteOnce",
                "10",
                "Filesystem",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "ReadWriteOnce",
                "11",
                "Block",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd-thick",
                "ReadWriteOnce",
                "12",
                "Block",
                marks=[skipif_ocp_version("<4.9")],
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd",
                "ReadWriteOnce",
                "13",
                "Filesystem",
            ),
            pytest.param(
                "ocs-storagecluster-ceph-rbd-thick",
                "ReadWriteOnce",
                "4",
                "Filesystem",
                marks=[skipif_ocp_version("<4.9")],
            ),
        ],
    )
    def test_create_resize_delete_pvc(
        self,
        project_factory,
        teardown_factory,
        setup_ui,
        sc_name,
        access_mode,
        pvc_size,
        vol_mode,
    ):
        """
        Test create, resize and delete pvc via UI

        """
        # Creating a test project via CLI
        pro_obj = project_factory()
        project_name = pro_obj.namespace

        pvc_ui_obj = PvcUI(setup_ui)

        # Creating PVC via UI
        pvc_name = create_unique_resource_name("test", "pvc")
        pvc_ui_obj.create_pvc_ui(
            project_name, sc_name, pvc_name, access_mode, pvc_size, vol_mode
        )

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvc = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]

        assert pvc[0].size == int(pvc_size), (
            f"size error| expected size:{pvc_size} \n "
            f"actual size:{str(pvc[0].size)}"
        )

        assert pvc[0].get_pvc_access_mode == access_mode, (
            f"access mode error| expected access mode:{access_mode} "
            f"\n actual access mode:{pvc[0].get_pvc_access_mode}"
        )

        assert pvc[0].backed_sc == sc_name, (
            f"storage class error| expected storage class:{sc_name} "
            f"\n actual storage class:{pvc[0].backed_sc}"
        )

        assert pvc[0].get_pvc_vol_mode == vol_mode, (
            f"volume mode error| expected volume mode:{vol_mode} "
            f"\n actual volume mode:{pvc[0].get_pvc_vol_mode}"
        )

        # Verifying PVC via UI
        logger.info("Verifying PVC Details via UI")
        pvc_ui_obj.verify_pvc_ui(
            pvc_size=pvc_size,
            access_mode=access_mode,
            vol_mode=vol_mode,
            sc_name=sc_name,
            pvc_name=pvc_name,
            project_name=project_name,
        )
        logger.info("PVC Details Verified via UI..!!")

        # Creating Pod via CLI
        logger.info("Creating Pod")
        if sc_name in (
            constants.DEFAULT_STORAGECLASS_RBD_THICK,
            constants.DEFAULT_STORAGECLASS_RBD,
        ):
            interface_type = constants.CEPHBLOCKPOOL
        else:
            interface_type = constants.CEPHFILESYSTEM

        new_pod = helpers.create_pod(
            interface_type=interface_type,
            pvc_name=pvc_name,
            namespace=project_name,
            raw_block_pv=vol_mode == constants.VOLUME_MODE_BLOCK,
        )

        logger.info(f"Waiting for Pod: state= {constants.STATUS_RUNNING}")
        wait_for_resource_state(resource=new_pod, state=constants.STATUS_RUNNING)

        # Calling the Teardown Factory Method to make sure Pod is deleted
        teardown_factory(new_pod)

        # Expanding the PVC
        logger.info("Pvc Resizing")
        new_size = int(pvc_size) + 3
        pvc_ui_obj.pvc_resize_ui(
            pvc_name=pvc_name, new_size=new_size, project_name=project_name
        )

        assert new_size > int(
            pvc_size
        ), f"New size of the PVC cannot be less than existing size: new size is {new_size})"

        ocp_version = get_ocp_version()
        self.pvc_loc = locators[ocp_version]["pvc"]

        # Verifying PVC expansion
        logger.info("Verifying PVC resize")
        expected_capacity = f"{new_size} GiB"
        pvc_resize = pvc_ui_obj.verify_pvc_resize_ui(
            project_name=project_name,
            pvc_name=pvc_name,
            expected_capacity=expected_capacity,
        )

        assert pvc_resize, "PVC resize failed"
        logger.info(
            "Pvc resize verified..!!"
            f"New Capacity after PVC resize is {expected_capacity}"
        )

        # Running FIO
        logger.info("Execute FIO on a Pod")
        if vol_mode == constants.VOLUME_MODE_BLOCK:
            storage_type = constants.WORKLOAD_STORAGE_TYPE_BLOCK
        else:
            storage_type = constants.WORKLOAD_STORAGE_TYPE_FS

        new_pod.run_io(storage_type, size=(new_size - 1), invalidate=0, rate="1000m")

        get_fio_rw_iops(new_pod)
        logger.info("FIO execution on Pod successfully completed..!!")

        # Checking if the Pod is deleted or not
        new_pod.delete(wait=True)
        new_pod.ocp.wait_for_delete(resource_name=new_pod.name)

        # Deleting the PVC via UI
        logger.info(f"Delete {pvc_name} pvc")
        pvc_ui_obj.delete_pvc_ui(pvc_name, project_name)

        pvc[0].ocp.wait_for_delete(pvc_name, timeout=120)

        pvc_objs = get_all_pvc_objs(namespace=project_name)
        pvcs = [pvc_obj for pvc_obj in pvc_objs if pvc_obj.name == pvc_name]
        if len(pvcs) > 0:
            assert f"PVC {pvcs[0].name} does not deleted"
