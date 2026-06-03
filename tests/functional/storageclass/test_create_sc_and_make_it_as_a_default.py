import pytest
import logging
from ocs_ci.ocs import ocp
from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import ManageTest, tier2
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from tests.fixtures import create_project

logger = logging.getLogger(__name__)


@green_squad
@tier2
@pytest.mark.usefixtures(
    create_project.__name__,
)
class TestCreateStorageClassandMakeItAsDefault(ManageTest):
    """
    Verifies that a storageclass can be made as default
    storageclass and pvc based on this storageclass
    can be successfully used to create an app pod and
    run IOs from it.
    """

    @pytest.mark.parametrize(
        argnames="interface_type",
        argvalues=[
            pytest.param(
                *[constants.CEPHBLOCKPOOL], marks=pytest.mark.polarion_id("OCS-626")
            ),
            pytest.param(
                *[constants.CEPHFILESYSTEM], marks=pytest.mark.polarion_id("OCS-627")
            ),
        ],
    )
    def test_create_sc_and_make_it_as_a_default(
        self, interface_type, storageclass_factory, pvc_factory, pod_factory
    ):
        """
        Test function which verifies the above class
        """
        logger.test_step("Get the initial default StorageClass")
        initial_default_sc = helpers.get_default_storage_class()

        logger.test_step(f"Create a {interface_type} StorageClass")
        sc_obj = storageclass_factory(interface=interface_type)
        logger.info(
            f"{interface_type} StorageClass: {sc_obj.name} created successfully"
        )

        logger.test_step(f"Change the default StorageClass to {sc_obj.name}")
        helpers.change_default_storageclass(scname=sc_obj.name)
        tmp_default_sc = helpers.get_default_storage_class()
        logger.assertion(
            f"Number of default storage classes: expected='1', actual='{len(tmp_default_sc)}'"
        )
        assert len(tmp_default_sc) == 1, "More than 1 default storage class exist"
        logger.info(f"Current Default StorageClass is: {tmp_default_sc[0]}")
        logger.assertion(
            f"Default StorageClass: expected='{sc_obj.name}', actual='{tmp_default_sc[0]}'"
        )
        assert tmp_default_sc[0] == sc_obj.name, "Failed to change default StorageClass"
        logger.info(f"Successfully changed the default StorageClass to {sc_obj.name}")

        logger.test_step(f"Create a PVC using default StorageClass {sc_obj.name}")
        pvc_obj = pvc_factory(interface=interface_type)
        logger.info(f"PVC: {pvc_obj.name} created successfully using {sc_obj.name}")

        logger.test_step(f"Create an app pod and mount {pvc_obj.name}")
        pod_obj = pod_factory(interface=interface_type)
        logger.info(f"{pod_obj.name} created successfully and mounted {pvc_obj.name}")

        logger.test_step(f"Run IO on {pod_obj.name}")
        pod_obj.run_io("fs", size="2G")
        get_fio_rw_iops(pod_obj)

        logger.test_step("Restore the initial default StorageClass")
        # Currently we are not setting default SC after deployment
        # hence handling the initial_default_sc None case
        # This check can be removed once the default sc is set
        if len(initial_default_sc) != 0:
            helpers.change_default_storageclass(initial_default_sc[0])
            end_default_sc = helpers.get_default_storage_class()
            logger.info(f"Current Default StorageClass is: {end_default_sc[0]}")
            logger.assertion(
                f"Restored default StorageClass: expected='{initial_default_sc[0]}', "
                f"actual='{end_default_sc[0]}'"
            )
            assert (
                end_default_sc[0] == initial_default_sc[0]
            ), "Failed to change back to default StorageClass"
            logger.info(
                f"Successfully changed back to default StorageClass "
                f"{end_default_sc[0]}"
            )
        ocp_obj = ocp.OCP()
        patch = (
            ' \'{"metadata": {"annotations":'
            '{"storageclass.kubernetes.io/is-default-class"'
            ':"false"}}}\' '
        )
        patch_cmd = f"patch storageclass {tmp_default_sc[0]} -p" + patch
        ocp_obj.exec_oc_cmd(command=patch_cmd)
        logger.info(
            "Initially there is no default StorageClass, hence "
            "setting the current default StorageClass to False"
        )
