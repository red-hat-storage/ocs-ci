import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import tier1, skipif_lvm_not_installed
from ocs_ci.framework.testlib import skipif_ocs_version, ManageTest, acceptance
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import LVM
from ocs_ci.ocs.exceptions import ResourceWrongStatusException

logger = logging.getLogger(__name__)


@tier1
@acceptance
@skipif_lvm_not_installed
@skipif_ocs_version("<4.11")
class TestLvmOverProvisioning(ManageTest):
    """
    Test lvm snapshot bigger than disk

    """

    @pytest.mark.parametrize(
        argnames=["volume_mode", "volume_binding_mode"],
        argvalues=[
            pytest.param(
                *[
                    constants.VOLUME_MODE_FILESYSTEM,
                    constants.IMMEDIATE_VOLUMEBINDINGMODE,
                ],
                marks=pytest.mark.polarion_id("OCS-4469"),
            ),
            pytest.param(
                *[constants.VOLUME_MODE_BLOCK, constants.IMMEDIATE_VOLUMEBINDINGMODE],
                marks=pytest.mark.polarion_id("OCS-4469"),
            ),
        ],
    )
    def test_lvm_over_over_provisioning(
        self,
        volume_mode,
        volume_binding_mode,
        project_factory,
        lvm_storageclass_factory,
        pvc_factory,
    ):
        """
        test create delete snapshot
        .* Check overprovisioning value
        .* Check thin pool size
        .* Create PVCs till you reach overprovisioning
        .* Check for error

        """

        access_mode = constants.ACCESS_MODE_RWO

        lvm = LVM(fstrim=True, fail_on_thin_pool_not_empty=True)
        thin_pool_size = lvm.get_thin_pool1_size()

        overprovision_ratio = lvm.get_lvm_thin_pool_config_overprovision_ratio()
        size_to_go_beyond_provision = float(overprovision_ratio) * float(thin_pool_size)
        number_of_legit_pvc = 2
        pvc_size = size_to_go_beyond_provision / number_of_legit_pvc - 100

        proj_obj = project_factory()

        sc_obj = lvm_storageclass_factory(volume_binding_mode)

        status = constants.STATUS_PENDING
        if volume_binding_mode == constants.IMMEDIATE_VOLUMEBINDINGMODE:
            status = constants.STATUS_BOUND
        pvc_obj_list = []
        for pvc_number_creation in range(0, number_of_legit_pvc + 1):
            try:
                pvc_obj_list.append(
                    pvc_factory(
                        project=proj_obj,
                        interface=None,
                        storageclass=sc_obj,
                        size=pvc_size,
                        status=status,
                        access_mode=access_mode,
                        volume_mode=volume_mode,
                    )
                )

            except ResourceWrongStatusException as er:
                if len(pvc_obj_list) == number_of_legit_pvc:
                    logger.info(
                        f"PVC creation should fail here because we passed overprovsioning "
                        f"{overprovision_ratio} which should allow us {number_of_legit_pvc}."
                        f"This last successful PVC number was {len(pvc_obj_list)}. The error message {er}"
                        f"suppose to happen"
                    )
                else:
                    raise ResourceWrongStatusException(
                        "We have not reached the limit of "
                        "overprovisioning and PVC has failed to "
                        "be created. Please debug."
                    )
