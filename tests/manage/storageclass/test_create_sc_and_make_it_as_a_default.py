import pytest
import logging
from ocs_ci.ocs import ocp
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.ocs.resources.pod import get_fio_rw_iops
from tests.fixtures import create_project

log = logging.getLogger(__name__)


@tier1
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
                *[
                    constants.CEPHBLOCKPOOL
                ], marks=pytest.mark.polarion_id("OCS-626")
            ),
            pytest.param(
                *[
                    constants.CEPHFILESYSTEM
                ], marks=pytest.mark.polarion_id("OCS-627")
            )
        ]
    )
    def test_create_sc_and_make_it_as_a_default(
        self, interface_type, storageclass_factory,
        pvc_factory, pod_factory
    ):
        """
        Test function which verifies the above class
        """
        # Get default StorageClass
        initial_default_sc = ocp.get_default_storage_class()
        log.info(f"Current default StorageClass is:{initial_default_sc}")

        # Create a Storage Class
        sc_obj = storageclass_factory(interface=interface_type)
        log.info(
            f"{interface_type}StorageClass: {sc_obj.name} "
            f"created successfully"
        )

        # Change the above created StorageClass to default
        log.info(
            f"Changing the default StorageClass from {initial_default_sc}"
            f" to {sc_obj.name}"
        )
        ocp.change_default_storageclass(scname=sc_obj.name)
        # Confirm that the default StorageClass is changed
        tmp_default_sc = ocp.get_default_storage_class()
        log.info(f"Current Default StorageClass is:{tmp_default_sc}")
        assert tmp_default_sc == sc_obj.name, (
            "Failed to change default StorageClass"
        )
        log.info(
            f"Successfully changed the default StorageClass to "
            f"{sc_obj.name}"
        )

        # Create a PVC using the default StorageClass
        log.info(f"Creating a PVC using {sc_obj.name}")
        pvc_obj = pvc_factory(interface=interface_type)
        log.info(
            f"PVC: {pvc_obj.name} created successfully using "
            f"{sc_obj.name}"
        )

        # Create app pod and mount each PVC
        log.info(f"Creating an app pod and mount {pvc_obj.name}")
        pod_obj = pod_factory(interface=interface_type)
        log.info(
            f"{pod_obj.name} created successfully and mounted {pvc_obj.name}"
        )

        # Run IO on each app pod for sometime
        log.info(f"Running FIO on {pod_obj.name}")
        pod_obj.run_io('fs', size='2G')
        get_fio_rw_iops(pod_obj)

        # Switch back to initial default storageclass

        # Currently we are not setting default SC after deployment
        # hence handling the initial_default_sc None case
        # This check can be removed once the default sc is set
        if initial_default_sc is not None:
            ocp.change_default_storageclass(initial_default_sc)
            # Confirm that the default StorageClass is changed
            end_default_sc = ocp.get_default_storage_class()
            log.info(f"Current Default StorageClass is:{tmp_default_sc}")
            assert end_default_sc == initial_default_sc, (
                "Failed to change back to default StorageClass"
            )
            log.info(
                f"Successfully changed back to default StorageClass "
                f"{end_default_sc}"
            )
        ocp_obj = ocp.OCP()
        patch = " '{\"metadata\": {\"annotations\":" \
                "{\"storageclass.kubernetes.io/is-default-class\"" \
                ":\"false\"}}}' "
        patch_cmd = f"patch storageclass {tmp_default_sc} -p" + patch
        ocp_obj.exec_oc_cmd(command=patch_cmd)
        log.info(
            "Initially there is no default StorageClass, hence "
            "setting the current default StorageClass to False"
        )
