import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    green_squad,
    encryption_at_rest_required,
    skipif_kms_deployment,
    skipif_external_mode,
)
from ocs_ci.helpers.keyrotation_helper import (
    NoobaaKeyrotation,
    OSDKeyrotation,
    KeyRotation,
)

from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry


log = logging.getLogger(__name__)


@skipif_external_mode
@encryption_at_rest_required
@skipif_kms_deployment
@green_squad
@tier1
class TestEncryptionKeyrotation:
    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Resetting the default value of KeyRotation
        """

        def finalizer():
            kr_obj = KeyRotation()
            kr_obj.set_keyrotation_defaults()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-5790")
    def test_osd_keyrotation(self):
        """
        Test to verify the key rotation of the OSD

        Steps:
            1. Disable Keyrotation and verify its disable status at rook and storagecluster end.
            2. Record existing OSD keys before rotation is happen.
            3. Enable Keyrotation and verify its enable status at rook and storagecluster end.
            4. Set keyrotation status to every 3 minutes.
            5. wait for 3 minute.
            6. Verify the keyrotation is happen for each osd by comparing the old keys with new keys.
            7. Change the keyrotation value to default.
        """
        osd_keyrotation = OSDKeyrotation()
        osd_keyrotation.set_keyrotation_defaults()

        # Disable keyrotation and verify its enable status at rook and storagecluster end.
        log.info("Disabling the Keyrotation in storagecluster Spec.")
        osd_keyrotation.disable_keyrotation()

        assert (
            not osd_keyrotation.is_keyrotation_enable()
        ), "Keyrotation is Not Disable in the storagecluster object"
        assert (
            not osd_keyrotation.is_osd_keyrotation_enabled()
        ), "KeyRotation is not Disable in the Rook Object."

        # Recored existing OSD keys before rotation is happen.
        osd_keys_before_rotation = {}
        for device in osd_keyrotation.deviceset:
            osd_keys_before_rotation[device] = osd_keyrotation.get_osd_dm_crypt(device)

        # Enable Keyrotation and verify its enable status at rook and storagecluster end.
        log.info("Enabling the Keyrotation in storagecluster Spec.")
        osd_keyrotation.enable_keyrotation()

        assert (
            osd_keyrotation.is_keyrotation_enable()
        ), "Keyrotation is Not enabled in the storagecluster object"
        assert (
            osd_keyrotation.is_osd_keyrotation_enabled()
        ), "KeyRotation is not enabled in the Rook Object."

        # Set Key Rotation schedule to every 3 minutes.
        schedule = "*/3 * * * *"
        osd_keyrotation.set_keyrotation_schedule(schedule)

        # Verify Keyrotation schedule changed at storagecluster object and rook object.
        assert (
            osd_keyrotation.get_keyrotation_schedule() == schedule
        ), "Keyrotation schedule is not set to 3 minutes."
        assert (
            osd_keyrotation.get_osd_keyrotation_schedule() == schedule
        ), "KeyRotation is not enabled in the Rook Object."

        # Wait for 3 minuted and verify the keyrotation is happen for each osd by comparing the old keys with new keys.
        log.info(
            "Waiting for 3 minutes to verify the keyrotation is happen for each osd."
        )

        @retry(UnexpectedBehaviour, tries=10, delay=20)
        def compare_old_with_new_keys():
            for device in osd_keyrotation.deviceset:
                osd_keys_after_rotation = osd_keyrotation.get_osd_dm_crypt(device)
                log.info(
                    f"Fetching New Key for device {device}: {osd_keys_after_rotation}"
                )
                if osd_keys_before_rotation[device] == osd_keys_after_rotation:
                    log.info(f"Keyrotation Still not happend for device {device}")
                    raise UnexpectedBehaviour(
                        f"Keyrotation is not happened for the device {device}"
                    )
                log.info(f"Keyrotation is happend for device {device}")
            return True

        # with pytest.raises(UnexpectedBehaviour):
        try:
            compare_old_with_new_keys()
        except UnexpectedBehaviour:
            log.error("Key rotation is Not happend after schedule is passed. ")
            assert False

        # Change the keyrotation value to default.
        log.info("Changing the keyrotation value to default.")
        osd_keyrotation.set_keyrotation_schedule("@weekly")

    @pytest.mark.polarion_id("OCS-5791")
    def test_noobaa_keyrotation(self):
        """
        Test to verify the keyrotation for noobaa.

        Steps:
            1. Disable Keyrotation and verify its Disable status at noobaa and storagecluster end.
            3. Record existing NooBaa Volume and backend keys before rotation is happen.
            4. Set keyrotation status to every 3 minutes.
            5. wait for 3 minute.
            6. Verify the keyrotation is happen NooBaa volume and backend keys
                by comparing the old keys with new keys.
            7. Change the keyrotation value to default.
        """

        # Get the noobaa object.
        noobaa_keyrotation = NoobaaKeyrotation()
        noobaa_keyrotation.set_keyrotation_defaults()

        # Disable keyrotation and verify its disable status at noobaa and storagecluster end.
        noobaa_keyrotation.disable_keyrotation()

        assert (
            not noobaa_keyrotation.is_keyrotation_enable()
        ), "Keyrotation is not disabled."
        assert (
            not noobaa_keyrotation.is_noobaa_keyrotation_enable()
        ), "Keyrotation is not disabled."

        # Recoard Noobaa volume and backend keys before rotation.
        (
            old_noobaa_backend_key,
            old_noobaa_backend_secret,
        ) = noobaa_keyrotation.get_noobaa_backend_secret()
        log.info(
            f" Noobaa backend secrets before Rotation {old_noobaa_backend_key} : {old_noobaa_backend_secret}"
        )

        (
            old_noobaa_volume_key,
            old_noobaa_volume_secret,
        ) = noobaa_keyrotation.get_noobaa_volume_secret()
        log.info(
            f"Noobaa Volume secrets before Rotation {old_noobaa_volume_key} : {old_noobaa_volume_secret}"
        )

        # Enable Keyrotation and verify its enable status at Noobaa and storagecluster end.
        noobaa_keyrotation.enable_keyrotation()

        assert (
            noobaa_keyrotation.is_keyrotation_enable
        ), "Keyrotation is not enabled in the storagecluster object."
        assert (
            noobaa_keyrotation.is_noobaa_keyrotation_enable
        ), "Keyrotation is not enabled in the noobaa object."

        # Set keyrotatiojn schedule to every 3 minutes.
        schedule = "*/3 * * * *"
        noobaa_keyrotation.set_keyrotation_schedule(schedule)

        # Verify keyrotation is set for every 3 minute in storagecluster and noobaa object.
        assert (
            noobaa_keyrotation.get_keyrotation_schedule() == schedule
        ), "Keyrotation schedule is not set to every 3 minutes in storagecluster object."
        assert (
            noobaa_keyrotation.get_noobaa_keyrotation_schedule() == schedule
        ), "Keyrotation schedule is not set to every 3 minutes in Noobaa object."

        @retry(UnexpectedBehaviour, tries=10, delay=20)
        def compare_old_keys_with_new_keys():
            """
            Compare old keys with new keys.
            """
            (
                new_noobaa_backend_key,
                new_noobaa_backend_secret,
            ) = noobaa_keyrotation.get_noobaa_backend_secret()
            (
                new_noobaa_volume_key,
                new_noobaa_volume_secret,
            ) = noobaa_keyrotation.get_noobaa_volume_secret()

            if new_noobaa_backend_key == old_noobaa_backend_key:
                raise UnexpectedBehaviour("Noobaa Key Rotation is not happend")

            if new_noobaa_volume_key == old_noobaa_volume_key:
                raise UnexpectedBehaviour("Noobaa Key Rotation is not happend.")

            log.info(
                f"Noobaa Backend key rotated {new_noobaa_backend_key} : {new_noobaa_backend_secret}"
            )
            log.info(
                f"Noobaa Volume key rotated {new_noobaa_volume_key} : {new_noobaa_volume_secret}"
            )

        try:
            compare_old_keys_with_new_keys()
        except UnexpectedBehaviour:
            log.info("Noobaa Key Rotation is not happend.")
            assert False

        # Change the keyrotation value to default.
        log.info("Changing the keyrotation value to default.")
        noobaa_keyrotation.set_keyrotation_schedule("@weekly")
