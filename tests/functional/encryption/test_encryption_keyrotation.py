import logging
import pytest
import json
from time import sleep

from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    tier2,
    green_squad,
    encryption_at_rest_required,
    skipif_kms_deployment,
    skipif_external_mode,
    vault_kms_deployment_required,
)
from ocs_ci.framework.testlib import skipif_disconnected_cluster
from ocs_ci.helpers.keyrotation_helper import (
    NoobaaKeyrotation,
    OSDKeyrotation,
    KeyRotation,
    compare_noobaa_old_keys_with_new_keys,
)

from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_pods
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.ocs.bucket_utils import (
    sync_object_directory,
    write_random_test_objects_to_bucket,
    verify_s3_object_integrity,
)

logger = logging.getLogger(__name__)


@skipif_external_mode
@encryption_at_rest_required
@skipif_kms_deployment
@green_squad
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
    @tier1
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
        logger.test_step("Disable OSD keyrotation and verify disabled status")
        osd_keyrotation.disable_keyrotation()

        logger.assertion(
            f"Keyrotation disabled in storagecluster: expected='False', "
            f"actual='{osd_keyrotation.is_keyrotation_enable()}'"
        )
        assert (
            not osd_keyrotation.is_keyrotation_enable()
        ), "Keyrotation is Not Disable in the storagecluster object"
        logger.assertion(
            f"Keyrotation disabled in Rook: expected='False', actual='{osd_keyrotation.is_osd_keyrotation_enabled()}'"
        )
        assert (
            not osd_keyrotation.is_osd_keyrotation_enabled()
        ), "KeyRotation is not Disable in the Rook Object."

        # Record existing OSD keys before rotation is happen.
        logger.test_step("Record existing OSD keys before rotation")
        osd_keys_before_rotation = {}
        for device in osd_keyrotation.deviceset:
            osd_keys_before_rotation[device] = osd_keyrotation.get_osd_dm_crypt(device)

        # Enable Keyrotation and verify its enable status at rook and storagecluster end.
        logger.test_step("Enable OSD keyrotation and verify enabled status")
        osd_keyrotation.enable_keyrotation()

        logger.assertion(
            f"Keyrotation enabled in storagecluster: expected='True', "
            f"actual='{osd_keyrotation.is_keyrotation_enable()}'"
        )
        assert (
            osd_keyrotation.is_keyrotation_enable()
        ), "Keyrotation is Not enabled in the storagecluster object"
        logger.assertion(
            f"Keyrotation enabled in Rook: expected='True', actual='{osd_keyrotation.is_osd_keyrotation_enabled()}'"
        )
        assert (
            osd_keyrotation.is_osd_keyrotation_enabled()
        ), "KeyRotation is not enabled in the Rook Object."

        # Set Key Rotation schedule to every 3 minutes.
        logger.test_step("Set keyrotation schedule to every 3 minutes and verify")
        schedule = "*/3 * * * *"
        osd_keyrotation.set_keyrotation_schedule(schedule)

        # Verify Keyrotation schedule changed at storagecluster object and rook object.
        logger.assertion(
            f"Keyrotation schedule in storagecluster: expected='{schedule}', "
            f"actual='{osd_keyrotation.get_keyrotation_schedule()}'"
        )
        assert (
            osd_keyrotation.get_keyrotation_schedule() == schedule
        ), "Keyrotation schedule is not set to 3 minutes."
        logger.assertion(
            f"Keyrotation schedule in Rook: expected='{schedule}', "
            f"actual='{osd_keyrotation.get_osd_keyrotation_schedule()}'"
        )
        assert (
            osd_keyrotation.get_osd_keyrotation_schedule() == schedule
        ), "KeyRotation is not enabled in the Rook Object."

        # Wait for 3 minutes and verify the keyrotation is happen for each osd by comparing the old keys with new keys.
        logger.test_step(
            "Wait for keyrotation and verify OSD keys changed for each device"
        )

        @retry(UnexpectedBehaviour, tries=10, delay=20)
        def compare_old_with_new_keys():
            for device in osd_keyrotation.deviceset:
                osd_keys_after_rotation = osd_keyrotation.get_osd_dm_crypt(device)
                logger.debug(
                    f"Fetching New Key for device {device}: {osd_keys_after_rotation}"
                )
                if osd_keys_before_rotation[device] == osd_keys_after_rotation:
                    logger.debug(f"Keyrotation still not happened for device {device}")
                    raise UnexpectedBehaviour(
                        f"Keyrotation is not happened for the device {device}"
                    )
                logger.debug(f"Keyrotation happened for device {device}")
            return True

        try:
            compare_old_with_new_keys()
        except UnexpectedBehaviour:
            logger.exception("Key rotation did not happen after schedule passed")
            assert False

        # Change the keyrotation value to default.
        logger.test_step("Reset keyrotation schedule to default (@weekly)")
        osd_keyrotation.set_keyrotation_schedule("@weekly")

    @pytest.mark.polarion_id("OCS-5791")
    @tier1
    def test_noobaa_keyrotation(self, minutes=3):
        """
        Test to verify the keyrotation for noobaa.
        args:
            minutes: int: Minutes after which rotation will happen.

        Steps:
            1. Disable Keyrotation and verify its Disable status at noobaa and storagecluster end.
            2. Record existing NooBaa Volume and backend keys before rotation is happen.
            3. Set keyrotation status to every given minutes (default is 3 min)
            4. wait for given minute (default is 3 min).
            5. Verify the keyrotation is happen NooBaa volume and backend keys
                by comparing the old keys with new keys.
            6. Change the keyrotation value to default.
        """

        # Get the noobaa object.
        logger.test_step("Disable NooBaa keyrotation and verify disabled status")
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

        # Record Noobaa volume and backend keys before rotation.
        logger.test_step("Record NooBaa backend and volume keys before rotation")
        (
            old_noobaa_backend_key,
            old_noobaa_backend_secret,
        ) = noobaa_keyrotation.get_noobaa_backend_secret()
        logger.debug(
            f"Noobaa backend secrets before Rotation {old_noobaa_backend_key} : {old_noobaa_backend_secret}"
        )

        (
            old_noobaa_volume_key,
            old_noobaa_volume_secret,
        ) = noobaa_keyrotation.get_noobaa_volume_secret()
        logger.debug(
            f"Noobaa Volume secrets before Rotation {old_noobaa_volume_key} : {old_noobaa_volume_secret}"
        )

        # Enable Keyrotation and verify its enable status at Noobaa and storagecluster end.
        logger.test_step("Enable NooBaa keyrotation and set schedule")
        noobaa_keyrotation.enable_keyrotation()

        assert (
            noobaa_keyrotation.is_keyrotation_enable
        ), "Keyrotation is not enabled in the storagecluster object."
        assert (
            noobaa_keyrotation.is_noobaa_keyrotation_enable
        ), "Keyrotation is not enabled in the noobaa object."

        # Set keyrotation schedule to every given minutes.
        schedule = f"*/{minutes} * * * *"
        noobaa_keyrotation.set_keyrotation_schedule(schedule)

        # Verify keyrotation is set for every given minute in storagecluster and noobaa object.
        logger.test_step(
            f"Verify keyrotation schedule is set to every {minutes} minutes"
        )
        assert (
            noobaa_keyrotation.get_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in storagecluster object."
        assert (
            noobaa_keyrotation.get_noobaa_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in Noobaa object."

        logger.test_step("Compare old NooBaa keys with new keys after rotation")
        try:
            compare_noobaa_old_keys_with_new_keys(
                noobaa_keyrotation, old_noobaa_backend_key, old_noobaa_volume_key
            )
        except UnexpectedBehaviour:
            logger.warning("Noobaa Key Rotation did not happen.")
            assert False

        # Change the keyrotation value to default.
        logger.info("Changing the keyrotation value to default.")
        noobaa_keyrotation.set_keyrotation_schedule("@weekly")

    @pytest.mark.polarion_id("OCS-5963")
    @tier2
    def test_bucket_checksum_with_noobaa_keyrotation(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Test to verify the keyrotation for noobaa.

        Steps:
            1. Write object on bucket and get checksum
            2. Disable Keyrotation and verify its Disable status at noobaa and storagecluster end.
            3. Record existing NooBaa Volume and backend keys before rotation is happen.
            4. Set keyrotation status to every 5 minutes.
            5. wait for 5 minute.
            6. Verify the keyrotation is happen NooBaa volume and backend keys
                by comparing the old keys with new keys.
            7. Change the keyrotation value to default.
            8. Get checksum of the object after key rotation
            9. Compate old and new checksums are same.
        """
        logger.test_step("Write test objects to bucket and sync before keyrotation")
        data_dir = test_directory_setup.origin_dir
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"

        write_random_test_objects_to_bucket(
            awscli_pod,
            bucketname,
            data_dir,
            mcg_obj=mcg_obj,
            pattern="obj-",
        )
        # object before key rotation to be placed in seperate dir
        awscli_pod.exec_cmd_on_pod("mkdir before_keyrotation_dir")
        sync_object_directory(
            awscli_pod,
            full_object_path,
            "before_keyrotation_dir",
            mcg_obj,
        )

        # do noobaa keyrotation for 5 minutes
        logger.test_step("Perform NooBaa keyrotation with 5-minute schedule")
        self.test_noobaa_keyrotation(minutes=5)

        # object after key rotation to be placed in seperate dir
        logger.test_step("Sync objects after keyrotation and verify data integrity")
        awscli_pod.exec_cmd_on_pod("mkdir after_keyrotation_dir")
        sync_object_directory(
            awscli_pod,
            full_object_path,
            "after_keyrotation_dir",
            mcg_obj,
        )

        # checking checksum of objects before and after keyrotation
        verify_s3_object_integrity(
            original_object_path="before_keyrotation_dir/obj-0",
            result_object_path="after_keyrotation_dir/obj-0",
            awscli_pod=awscli_pod,
        ),


@green_squad
@tier1
@encryption_at_rest_required
@vault_kms_deployment_required
@skipif_external_mode
@skipif_disconnected_cluster
class TestOSDKeyrotationWithKMS:
    @pytest.fixture(autouse=True)
    def setup(
        self,
    ):
        self.keyrotation = OSDKeyrotation()
        preserve_encryption_status = (
            self.keyrotation.storagecluster_obj.data["spec"]
            .get("encryption")
            .get("keyRotation")
        )
        self.keyrotation.set_keyrotation_schedule("*/2 * * * *")
        self.keyrotation.enable_keyrotation()
        yield
        if preserve_encryption_status:
            param = json.dumps(
                [
                    {
                        "op": "add",
                        "path": "/spec/encryption/keyRotation",
                        "value": preserve_encryption_status,
                    }
                ]
            )
            self.keyrotation.storagecluster_obj.patch(params=param, format_type="json")
            self.keyrotation.storagecluster_obj.wait_for_resource(
                constants.STATUS_READY,
                self.keyrotation.storagecluster_obj.resource_name,
                column="PHASE",
                timeout=180,
            )

    def test_osd_keyrotation_with_kms(self, multi_pvc_factory, pod_factory):
        """Test OSD KEyrotation operation for vault KMS.

        Steps:
            1. Deploy cluster with clusterwide encryption
            2. enable keyrotation and set keyrotation schedule for every 2 minute.
            3. create a multiple PVC and attach it to the pod.
            4. start IO's on PVC to pods.
            5. Verify keyrotation operation happening for every 2 minutes.

        """
        logger.test_step(
            "Create PVCs for CephBlockPool and CephFS with multiple access modes"
        )
        size = 5
        access_modes = {
            constants.CEPHBLOCKPOOL: [
                f"{constants.ACCESS_MODE_RWO}-Block",
                f"{constants.ACCESS_MODE_RWX}-Block",
            ],
            constants.CEPHFILESYSTEM: [
                constants.ACCESS_MODE_RWO,
                constants.ACCESS_MODE_RWX,
            ],
        }

        # Create PVCs for CephBlockPool and CephFS
        pvc_objects = {
            interface: multi_pvc_factory(
                interface=interface,
                access_modes=modes,
                size=size,
                num_of_pvc=2,
            )
            for interface, modes in access_modes.items()
        }

        # Create pods for each interface
        logger.test_step("Create pods for each interface and start IO")
        self.all_pods = []
        for interface, pvcs in pvc_objects.items():
            pods = create_pods(
                pvc_objs=pvcs,
                pod_factory=pod_factory,
                interface=interface,
                pods_for_rwx=2,  # Create 2 pods for each RWX PVC
                status=constants.STATUS_RUNNING,
            )
            assert pods, f"Failed to create pods for {interface}."
            logger.info(f"Created {len(pods)} pods for interface: {interface}")
            self.all_pods.extend(pods)

        # Perform I/O on all pods using ThreadPoolExecutor
        logger.test_step("Run IO on all pods and verify OSD keyrotation for KMS")
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    pod_obj.run_io, storage_type="fs", size="1G", runtime=60
                )
                for pod_obj in self.all_pods
            ]

            logger.info("Verifying OSD keyrotation for KMS.")
            logger.assertion("OSD keyrotation for KMS: expected='successful rotation'")
            assert self.keyrotation.verify_osd_keyrotation_for_kms(
                tries=6, delay=10
            ), "Failed to rotate OSD and NooBaa Keys in KMS."
            logger.info("Keyrotation verification successful.")

            # Wait for I/O operations to complete
            for future in futures:
                future.result()

        # Disable Keyrotation
        self.keyrotation.disable_keyrotation()


@green_squad
@encryption_at_rest_required
@vault_kms_deployment_required
@skipif_external_mode
class TestNoobaaKeyrotationWithKMS:
    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Resetting the default value of KeyRotation
        """

        def finalizer():
            kr_obj = KeyRotation()
            kr_obj.set_keyrotation_defaults()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-5791")
    @tier1
    def test_noobaa_keyrotation(self, minutes=3):
        """
        Test to verify the keyrotation for noobaa.
        args:
            minutes: int: Minutes after which rotation will happen.

        Steps:
            1. Disable Keyrotation and verify its Disable status at noobaa and storagecluster end.
            2. Record existing NooBaa Volume and backend keys before rotation is happen.
            3. Set keyrotation status to every given minutes (default is 3 min)
            4. wait for given minute (default is 3 min).
            5. Verify the keyrotation is happen NooBaa volume and backend keys
                by comparing the old keys with new keys.
            6. Change the keyrotation value to default.
        """

        # Get the noobaa object.
        logger.test_step(
            "Disable NooBaa keyrotation with KMS and verify disabled status"
        )
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

        # Record Noobaa volume and backend keys before rotation.
        logger.test_step("Record NooBaa backend and volume keys before rotation (KMS)")
        (
            old_noobaa_backend_key,
            old_noobaa_backend_secret,
        ) = noobaa_keyrotation.get_noobaa_backend_secret(kms_deployment=True)
        logger.debug(
            f"Noobaa backend secrets before Rotation {old_noobaa_backend_key} : {old_noobaa_backend_secret}"
        )

        (
            old_noobaa_volume_key,
            old_noobaa_volume_secret,
        ) = noobaa_keyrotation.get_noobaa_volume_secret()
        logger.debug(
            f"Noobaa Volume secrets before Rotation {old_noobaa_volume_key} : {old_noobaa_volume_secret}"
        )

        # Enable Keyrotation and verify its enable status at Noobaa and storagecluster end.
        logger.test_step("Enable NooBaa keyrotation with KMS and set schedule")
        noobaa_keyrotation.enable_keyrotation()

        assert (
            noobaa_keyrotation.is_keyrotation_enable
        ), "Keyrotation is not enabled in the storagecluster object."
        assert (
            noobaa_keyrotation.is_noobaa_keyrotation_enable
        ), "Keyrotation is not enabled in the noobaa object."

        # Set keyrotation schedule to every given minutes.
        schedule = f"*/{minutes} * * * *"
        noobaa_keyrotation.set_keyrotation_schedule(schedule)

        sleep(
            120
        )  # adding sleep to compensate the time taken to reflect the schedule on noobaa

        # Verify keyrotation is set for every given minute in storagecluster and noobaa object.
        logger.test_step(
            f"Verify keyrotation schedule is set to every {minutes} minutes (KMS)"
        )
        assert (
            noobaa_keyrotation.get_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in storagecluster object."
        assert (
            noobaa_keyrotation.get_noobaa_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in Noobaa object."

        logger.test_step("Compare old NooBaa keys with new keys after rotation (KMS)")
        try:
            compare_noobaa_old_keys_with_new_keys(
                noobaa_keyrotation, old_noobaa_backend_key, old_noobaa_volume_key
            )
        except UnexpectedBehaviour:
            logger.warning("Noobaa Key Rotation did not happen.")
            assert False

        # Change the keyrotation value to default.
        logger.info("Changing the keyrotation value to default.")
        noobaa_keyrotation.set_keyrotation_schedule("@weekly")

    @pytest.mark.polarion_id("OCS-5963")
    @tier2
    def test_bucket_checksum_with_noobaa_keyrotation(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Test to verify the keyrotation for noobaa.

        Steps:
            1. Write object on bucket and get checksum
            2. Disable Keyrotation and verify its Disable status at noobaa and storagecluster end.
            3. Record existing NooBaa Volume and backend keys before rotation is happen.
            4. Set keyrotation status to every 5 minutes.
            5. wait for 5 minute.
            6. Verify the keyrotation is happen NooBaa volume and backend keys
                by comparing the old keys with new keys.
            7. Change the keyrotation value to default.
            8. Get checksum of the object after key rotation
            9. Compate old and new checksums are same.
        """
        logger.test_step(
            "Write test objects to bucket and sync before keyrotation (KMS)"
        )
        data_dir = test_directory_setup.origin_dir
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"

        write_random_test_objects_to_bucket(
            awscli_pod,
            bucketname,
            data_dir,
            mcg_obj=mcg_obj,
            pattern="obj-",
        )
        # object before key rotation to be placed in seperate dir
        awscli_pod.exec_cmd_on_pod("mkdir before_keyrotation_dir")
        sync_object_directory(
            awscli_pod,
            full_object_path,
            "before_keyrotation_dir",
            mcg_obj,
        )

        # do noobaa keyrotation for 5 minutes
        logger.test_step("Perform NooBaa keyrotation with 5-minute schedule (KMS)")
        self.test_noobaa_keyrotation(minutes=5)

        # object after key rotation to be placed in seperate dir
        logger.test_step(
            "Sync objects after keyrotation and verify data integrity (KMS)"
        )
        awscli_pod.exec_cmd_on_pod("mkdir after_keyrotation_dir")
        sync_object_directory(
            awscli_pod,
            full_object_path,
            "after_keyrotation_dir",
            mcg_obj,
        )

        # checking checksum of objects before and after keyrotation
        verify_s3_object_integrity(
            original_object_path="before_keyrotation_dir/obj-0",
            result_object_path="after_keyrotation_dir/obj-0",
            awscli_pod=awscli_pod,
        ),
