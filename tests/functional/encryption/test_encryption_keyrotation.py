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
    NoobaaKeyRotation,
    OSDKeyRotation,
    KeyRotationManager,
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

log = logging.getLogger(__name__)


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
            kr_obj = KeyRotationManager()
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
        osd_keyrotation = OSDKeyRotation()
        osd_keyrotation.set_keyrotation_defaults()

        # Disable keyrotation and verify its enable status at rook and storagecluster end.
        log.info("Disabling the Keyrotation in storagecluster Spec.")
        osd_keyrotation.disable_keyrotation()

        assert (
            not osd_keyrotation.is_keyrotation_enabled()
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
            osd_keyrotation.is_keyrotation_enabled()
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
        noobaa_keyrotation = NoobaaKeyRotation()
        noobaa_keyrotation.set_keyrotation_defaults()

        # Disable keyrotation and verify its disable status at noobaa and storagecluster end.
        noobaa_keyrotation.disable_keyrotation()

        assert (
            not noobaa_keyrotation.is_keyrotation_enabled()
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
            noobaa_keyrotation.is_keyrotation_enabled()
        ), "Keyrotation is not enabled in the storagecluster object."
        assert (
            noobaa_keyrotation.is_noobaa_keyrotation_enabled()
        ), "Keyrotation is not enabled in the noobaa object."

        # Set keyrotatiojn schedule to every given minutes.
        schedule = f"*/{minutes} * * * *"
        noobaa_keyrotation.set_keyrotation_schedule(schedule)

        # Verify keyrotation is set for every given minute in storagecluster and noobaa object.
        assert (
            noobaa_keyrotation.get_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in storagecluster object."
        assert (
            noobaa_keyrotation.get_noobaa_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in Noobaa object."

        try:
            compare_noobaa_old_keys_with_new_keys(
                noobaa_keyrotation, old_noobaa_backend_key, old_noobaa_volume_key
            )
        except UnexpectedBehaviour:
            log.info("Noobaa Key Rotation is not happend.")
            assert False

        # Change the keyrotation value to default.
        log.info("Changing the keyrotation value to default.")
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
        self.test_noobaa_keyrotation(minutes=5)

        # object after key rotation to be placed in seperate dir
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
        self.keyrotation = OSDKeyRotation()
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
            log.info(f"Created {len(pods)} pods for interface: {interface}")
            self.all_pods.extend(pods)

        # Perform I/O on all pods using ThreadPoolExecutor
        log.info("Starting I/O operations on all pods.")
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    pod_obj.run_io, storage_type="fs", size="1G", runtime=60
                )
                for pod_obj in self.all_pods
            ]

            log.info("Verifying OSD keyrotation for KMS.")
            assert self.keyrotation.verify_osd_keyrotation_for_kms(
                tries=6, delay=10
            ), "Failed to rotate OSD and NooBaa Keys in KMS."
            log.info("Keyrotation verification successful.")

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
            kr_obj = KeyRotationManager()
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
        noobaa_keyrotation = NoobaaKeyRotation()
        noobaa_keyrotation.set_keyrotation_defaults()

        # Disable keyrotation and verify its disable status at noobaa and storagecluster end.
        noobaa_keyrotation.disable_keyrotation()

        assert (
            not noobaa_keyrotation.is_keyrotation_enabled()
        ), "Keyrotation is not disabled."
        assert (
            not noobaa_keyrotation.is_noobaa_keyrotation_enable()
        ), "Keyrotation is not disabled."

        # Recoard Noobaa volume and backend keys before rotation.
        (
            old_noobaa_backend_key,
            old_noobaa_backend_secret,
        ) = noobaa_keyrotation.get_noobaa_backend_secret(kms_deployment=True)
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
            noobaa_keyrotation.is_keyrotation_enabled()
        ), "Keyrotation is not enabled in the storagecluster object."
        assert (
            noobaa_keyrotation.is_noobaa_keyrotation_enabled()
        ), "Keyrotation is not enabled in the noobaa object."

        # Set keyrotatiojn schedule to every given minutes.
        schedule = f"*/{minutes} * * * *"
        noobaa_keyrotation.set_keyrotation_schedule(schedule)

        sleep(
            120
        )  # adding sleep to compensate the time taken to reflect the schedule on noobaa

        # Verify keyrotation is set for every given minute in storagecluster and noobaa object.
        assert (
            noobaa_keyrotation.get_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in storagecluster object."
        assert (
            noobaa_keyrotation.get_noobaa_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in Noobaa object."

        try:
            compare_noobaa_old_keys_with_new_keys(
                noobaa_keyrotation, old_noobaa_backend_key, old_noobaa_volume_key
            )
        except UnexpectedBehaviour:
            log.info("Noobaa Key Rotation is not happend.")
            assert False

        # Change the keyrotation value to default.
        log.info("Changing the keyrotation value to default.")
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
        self.test_noobaa_keyrotation(minutes=5)

        # object after key rotation to be placed in seperate dir
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
@skipif_external_mode
@skipif_disconnected_cluster
class TestOSDKeyrotationWithKMIP:
    """
    Test OSD key rotation with KMIP provider (Thales CipherTrust Manager).

    This test validates that OSD key rotation works correctly when using
    KMIP as the KMS provider.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        """
        Setup for KMIP key rotation tests.
        Preserves original encryption status and restores after test.
        """
        # Skip if not using KMIP provider
        from ocs_ci.framework.testlib import config

        if (
            config.ENV_DATA.get("KMS_PROVIDER", "").lower()
            != constants.KMIP_KMS_PROVIDER
        ):
            pytest.skip("Test requires KMIP KMS provider")

        self.keyrotation = OSDKeyRotation()
        preserve_encryption_status = (
            self.keyrotation.storagecluster_obj.data["spec"]
            .get("encryption", {})
            .get("keyRotation")
        )
        self.keyrotation.set_keyrotation_schedule("*/2 * * * *")
        self.keyrotation.enable_keyrotation()
        yield

        # Restore original encryption status
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

    @pytest.mark.polarion_id("OCS-6300")
    def test_osd_keyrotation_with_kmip(self, multi_pvc_factory, pod_factory):
        """
        Test OSD key rotation operation with KMIP KMS provider.

        Steps:
            1. Deploy cluster with clusterwide encryption using KMIP
            2. Enable keyrotation and set keyrotation schedule for every 2 minutes
            3. Create multiple PVCs and attach them to pods
            4. Start I/O operations on PVCs
            5. Verify keyrotation operation is happening every 2 minutes
            6. Verify keys are rotated in KMIP server

        """
        log.info("Starting OSD key rotation test with KMIP provider")

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
        log.info("Creating PVCs for CephBlockPool and CephFS interfaces")
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
        self.all_pods = []
        for interface, pvcs in pvc_objects.items():
            log.info(f"Creating pods for {interface} interface")
            pods = create_pods(
                pvc_objs=pvcs,
                pod_factory=pod_factory,
                interface=interface,
                pods_for_rwx=2,  # Create 2 pods for each RWX PVC
                status=constants.STATUS_RUNNING,
            )
            assert pods, f"Failed to create pods for {interface}."
            log.info(f"Created {len(pods)} pods for interface: {interface}")
            self.all_pods.extend(pods)

        # Perform I/O on all pods using ThreadPoolExecutor
        log.info("Starting I/O operations on all pods")
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    pod_obj.run_io, storage_type="fs", size="1G", runtime=60
                )
                for pod_obj in self.all_pods
            ]

            log.info("Verifying OSD keyrotation for KMIP provider")
            assert self.keyrotation.verify_osd_keyrotation_for_kms(
                tries=6, delay=10
            ), "Failed to rotate OSD and NooBaa keys in KMIP KMS"
            log.info("KMIP keyrotation verification successful")

            # Wait for I/O operations to complete
            log.info("Waiting for I/O operations to complete")
            for future in futures:
                future.result()

        log.info("I/O operations completed successfully")

        # Disable Keyrotation
        self.keyrotation.disable_keyrotation()
        log.info("Test completed successfully")


@green_squad
@encryption_at_rest_required
@skipif_external_mode
class TestNoobaaKeyrotationWithKMIP:
    """
    Test NooBaa key rotation with KMIP provider (Thales CipherTrust Manager).

    This test validates that NooBaa backend and volume key rotation works
    correctly when using KMIP as the KMS provider.
    """

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Resetting the default value of KeyRotation after test.
        """
        # Skip if not using KMIP provider
        from ocs_ci.framework.testlib import config

        if (
            config.ENV_DATA.get("KMS_PROVIDER", "").lower()
            != constants.KMIP_KMS_PROVIDER
        ):
            pytest.skip("Test requires KMIP KMS provider")

        def finalizer():
            kr_obj = KeyRotationManager()
            kr_obj.set_keyrotation_defaults()

        request.addfinalizer(finalizer)

    @pytest.mark.polarion_id("OCS-6301")
    @tier1
    def test_noobaa_keyrotation_with_kmip(self, minutes=3):
        """
        Test to verify NooBaa key rotation with KMIP provider.

        Args:
            minutes (int): Minutes after which rotation will happen (default: 3)

        Steps:
            1. Disable Keyrotation and verify its Disable status at noobaa and storagecluster
            2. Record existing NooBaa Volume and backend keys before rotation
            3. Enable Keyrotation and set schedule to every given minutes
            4. Wait for given minutes
            5. Verify keyrotation happened for NooBaa volume and backend keys
            6. Verify new keys exist in KMIP server
            7. Change keyrotation value to default

        """
        log.info("Starting NooBaa key rotation test with KMIP provider")

        # Get the noobaa object
        noobaa_keyrotation = NoobaaKeyRotation()
        noobaa_keyrotation.set_keyrotation_defaults()

        # Disable keyrotation and verify its disable status
        log.info("Disabling keyrotation")
        noobaa_keyrotation.disable_keyrotation()

        assert (
            not noobaa_keyrotation.is_keyrotation_enabled()
        ), "Keyrotation is not disabled in storagecluster"
        assert (
            not noobaa_keyrotation.is_noobaa_keyrotation_enable()
        ), "Keyrotation is not disabled in noobaa"

        # Record Noobaa volume and backend keys before rotation
        log.info("Recording NooBaa keys before rotation")
        (
            old_noobaa_backend_key,
            old_noobaa_backend_secret,
        ) = noobaa_keyrotation.get_noobaa_backend_secret(kms_deployment=True)
        log.info(
            f"NooBaa backend secrets before rotation: {old_noobaa_backend_key} : {old_noobaa_backend_secret}"
        )

        (
            old_noobaa_volume_key,
            old_noobaa_volume_secret,
        ) = noobaa_keyrotation.get_noobaa_volume_secret()
        log.info(
            f"NooBaa volume secrets before rotation: {old_noobaa_volume_key} : {old_noobaa_volume_secret}"
        )

        # Enable Keyrotation and verify its enable status
        log.info("Enabling keyrotation")
        noobaa_keyrotation.enable_keyrotation()

        assert (
            noobaa_keyrotation.is_keyrotation_enabled()
        ), "Keyrotation is not enabled in storagecluster"
        assert (
            noobaa_keyrotation.is_noobaa_keyrotation_enabled()
        ), "Keyrotation is not enabled in noobaa"

        # Set keyrotation schedule to every given minutes
        schedule = f"*/{minutes} * * * *"
        log.info(f"Setting keyrotation schedule to: {schedule}")
        noobaa_keyrotation.set_keyrotation_schedule(schedule)

        # Add sleep to compensate for schedule reflection time
        sleep(120)

        # Verify keyrotation schedule is set correctly
        assert (
            noobaa_keyrotation.get_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in storagecluster"
        assert (
            noobaa_keyrotation.get_noobaa_keyrotation_schedule() == schedule
        ), f"Keyrotation schedule is not set to every {minutes} minutes in noobaa"

        # Verify key rotation occurred
        log.info("Waiting for key rotation to occur and verifying new keys")
        try:
            compare_noobaa_old_keys_with_new_keys(
                noobaa_keyrotation, old_noobaa_backend_key, old_noobaa_volume_key
            )
        except UnexpectedBehaviour:
            log.error("NooBaa key rotation did not occur with KMIP provider")
            assert False, "NooBaa key rotation failed with KMIP provider"

        log.info("NooBaa key rotation with KMIP verified successfully")

        # Change the keyrotation value to default
        log.info("Changing keyrotation value to default")
        noobaa_keyrotation.set_keyrotation_schedule("@weekly")
        log.info("Test completed successfully")

    @pytest.mark.polarion_id("OCS-6302")
    @tier2
    def test_bucket_checksum_with_noobaa_keyrotation_kmip(
        self, mcg_obj, awscli_pod, bucket_factory, test_directory_setup
    ):
        """
        Test to verify bucket data integrity during NooBaa key rotation with KMIP.

        Steps:
            1. Write objects to bucket and get checksum
            2. Perform NooBaa key rotation with KMIP provider
            3. Get checksum of objects after key rotation
            4. Compare old and new checksums to ensure data integrity

        """
        log.info("Starting bucket checksum test with NooBaa key rotation using KMIP")

        data_dir = test_directory_setup.origin_dir
        bucketname = bucket_factory(1)[0].name
        full_object_path = f"s3://{bucketname}"

        # Write test objects to bucket
        log.info(f"Writing test objects to bucket: {bucketname}")
        write_random_test_objects_to_bucket(
            awscli_pod,
            bucketname,
            data_dir,
            mcg_obj=mcg_obj,
            pattern="obj-",
        )

        # Download objects before key rotation
        log.info("Downloading objects before key rotation")
        awscli_pod.exec_cmd_on_pod("mkdir before_keyrotation_dir")
        sync_object_directory(
            awscli_pod,
            full_object_path,
            "before_keyrotation_dir",
            mcg_obj,
        )

        # Perform noobaa keyrotation for 5 minutes
        log.info("Performing NooBaa key rotation with KMIP")
        self.test_noobaa_keyrotation_with_kmip(minutes=5)

        # Download objects after key rotation
        log.info("Downloading objects after key rotation")
        awscli_pod.exec_cmd_on_pod("mkdir after_keyrotation_dir")
        sync_object_directory(
            awscli_pod,
            full_object_path,
            "after_keyrotation_dir",
            mcg_obj,
        )

        # Verify checksum of objects before and after keyrotation
        log.info("Verifying object checksums before and after key rotation")
        verify_s3_object_integrity(
            original_object_path="before_keyrotation_dir/obj-0",
            result_object_path="after_keyrotation_dir/obj-0",
            awscli_pod=awscli_pod,
        )

        log.info("Bucket data integrity verified successfully after KMIP key rotation")
