import base64
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry
from ocs_ci.utility.kms import (
    get_kms_details,
    is_kms_enabled,
    fetch_noobaa_secret_from_vault,
)

log = logging.getLogger(__name__)


class KeyRotation:
    """
    Handles key rotation operations for a storage cluster.
    """

    def __init__(self):
        """
        Initializes KeyRotation object with necessary parameters.
        """
        self.cluster_name = constants.DEFAULT_CLUSTERNAME
        self.resource_name = constants.STORAGECLUSTER
        self.cluster_namespace = config.ENV_DATA["cluster_namespace"]

        if config.DEPLOYMENT["external_mode"]:
            self.cluster_name = constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE

        self.storagecluster_obj = OCP(
            resource_name=self.cluster_name,
            namespace=self.cluster_namespace,
            kind=self.resource_name,
        )

    def set_keyrotation_defaults(self):
        """
        Setting Keyrotation Defaults on the cluster.
        """
        param = '[{"op":"add","path":"/spec/encryption/keyRotation","value":{"schedule":"@weekly"}}]'
        self.storagecluster_obj.patch(params=param, format_type="json")
        self.storagecluster_obj.wait_for_resource(
            constants.STATUS_READY,
            self.storagecluster_obj.resource_name,
            column="PHASE",
            timeout=180,
        )
        self.storagecluster_obj.reload_data()

    def _exec_oc_cmd(self, cmd, **kwargs):
        """
        Executes the given command.

        Args:
            cmd : command to run.

        Returns:
            str: The output of the command.

        Raises:
            CommandFailed: If the command fails.
        """

        try:
            cmd_out = self.storagecluster_obj.exec_oc_cmd(cmd, **kwargs)
        except CommandFailed as ex:
            log.error(f"Error while executing command {cmd}: {ex}")
            raise ex

        return cmd_out

    def get_keyrotation_schedule(self):
        """
        Retrieves the current key rotation schedule for the storage cluster.

        Returns:
            str: The key rotation schedule.
        """
        cmd = f"get {self.resource_name}  -o jsonpath='{{.items[*].spec.encryption.keyRotation.schedule}}'"
        schedule = self._exec_oc_cmd(cmd, out_yaml_format=False)
        log.info(f"Keyrotation schedule set to {schedule} in storagecluster spec.")
        return schedule

    def set_keyrotation_schedule(self, schedule):
        """
        Sets the key rotation schedule for the storage cluster.

        Args:
            schedule (str): The new key rotation schedule.
        """
        param = f'[{{"op":"replace","path":"/spec/encryption/keyRotation/schedule","value":"{schedule}"}}]'
        self.storagecluster_obj.patch(params=param, format_type="json")
        self.storagecluster_obj.wait_for_resource(
            constants.STATUS_READY,
            self.storagecluster_obj.resource_name,
            column="PHASE",
            timeout=180,
        )
        self.storagecluster_obj.reload_data()
        if self.get_keyrotation_schedule() == schedule:
            log.info(f"Storagecluster keyrotation schedule is set as {schedule}")

    def enable_keyrotation(self):
        """
        Enables key rotation for the storage cluster.

        Returns:
            bool: True if key rotation is enabled, False otherwise.
        """
        param = '[{"op": "add", "path": "/spec/encryption/keyRotation/enable", "value": true}]'
        self.storagecluster_obj.patch(params=param, format_type="json")
        resource_status = self.storagecluster_obj.wait_for_resource(
            constants.STATUS_READY,
            self.storagecluster_obj.resource_name,
            column="PHASE",
            timeout=180,
        )

        if not resource_status:
            log.info(
                f"StorageCluster resource is not reach to state {constants.STATUS_READY}"
            )
            return False

        self.storagecluster_obj.reload_data()
        log.info("Keyrotation is enabled in storegeclujster object.")
        return True

    def disable_keyrotation(self):
        """
        Disables key rotation for the storage cluster.

        Returns:
            bool: True if key rotation is disabled, False otherwise.
        """
        if not self.is_keyrotation_enable():
            log.info("Keyrotation is Already in  Disabled state.")
            return True

        param = '[{"op":"replace","path":"/spec/encryption/keyRotation/enable","value":False}]'
        self.storagecluster_obj.patch(params=param, format_type="json")
        resource_status = self.storagecluster_obj.wait_for_resource(
            constants.STATUS_READY,
            self.storagecluster_obj.resource_name,
            column="PHASE",
            timeout=180,
        )

        if not resource_status:
            log.info(
                f"StorageCluster resource is not reach to state {constants.STATUS_READY}"
            )
            return False

        self.storagecluster_obj.reload_data()
        log.info("Keyrotation is Disabled in storagecluster object.")
        return True

    def is_keyrotation_enable(self):
        """
        Checks if key rotation is enabled for the storage cluster.

        Returns:
            bool: True if key rotation is enabled, False otherwise.
        """
        cmd = f" get {self.resource_name}  -o jsonpath='{{.items[*].spec.encryption.keyRotation.enable}}'"
        cmd_out = self._exec_oc_cmd(cmd)
        if (cmd_out is True) or (cmd_out is None):
            log.info("Keyrotation in storagecluster object is enabled.")
            return True
        log.info("Keyrotation in storagecluster object is not enabled.")
        return False


class NoobaaKeyrotation(KeyRotation):
    """
    Extends KeyRotation class to handle key rotation operations for Noobaa.
    """

    def __init__(self):
        """
        Initializes NoobaaKeyrotation object.
        """
        super().__init__()
        self.kms = get_kms_details()
        self.kms.gather_init_vault_conf()
        self.kms.update_vault_env_vars()
        self.kms.get_vault_backend_path()

    def get_noobaa_keyrotation_schedule(self):
        """
        Retrieves the current key rotation schedule for Noobaa.

        Returns:
            str: The key rotation schedule for Noobaa.

        Raises:
            ValueError: If the key rotation schedule is not found or is invalid.
        """
        cmd = " get noobaas.noobaa.io -o jsonpath='{.items[*].spec.security.kms.schedule}'"

        cmd_out = self._exec_oc_cmd(cmd=cmd, out_yaml_format=False)
        if cmd_out == "":
            raise ValueError("Noobaa Keyrotation schedule is not found.")
        log.info(f"Noobaa Keyrotation schedule: {cmd_out}")
        return cmd_out.strip()

    def is_noobaa_keyrotation_enable(self):
        """
        Checks if key rotation is enabled for Noobaa.

        Returns:
            bool: True if key rotation is enabled for Noobaa, False otherwise.
        """
        cmd = " get noobaas.noobaa.io -o jsonpath='{.items[*].spec.security.kms.enableKeyRotation}'"

        cmd_out = self._exec_oc_cmd(cmd=cmd)
        if cmd_out == "true":
            log.info("Noobaa Keyrotation is Enabled.")
            return True
        log.info("Noobaa Keyrotation is disabled.")
        return False

    def get_noobaa_backend_secret(self, kms_deployment=False):
        """
        Retrieves the backend secret for Noobaa.

        kms_deployment: Boolean: Sets as False, if True it will check NOOBAA_BACKEND_SECRET in vault

        Returns:
            tuple (str, str): containing the Noobaa backend root key and secret.

        Raises:
            ValueError: If failed to retrieve the backend secret.
        """
        if kms_deployment:
            (noobaa_backend_root_key, noobaa_backend_secret) = (
                fetch_noobaa_secret_from_vault(self.kms.vault_backend_path)
            )
        else:
            cmd = (
                f" get secret {constants.NOOBAA_BACKEND_SECRET} -o jsonpath='{{.data}}'"
            )
            cmd_out = self._exec_oc_cmd(cmd=cmd)
            noobaa_backend_root_key = base64.b64decode(
                cmd_out["active_root_key"]
            ).decode()
            noobaa_backend_secret = cmd_out[noobaa_backend_root_key]
        log.info(
            f"Noobaa Backend root key : {noobaa_backend_root_key}, Noobaa backend secrets : {noobaa_backend_secret}"
        )
        return noobaa_backend_root_key, noobaa_backend_secret

    def get_noobaa_volume_secret(self):
        """
        Retrieves the volume secret for Noobaa.

        Returns:
            tuple (str, str): containing the Noobaa volume root key and secret.

        Raises:
            ValueError: If failed to retrieve the volume secret.
        """
        cmd = f" get secret {constants.NOOBAA_VOLUME_SECRET} -o jsonpath='{{.data}}'"

        cmd_out = self._exec_oc_cmd(cmd=cmd)
        noobaa_volume_root_key = base64.b64decode(cmd_out["active_root_key"]).decode()
        noobaa_volume_secret = cmd_out[noobaa_volume_root_key]
        log.info(
            f"Noobaa volume root key: {noobaa_volume_root_key},  Noobaa Volume sceret: {noobaa_volume_secret}"
        )
        return noobaa_volume_root_key, noobaa_volume_secret


class OSDKeyrotation(KeyRotation):
    """
    Extends KeyRotation class to handle key rotation operations for Rook.
    """

    def __init__(self):
        """
        Initializes RookKeyrotation object.
        """
        super().__init__()
        self.deviceset = self._get_deviceset()

        # get the kms config for the OSD keyrotation
        if is_kms_enabled(dont_raise=True) and (
            config.ENV_DATA.get("KMS_PROVIDER")
            in [
                constants.VAULT_KMS_PROVIDER,
                constants.HPCS_KMS_PROVIDER,
                constants.KMIP_KMS_PROVIDER,
            ]
        ):
            self.kms = get_kms_details()
            # Vault and HPCS require vault-specific configuration
            if config.ENV_DATA.get("KMS_PROVIDER") in [
                constants.VAULT_KMS_PROVIDER,
                constants.HPCS_KMS_PROVIDER,
            ]:
                self.kms.gather_init_vault_conf()
                self.kms.update_vault_env_vars()
            # KMIP requires different configuration
            elif config.ENV_DATA.get("KMS_PROVIDER") == constants.KMIP_KMS_PROVIDER:
                self.kms.update_kmip_env_vars()

    def _get_deviceset(self):
        """
        Listing deviceset for OSD.
        """
        return [pvc.name for pvc in get_deviceset_pvcs()]

    def enable_osd_keyrotatio(self):
        """Enable OSD keyrotation in storagecluster Spec.

        Returns:
            bool: True if keyrotation is Enabled otherwise False
        """
        return self.enable_keyrotation()

    def is_osd_keyrotation_enabled(self):
        """
        Checks if key rotation is enabled for OSD.

        Returns:
            bool: True if key rotation is enabled for OSD, False otherwise.
        """
        cmd = " get cephclusters.ceph.rook.io -o jsonpath='{.items[].spec.security.keyRotation.enabled}'"

        cmd_out = self._exec_oc_cmd(cmd=cmd)
        if cmd_out:
            log.info("OSD keyrotation is Enabled.")
            return True
        log.info("OSD keyrotation is Disabled.")
        return False

    def get_osd_keyrotation_schedule(self):
        """
        Retrieves the key rotation schedule for OSD.

        Returns:
            str: The key rotation schedule for OSD.
        """
        cmd = " get cephclusters.ceph.rook.io -o jsonpath='{.items[].spec.security.keyRotation.schedule}'"

        schedule = self._exec_oc_cmd(cmd=cmd, out_yaml_format=False)
        log.info(f"OSD keyrotation schedule set as {schedule}")
        return schedule

    def get_osd_dm_crypt(self, device):
        """
        Retrieves the dmcrypt key for OSD.

        Args:
            device (str): The OSD device name.

        Returns:
            str: The dmcrypt key for the specified OSD device.

        """
        cmd = f" get secret rook-ceph-osd-encryption-key-{device} -o jsonpath='{{.data.dmcrypt-key}}'"

        dmcrypt_key = self._exec_oc_cmd(cmd=cmd, out_yaml_format=False)
        log.info(f"dmcrypt-key of device {device} is {dmcrypt_key}")
        return dmcrypt_key

    def verify_keyrotation(self, old_keys, tries=10, delay=20):
        """
        Verify Keyrotation is suceeded for all OSD devices.

        Args:
            old_keys (dict): osd devices and their keys.

        Returns:
            bool: True if all OSD keyrotation is happend, orherwise False.
        """
        log.info("Verifying OSD keyrotation is happening")

        @retry(UnexpectedBehaviour, tries=tries, delay=delay)
        def compare_old_with_new_keys():
            for device in self._get_deviceset():
                osd_keys_after_rotation = self.get_osd_dm_crypt(device)
                log.info(
                    f"Fetching New Key for device {device}: {osd_keys_after_rotation}"
                )
                if old_keys[device] == osd_keys_after_rotation:
                    log.info(f"Keyrotation Still not happend for device {device}")
                    raise UnexpectedBehaviour(
                        f"Keyrotation is not happened for the device {device}"
                    )
                log.info(f"Keyrotation is happend for device {device}")
            return True

        try:
            compare_old_with_new_keys()
        except UnexpectedBehaviour:
            log.error("Key rotation is Not happend after schedule is passed. ")
            assert False

        log.info("Keyrotation is sucessfully done for the all OSD.")
        return True

    def verify_osd_keyrotation_for_kms(self, tries=10, delay=10):
        """Verify OSD KeyRotation for Vault KMS

        Returns:
            bool: return True If KeyRotation is sucessfull otherwise False.
        """

        old_keys = {}

        for dev in self.deviceset:
            old_keys[dev] = self.kms.get_osd_secret(dev)

        # Noobaa Secret
        old_keys[constants.NOOBAA_BACKEND_SECRET] = self.kms.get_noobaa_secret()

        log.info(f"OSD and NooBaa keys before Rotation : {old_keys}")

        @retry(UnexpectedBehaviour, tries=tries, delay=delay)
        def compare_keys():
            new_keys = {}
            for dev in self.deviceset:
                new_keys[dev] = self.kms.get_osd_secret(dev)

            new_keys[constants.NOOBAA_BACKEND_SECRET] = self.kms.get_noobaa_secret()

            unmatched_keys = []
            for key in old_keys:
                if old_keys[key] == new_keys[key]:
                    log.info(f"Vault key for {key} is not yet rotated ")
                    unmatched_keys.append(key)

            if unmatched_keys:
                raise UnexpectedBehaviour(
                    f"These component keys are not rotated in vault : {','.join(unmatched_keys)}"
                )

            log.info(f"New OSD and Noobaa keys are rotated : {new_keys}")

        try:
            compare_keys()
        except UnexpectedBehaviour:
            log.info("OSD and Noobaa  Keys are not rotated.")
            return False

        return True


class PVKeyrotation(KeyRotation):
    def __init__(self, sc_obj):
        self.sc_obj = sc_obj
        self.kms = get_kms_details()
        self.kms_provider = config.ENV_DATA.get("KMS_PROVIDER", "vault").lower()
        self.all_pvc_key_data = None

        log.info(f"Initialized PVKeyrotation with KMS provider: {self.kms_provider}")

    def annotate_storageclass_key_rotation(self, schedule="@weekly"):
        """
        Annotate Storageclass To enable keyrotation for encrypted PV
        """
        annot_str = f"keyrotation.csiaddons.openshift.io/schedule='{schedule}'"
        log.info(f"Adding annotation to the storage class:  {annot_str}")
        self.sc_obj.annotate(annotation=annot_str)

    @retry(UnexpectedBehaviour, tries=5, delay=20)
    def compare_keys(self, device_handle, old_key):
        """
        Compares the current key with the rotated key.

        Args:
            device_handle (str): The handle or identifier for the device.
            old_key (str): The current key before rotation.

        Returns:
            bool: True if the key has rotated successfully.

        Raises:
            UnexpectedBehaviour: If the keys have not rotated.
        """
        rotated_key = self.kms.get_pv_secret(device_handle)
        if old_key == rotated_key:
            raise UnexpectedBehaviour(
                f"Keys are not rotated for device handle {device_handle}"
            )
        log.info(f"PV key rotated with new key : {rotated_key}")
        return True

    def wait_till_keyrotation(self, device_handle):
        """
        Waits until the key rotation occurs for a given device handle.

        Args:
            device_handle (str): The handle or identifier for the device whose key
                rotation is to be checked.

        Returns:
            bool: True if the key rotation is successful, otherwise False.
        """
        old_key = self.kms.get_pv_secret(device_handle)
        try:
            self.compare_keys(device_handle, old_key)
        except UnexpectedBehaviour:
            log.error(f"Keys are not rotated for device handle {device_handle}")
            assert False

        return True

    def set_keyrotation_state_by_annotation(self, enable: bool):
        """
        Enables or disables key rotation by annotating the StorageClass.
        """
        state = "true" if enable else "false"
        annotation = f"keyrotation.csiaddons.openshift.io/enable={state}"
        self.sc_obj.annotate(annotation=annotation)
        log.info(
            f"Key rotation {'enabled' if enable else 'disabled'} for the StorageClass."
        )

    def set_keyrotation_state_by_rbac_user(self, pvc_obj, suspend_state=True):
        """
        Updates key rotation CronJob state for a PVC.
        """
        cron_job = self.get_keyrotation_cronjob_for_pvc(pvc_obj)
        state = "unmanaged" if suspend_state else "managed"
        cron_job.annotate(f"csiaddons.openshift.io/state={state}", overwrite=True)

        log.info(f"Updated CronJob annotation for PVC '{pvc_obj.name}' to '{state}'")

        suspend_patch = (
            '[{"op": "add", "path": "/spec/suspend", "value": true}]'
            if suspend_state
            else '[{"op": "remove", "path": "/spec/suspend"}]'
        )
        cron_job.patch(params=suspend_patch, format_type="json")
        log.info(f"'suspend' {'enabled' if suspend_state else 'removed'} for CronJob.")

    def get_keyrotation_cronjob_for_pvc(self, pvc_obj):
        """
        Retrieves the key rotation CronJob associated with a PVC.

        Args:
            pvc_obj (object): The PVC object for which to retrieve the CronJob.

        Returns:
            object: The CronJob object associated with the PVC.

        Raises:
            ValueError: If the PVC lacks the key rotation CronJob annotation.
        """
        # Ensure annotations are loaded in the PVC object
        if "annotations" not in pvc_obj.data["metadata"]:
            pvc_obj.reload()

        # Extract the cronjob name from PVC annotations
        cron_job_name = (
            pvc_obj.data["metadata"]
            .get("annotations", {})
            .get("keyrotation.csiaddons.openshift.io/cronjob")
        )

        if not cron_job_name:
            log.error(f"PVC '{pvc_obj.name}' lacks keyrotation cronjob annotation.")
            raise ValueError(f"Missing keyrotation cronjob for PVC '{pvc_obj.name}'")

        log.info(f"Found CronJob '{cron_job_name}' for PVC '{pvc_obj.name}'.")

        cronjob_obj = OCP(
            kind=constants.ENCRYPTIONKEYROTATIONCRONJOB,
            namespace=pvc_obj.namespace,
            resource_name=cron_job_name,
        )

        if not cronjob_obj.is_exist():
            log.error(
                f"cronjob {cron_job_name} is not exists for the PVC: {pvc_obj.name}"
            )
            raise ValueError(
                f"Missing keyrotation cronjob Object for PVC '{pvc_obj.name}'"
            )

        return OCP(
            kind=constants.ENCRYPTIONKEYROTATIONCRONJOB,
            namespace=pvc_obj.namespace,
            resource_name=cron_job_name,
        )

    def get_pvc_keys_data(self, pvc_objs):
        """
        Retrieves key data for PVCs.

        Returns a dictionary with PVC names as keys and their encryption key details.
        The key name in the dict is generic ('kms_key') to support different KMS providers.
        """
        key_field_name = "kms_key"  # Generic name for KMS key/secret

        return {
            pvc.name: {
                "device_handle": pvc.get_pv_volume_handle_name,
                key_field_name: self.kms.get_pv_secret(pvc.get_pv_volume_handle_name),
            }
            for pvc in pvc_objs
        }

    @retry(UnexpectedBehaviour, tries=10, delay=20)
    def wait_till_all_pv_keyrotation_on_vault_kms(self, pvc_objs):
        """
        Waits for all PVC keys to be rotated in the KMS.

        Supports multiple KMS providers: Vault, HPCS, and KMIP.

        Args:
            pvc_objs (list): List of PVC objects to check for key rotation

        Returns:
            bool: True if all keys have been rotated successfully

        Raises:
            UnexpectedBehaviour: If keys have not rotated within the retry period
        """
        if not self.all_pvc_key_data:
            self.all_pvc_key_data = self.get_pvc_keys_data(pvc_objs)
            raise UnexpectedBehaviour(
                f"Initializing PVC key data for {self.kms_provider}"
            )

        new_pvc_keys = self.get_pvc_keys_data(pvc_objs)
        if self.all_pvc_key_data == new_pvc_keys:
            raise UnexpectedBehaviour(
                f"PVC keys have not rotated yet on {self.kms_provider}."
            )

        log.info(f"PVC keys rotated successfully on {self.kms_provider}.")
        return True

    def change_pvc_keyrotation_cronjob_state(self, pvc_objs, disable=True):
        """
        Modify the key rotation state of PVCs by annotating and patching their associated cronjobs.

        Args:
            pvc_objs (list): List of PVC objects to modify.
            disable (bool): If True, disables the key rotation. If False, enables it. Defaults to True.

        Returns:
            bool: True if the operation succeeds.
        """
        state_value = "unmanaged" if disable else "managed"

        for pvc in pvc_objs:
            # Retrieve the cronjob associated with the PVC
            cronjob = self.get_keyrotation_cronjob_for_pvc(pvc)
            if not cronjob:
                log.warning(
                    f"No KeyRotationCronjob found for PVC '{pvc.name}'. Skipping."
                )
                continue

            # Annotate the cronjob to reflect the new state
            state_annotation = f"csiaddons.openshift.io/state={state_value}"
            cronjob.annotate(state_annotation, overwrite=True)
            log.info(
                f"Annotated KeyRotationCronjob for PVC '{pvc.name}' with state: {state_value}."
            )

            # Prepare the patch for suspending or resuming the cronjob
            if disable:
                suspend_patch = (
                    '[{"op": "add", "path": "/spec/suspend", "value": true}]'
                )
                log.info(
                    f"'suspend' set to True in KeyRotationCronjob for PVC '{pvc.name}'."
                )
            else:
                suspend_patch = '[{"op": "remove", "path": "/spec/suspend"}]'
                log.info(
                    f"'suspend' removed from KeyRotationCronjob for PVC '{pvc.name}'."
                )

            # Apply the patch to the cronjob
            try:
                cronjob.patch(params=suspend_patch, format_type="json")
                log.info(
                    f"Successfully patched KeyRotationCronjob for PVC '{pvc.name}'."
                )
            except Exception as e:
                log.error(
                    f"Failed to patch KeyRotationCronjob for PVC '{pvc.name}': {e}"
                )
                raise
            pvc.reload()

        log.info("Completed key rotation state changes for all specified PVCs.")
        return True

    @retry(UnexpectedBehaviour, tries=10, delay=10)
    def wait_for_keyrotation_cronjobs_recreation(self, pvc_objs):
        """
        Wait for key rotation cronjobs to be recreated for all PVCs after re-enabling.

        Args:
            pvc_objs (list): List of PVC objects to check.

        Returns:
            bool: True if all cronjobs are recreated and active.

        Raises:
            UnexpectedBehaviour: If cronjobs are not recreated within timeout.
        """
        missing_cronjobs = []

        for pvc_obj in pvc_objs:
            # Reload PVC to get latest annotations
            pvc_obj.reload()

            try:
                cronjob = self.get_keyrotation_cronjob_for_pvc(pvc_obj)
                # Check if cronjob exists and is not suspended
                if not cronjob.is_exist():
                    missing_cronjobs.append(pvc_obj.name)
                    continue

                # Check if cronjob is not suspended
                cronjob_data = cronjob.get()
                if cronjob_data.get("spec", {}).get("suspend", False):
                    missing_cronjobs.append(f"{pvc_obj.name} (suspended)")
                    continue

                log.info(f"Key rotation cronjob for PVC '{pvc_obj.name}' is active.")

            except ValueError:
                # Cronjob annotation not found or cronjob doesn't exist
                missing_cronjobs.append(pvc_obj.name)

        if missing_cronjobs:
            raise UnexpectedBehaviour(
                f"Key rotation cronjobs not ready for PVCs: {', '.join(missing_cronjobs)}"
            )

        log.info("All key rotation cronjobs are recreated and active.")
        return True


def validate_key_rotation_schedules(schedule):
    """
    Validate key rotation schedules across different components.

    Args:
        schedule (str): The expected key rotation schedule.

    Raises:
        ValueError: If the schedule does not match in any of the components.
    """
    log.info(f"Starting key rotation schedule validation for schedule: {schedule}.")

    components = [
        ("Storage Cluster", OSDKeyrotation().get_keyrotation_schedule),
        ("Rook Object", OSDKeyrotation().get_osd_keyrotation_schedule),
        ("NooBaa Object", NoobaaKeyrotation().get_noobaa_keyrotation_schedule),
    ]

    for name, get_schedule in components:
        current_schedule = get_schedule()
        if current_schedule != schedule:
            raise ValueError(
                f"{name} key rotation schedule mismatch: expected {schedule}, got {current_schedule}."
            )
        log.info(f"{name} key rotation schedule verified successfully.")

    log.info("Key rotation schedule validation completed successfully.")
    return True


def verify_new_key_after_rotation(tries, delays):
    """
    This function records existing keys for OSD, Noobaa volume and backend
    and compare with the new keys generated on given schedule.

    """
    osd_keyrotation = OSDKeyrotation()
    noobaa_keyrotation = NoobaaKeyrotation()

    log.info("Record existing OSD keys before rotation is happened.")
    osd_keys_before_rotation = {}
    for device in osd_keyrotation.deviceset:
        osd_keys_before_rotation[device] = osd_keyrotation.get_osd_dm_crypt(device)

    log.info("Record Noobaa volume and backend keys before rotation.")
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

    @retry(UnexpectedBehaviour, tries=tries, delay=delays)
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
        osd_keyrotation.verify_keyrotation(osd_keys_before_rotation, tries=10, delay=30)
        compare_old_keys_with_new_keys()

    except UnexpectedBehaviour:
        log.error("Key rotation is Not happened after schedule is passed. ")
        assert False


@retry(UnexpectedBehaviour, tries=10, delay=20)
def compare_noobaa_old_keys_with_new_keys(
    noobaa_keyrotation, old_noobaa_backend_key, old_noobaa_volume_key
):
    """
    Compare noobaa old keys with new keys.
    args:
        noobaa_keyrotation: obj: NoobaaKeyrotation object
        old_noobaa_backend_key: str: old noobaa backend key
        old_noobaa_volume_key: str: old noobaa_volume_key

    """
    (
        new_noobaa_backend_key,
        new_noobaa_backend_secret,
    ) = noobaa_keyrotation.get_noobaa_backend_secret(kms_deployment=True)
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
