import base64
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.retry import retry
from ocs_ci.utility.kms import get_kms_details

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
            self.cluster_name_name = constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE

        self.storagecluster_obj = OCP(
            resource_name=self.cluster_name,
            namespace=self.cluster_namespace,
            kind=self.resource_name,
        )

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
        if self.is_keyrotation_enable():
            log.info("Keyrotation is Already in Enabled state.")
            return True

        param = '{"spec":{"encryption":{"keyRotation":{"enable":null}}}}'
        self.storagecluster_obj.patch(
            params=param, format_type="merge", resource_name=self.cluster_name
        )
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

    def get_noobaa_backend_secret(self):
        """
        Retrieves the backend secret for Noobaa.

        Returns:
            tuple (str, str): containing the Noobaa backend root key and secret.

        Raises:
            ValueError: If failed to retrieve the backend secret.
        """
        cmd = f" get secret {constants.NOOBAA_BACKEND_SECRET} -o jsonpath='{{.data}}'"

        cmd_out = self._exec_oc_cmd(cmd=cmd)
        noobaa_backend_root_key = base64.b64decode(cmd_out["active_root_key"]).decode()
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

    def _get_deviceset(self):
        """
        Listing deviceset for OSD.
        """
        return [pvc.name for pvc in get_deviceset_pvcs()]

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


class PVKeyrotation(KeyRotation):
    def __init__(self, sc_obj):
        self.sc_obj = sc_obj
        self.kms = get_kms_details()

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
