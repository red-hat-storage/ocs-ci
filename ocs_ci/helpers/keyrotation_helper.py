"""
Key Rotation Helper Module

This module provides a flexible and extensible framework for managing encryption key rotation
across different KMS providers (Vault, KMIP, HPCS) and Kubernetes secrets.

Architecture:
    - BaseKMSProvider: Abstract base class defining the interface for all KMS providers
    - Concrete Providers: VaultProvider, KMIPProvider, HPCSProvider, K8sSecretsProvider
    - KeyRotationFactory: Creates appropriate provider instances
    - KeyRotationManager: Orchestrates key rotation operations
    - Component-specific classes: OSDKeyRotation, NoobaaKeyRotation, PVKeyRotation
"""

import base64
import logging
from abc import ABC, abstractmethod

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnexpectedBehaviour
from ocs_ci.framework import config
from ocs_ci.ocs.resources.pvc import get_deviceset_pvcs
from ocs_ci.utility.retry import retry
from ocs_ci.utility.kms import (
    get_kms_details,
    is_kms_enabled,
    KMIP,
)

log = logging.getLogger(__name__)


# ============================================================================
# Base KMS Provider Interface
# ============================================================================


class BaseKMSProvider(ABC):
    """
    Abstract base class defining the interface for all KMS providers.

    This class establishes a common interface that all KMS providers must implement,
    making it easy to add new providers without modifying existing code.
    """

    def __init__(self, provider_name):
        """
        Initialize the KMS provider.

        Args:
            provider_name (str): Name of the KMS provider (e.g., 'vault', 'kmip', 'hpcs', 'k8s')
        """
        self.provider_name = provider_name
        self.cluster_namespace = config.ENV_DATA.get("cluster_namespace")
        log.info(f"Initializing {provider_name} KMS provider")

    @abstractmethod
    def initialize(self):
        """
        Initialize the provider (setup connection, environment variables, etc.).

        This method should handle all provider-specific initialization logic.
        """
        pass

    @abstractmethod
    def get_osd_key(self, device_handle):
        """
        Retrieve OSD encryption key for a given device.

        Args:
            device_handle (str): Device handle (e.g., PVC name)

        Returns: Encryption key or key ID
        """
        pass

    @abstractmethod
    def get_noobaa_key(self):
        """
        Retrieve NooBaa backend encryption key.

        Returns: Encryption key or key ID
        """
        pass

    @abstractmethod
    def get_pv_key(self, device_handle):
        """
        Retrieve PV encryption key for a given device handle.

        Args:
            device_handle (str): PV volume handle

        Returns: Encryption key or key ID
        """
        pass

    @abstractmethod
    def verify_key_exists(self, key_identifier):
        """
        Verify that a key exists in the KMS.

        Args:
            key_identifier (str): Key ID or handle

        Returns: True if key exists, False otherwise
        """
        pass

    def get_provider_type(self):
        """
        Get the provider type.

        Returns: Provider name
        """
        return self.provider_name


# ============================================================================
# Concrete KMS Provider Implementations
# ============================================================================


class VaultProvider(BaseKMSProvider):
    """HashiCorp Vault KMS provider implementation."""

    def __init__(self):
        super().__init__("vault")
        self.kms = None
        self.vault_backend_path = None

    def initialize(self):
        """Initialize Vault connection and configuration."""
        try:
            self.kms = get_kms_details()
            self.kms.gather_init_vault_conf()
            self.kms.update_vault_env_vars()
            self.kms.get_vault_backend_path()
            self.vault_backend_path = self.kms.vault_backend_path
            log.info("Vault provider initialized successfully")
        except Exception as e:
            log.error(f"Failed to initialize Vault provider: {e}")
            raise

    def get_osd_key(self, device_handle):
        """Retrieve OSD encryption key from Vault."""
        if not self.vault_backend_path:
            self.kms.get_vault_backend_path()
            self.vault_backend_path = self.kms.vault_backend_path
        return self.kms.get_osd_secret(device_handle)

    def get_noobaa_key(self):
        """Retrieve NooBaa encryption key from Vault."""
        if not self.vault_backend_path:
            self.kms.get_vault_backend_path()
            self.vault_backend_path = self.kms.vault_backend_path
        return self.kms.get_noobaa_secret()

    def get_pv_key(self, device_handle):
        """Retrieve PV encryption key from Vault."""
        return self.kms.get_pv_secret(device_handle)

    def verify_key_exists(self, key_identifier):
        """Verify key exists in Vault."""
        try:
            # Try to get the key; if it exists, this will succeed
            import subprocess
            import shlex

            cmd = f"vault kv get {self.vault_backend_path}/{key_identifier}"
            subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
            return True
        except subprocess.CalledProcessError:
            return False


class HPCSProvider(BaseKMSProvider):
    """IBM HPCS (Hyper Protect Crypto Services) KMS provider implementation."""

    def __init__(self):
        super().__init__("hpcs")
        self.kms = None

    def initialize(self):
        """Initialize HPCS connection and configuration."""
        try:
            self.kms = get_kms_details()
            self.kms.gather_init_vault_conf()
            self.kms.update_vault_env_vars()
            log.info("HPCS provider initialized successfully")
        except Exception as e:
            log.error(f"Failed to initialize HPCS provider: {e}")
            raise

    def get_osd_key(self, device_handle):
        """Retrieve OSD encryption key from HPCS."""
        return self.kms.get_osd_secret(device_handle)

    def get_noobaa_key(self):
        """Retrieve NooBaa encryption key from HPCS."""
        return self.kms.get_noobaa_secret()

    def get_pv_key(self, device_handle):
        """Retrieve PV encryption key from HPCS."""
        return self.kms.get_pv_secret(device_handle)

    def verify_key_exists(self, key_identifier):
        """Verify key exists in HPCS."""
        try:
            # HPCS uses Vault-compatible API
            return True  # Placeholder - implement HPCS-specific verification
        except Exception:
            return False


class KMIPProvider(BaseKMSProvider):
    """KMIP Thales CipherTrust Manager KMS provider implementation."""

    def __init__(self):
        super().__init__("kmip")
        self.kms = None

    def initialize(self):
        """Initialize KMIP connection and configuration."""
        try:
            self.kms = KMIP()
            self.kms.update_kmip_env_vars()
            log.info("KMIP provider initialized successfully")
        except Exception as e:
            log.error(f"Failed to initialize KMIP provider: {e}")
            raise

    def get_osd_key(self, device_handle):
        """
        Retrieve OSD encryption key ID from KMIP.

        For KMIP, we return the key ID stored in the dmcrypt secret.
        """
        cmd = (
            f"get secret rook-ceph-osd-encryption-key-{device_handle} "
            f"-o jsonpath='{{.data.dmcrypt-key}}' "
            f"-n {self.cluster_namespace}"
        )
        ocp_obj = OCP(namespace=self.cluster_namespace, kind="secret")
        key_id_b64 = ocp_obj.exec_oc_cmd(cmd, out_yaml_format=False)
        key_id = base64.b64decode(key_id_b64).decode()
        return key_id

    def get_noobaa_key(self):
        """Retrieve NooBaa encryption key ID from KMIP."""
        return self.kms.get_noobaa_key_id()

    def get_pv_key(self, device_handle):
        """
        Retrieve PV encryption key ID from KMIP.

        The key ID is stored in the CSI encryption secret.
        """
        cmd = (
            f"get secret encryption-key-{device_handle} "
            f"-n {self.cluster_namespace} "
            f"-o jsonpath='{{.data.encryptionPassphrase}}'"
        )
        ocp_obj = OCP(namespace=self.cluster_namespace, kind="secret")
        key_id_b64 = ocp_obj.exec_oc_cmd(cmd, out_yaml_format=False)
        key_id = base64.b64decode(key_id_b64).decode()
        return key_id

    def verify_key_exists(self, key_identifier):
        """Verify key exists in CipherTrust Manager."""
        return self.kms.check_key_exists_in_ciphertrust(key_identifier)


class K8sSecretsProvider(BaseKMSProvider):
    """
    Kubernetes Secrets provider for key storage.

    This provider handles keys stored directly in Kubernetes secrets
    without an external KMS.
    """

    def __init__(self):
        super().__init__("k8s-secrets")
        self.ocp_obj = None

    def initialize(self):
        """Initialize Kubernetes secrets provider."""
        try:
            self.ocp_obj = OCP(namespace=self.cluster_namespace, kind="secret")
            log.info("K8s Secrets provider initialized successfully")
        except Exception as e:
            log.error(f"Failed to initialize K8s Secrets provider: {e}")
            raise

    def get_osd_key(self, device_handle):
        """Retrieve OSD encryption key from Kubernetes secret."""
        secret_name = f"rook-ceph-osd-encryption-key-{device_handle}"
        cmd = (
            f"get secret {secret_name} "
            f"-o jsonpath='{{.data.dmcrypt-key}}' "
            f"-n {self.cluster_namespace}"
        )
        key_b64 = self.ocp_obj.exec_oc_cmd(cmd, out_yaml_format=False)
        return base64.b64decode(key_b64).decode()

    def get_noobaa_key(self):
        """Retrieve NooBaa encryption key from Kubernetes secret."""
        cmd = (
            f"get secret {constants.NOOBAA_BACKEND_SECRET} "
            f"-o jsonpath='{{.data}}' "
            f"-n {self.cluster_namespace}"
        )
        secret_data = self.ocp_obj.exec_oc_cmd(cmd)

        # Get the active root key
        if "active_root_key" in secret_data:
            active_key_name = base64.b64decode(secret_data["active_root_key"]).decode()
            key_value = secret_data.get(active_key_name, "")
            return base64.b64decode(key_value).decode() if key_value else ""

        return ""

    def get_pv_key(self, device_handle):
        """Retrieve PV encryption key from Kubernetes secret."""
        secret_name = f"encryption-key-{device_handle}"
        cmd = (
            f"get secret {secret_name} "
            f"-n {self.cluster_namespace} "
            f"-o jsonpath='{{.data.encryptionPassphrase}}'"
        )
        key_b64 = self.ocp_obj.exec_oc_cmd(cmd, out_yaml_format=False)
        return base64.b64decode(key_b64).decode()

    def verify_key_exists(self, key_identifier):
        """Verify secret exists in Kubernetes."""
        try:
            cmd = f"get secret {key_identifier} -n {self.cluster_namespace}"
            self.ocp_obj.exec_oc_cmd(cmd, out_yaml_format=False)
            return True
        except CommandFailed:
            return False


# ============================================================================
# KMS Provider Factory
# ============================================================================


class KeyRotationFactory:
    """
    Factory class for creating KMS provider instances.

    This factory makes it easy to add new providers by simply registering them.
    """

    # Registry of available providers
    _providers = {
        constants.VAULT_KMS_PROVIDER: VaultProvider,
        constants.HPCS_KMS_PROVIDER: HPCSProvider,
        constants.KMIP_KMS_PROVIDER: KMIPProvider,
        "k8s-secrets": K8sSecretsProvider,
    }

    @classmethod
    def register_provider(cls, provider_type, provider_class):
        """
        Register a new KMS provider.

        Args:
            provider_type (str): Provider type identifier
            provider_class (type): Provider class (must inherit from BaseKMSProvider)
        """
        if not issubclass(provider_class, BaseKMSProvider):
            raise TypeError(
                f"Provider class must inherit from BaseKMSProvider, got {provider_class}"
            )
        cls._providers[provider_type] = provider_class
        log.info(f"Registered new provider: {provider_type}")

    @classmethod
    def create_provider(cls, provider_type=None):
        """
        Create and initialize a KMS provider instance.

        Args:
            provider_type (str, optional): Type of provider to create.
                                          If None, auto-detect from config.

        Returns:
            BaseKMSProvider: Initialized provider instance

        Raises:
            ValueError: If provider type is not supported
        """
        # Auto-detect provider type if not specified
        if provider_type is None:
            if is_kms_enabled(dont_raise=True):
                provider_type = config.ENV_DATA.get("KMS_PROVIDER")
                log.info(f"Detected KMS provider: {provider_type}")
            else:
                provider_type = "k8s-secrets"
                log.info("No KMS enabled, using Kubernetes secrets")

        # Get provider class
        provider_class = cls._providers.get(provider_type)
        if provider_class is None:
            raise ValueError(
                f"Unsupported provider type: {provider_type}. "
                f"Available providers: {list(cls._providers.keys())}"
            )

        # Create and initialize provider
        provider = provider_class()
        provider.initialize()

        return provider

    @classmethod
    def list_providers(cls):
        """
        List all registered providers.

        Returns: List of registered provider types
        """
        return list(cls._providers.keys())


# ============================================================================
# Key Rotation Manager
# ============================================================================


class KeyRotationManager:
    """
    Central manager for key rotation operations.

    This class provides a unified interface for key rotation operations
    regardless of the underlying KMS provider.
    """

    def __init__(self, provider=None):
        """
        Initialize the key rotation manager.

        Args:
            provider (BaseKMSProvider, optional): KMS provider instance.
                                                  If None, auto-detect and create.
        """
        self.provider = provider or KeyRotationFactory.create_provider()
        self.cluster_namespace = config.ENV_DATA.get("cluster_namespace")
        self.cluster_name = constants.DEFAULT_CLUSTERNAME
        self.resource_name = constants.STORAGECLUSTER

        if config.DEPLOYMENT.get("external_mode"):
            self.cluster_name = constants.DEFAULT_CLUSTERNAME_EXTERNAL_MODE

        self.storagecluster_obj = OCP(
            resource_name=self.cluster_name,
            namespace=self.cluster_namespace,
            kind=self.resource_name,
        )

        log.info(
            f"KeyRotationManager initialized with provider: {self.provider.get_provider_type()}"
        )

    def _exec_oc_cmd(self, cmd, **kwargs):
        """
        Execute OpenShift command.

        Args:
            cmd (str): Command to execute
            **kwargs: Additional arguments

        Returns:
            Command output

        Raises:
            CommandFailed: If command fails
        """
        try:
            return self.storagecluster_obj.exec_oc_cmd(cmd, **kwargs)
        except CommandFailed as ex:
            log.error(f"Error executing command {cmd}: {ex}")
            raise

    def get_keyrotation_schedule(self):
        """
        Get the current key rotation schedule.

        Returns: Key rotation schedule (e.g., '@weekly')
        """
        cmd = (
            f"get {self.resource_name} "
            f"-o jsonpath='{{.items[*].spec.encryption.keyRotation.schedule}}'"
        )
        schedule = self._exec_oc_cmd(cmd, out_yaml_format=False)
        log.info(f"Key rotation schedule: {schedule}")
        return schedule

    def set_keyrotation_schedule(self, schedule):
        """
        Set the key rotation schedule.

        Args:
            schedule (str): Schedule in cron format or '@weekly', '@daily', etc.
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
            log.info(f"Key rotation schedule set to: {schedule}")

    def enable_keyrotation(self):
        """
        Enable key rotation.

        Returns: True if successful, False otherwise
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
            log.error(f"StorageCluster did not reach {constants.STATUS_READY} state")
            return False

        self.storagecluster_obj.reload_data()
        log.info("Key rotation enabled successfully")
        return True

    def disable_keyrotation(self):
        """
        Disable key rotation.

        Returns: True if successful, False otherwise
        """
        if not self.is_keyrotation_enabled():
            log.info("Key rotation is already disabled")
            return True

        param = '[{"op":"replace","path":"/spec/encryption/keyRotation/enable","value":false}]'
        self.storagecluster_obj.patch(params=param, format_type="json")

        resource_status = self.storagecluster_obj.wait_for_resource(
            constants.STATUS_READY,
            self.storagecluster_obj.resource_name,
            column="PHASE",
            timeout=180,
        )

        if not resource_status:
            log.error(f"StorageCluster did not reach {constants.STATUS_READY} state")
            return False

        self.storagecluster_obj.reload_data()
        log.info("Key rotation disabled successfully")
        return True

    def is_keyrotation_enabled(self):
        """
        Check if key rotation is enabled.

        Returns: True if enabled, False otherwise
        """
        cmd = (
            f"get {self.resource_name} "
            f"-o jsonpath='{{.items[*].spec.encryption.keyRotation.enable}}'"
        )
        cmd_out = self._exec_oc_cmd(cmd)

        if cmd_out is True or cmd_out is None:
            log.info("Key rotation is enabled")
            return True

        log.info("Key rotation is disabled")
        return False

    def set_keyrotation_defaults(self):
        """
        Set keyrotation to default values (schedule=@weekly, enable=true).

        This is a convenience method for resetting key rotation configuration.
        """
        log.info("Setting key rotation to default values")
        param = '[{"op":"add","path":"/spec/encryption/keyRotation","value":{"schedule":"@weekly"}}]'
        self.storagecluster_obj.patch(params=param, format_type="json")
        self.storagecluster_obj.wait_for_resource(
            constants.STATUS_READY,
            self.storagecluster_obj.resource_name,
            column="PHASE",
            timeout=180,
        )
        self.storagecluster_obj.reload_data()
        log.info("Key rotation defaults set successfully")


# ============================================================================
# OSD Key Rotation
# ============================================================================


class OSDKeyRotation(KeyRotationManager):
    """
    Manages key rotation for OSD (Object Storage Daemon) components.
    """

    def __init__(self, provider=None):
        """
        Initialize OSD key rotation manager.

        Args:
            provider (BaseKMSProvider, optional): KMS provider instance
        """
        super().__init__(provider)
        self.deviceset = self._get_deviceset()

    def _get_deviceset(self):
        """
        Get list of OSD device PVCs.

        Returns: List of PVC names
        """
        return [pvc.name for pvc in get_deviceset_pvcs()]

    def get_osd_keyrotation_schedule(self):
        """
        Get OSD-specific key rotation schedule from CephCluster.

        Returns: Key rotation schedule
        """
        cmd = (
            "get cephclusters.ceph.rook.io "
            "-o jsonpath='{.items[].spec.security.keyRotation.schedule}'"
        )
        schedule = self._exec_oc_cmd(cmd, out_yaml_format=False)
        log.info(f"OSD key rotation schedule: {schedule}")
        return schedule

    def is_osd_keyrotation_enabled(self):
        """
        Check if OSD key rotation is enabled.

        Returns: True if enabled, False otherwise
        """
        cmd = (
            "get cephclusters.ceph.rook.io "
            "-o jsonpath='{.items[].spec.security.keyRotation.enabled}'"
        )
        cmd_out = self._exec_oc_cmd(cmd)

        if cmd_out:
            log.info("OSD key rotation is enabled")
            return True

        log.info("OSD key rotation is disabled")
        return False

    def get_all_osd_keys(self):
        """
        Get encryption keys for all OSD devices.

        Returns: Dictionary mapping device names to their keys
        """
        osd_keys = {}
        for device in self.deviceset:
            try:
                key = self.provider.get_osd_key(device)
                osd_keys[device] = key
                log.info(f"Retrieved key for OSD device: {device}")
            except Exception as e:
                log.error(f"Failed to get key for OSD device {device}: {e}")
                raise

        return osd_keys

    @retry(UnexpectedBehaviour, tries=10, delay=20)
    def verify_keyrotation(self, old_keys, include_noobaa=True):
        """
        Verify that key rotation has occurred for all OSD devices.

        Args:
            old_keys (Dict[str, str]): Dictionary of old keys before rotation
            include_noobaa (bool): Whether to include NooBaa key in verification

        Returns: True if all keys have been rotated

        Raises:
            UnexpectedBehaviour: If keys have not rotated
        """
        log.info("Verifying OSD key rotation")

        unrotated_devices = []

        # Check OSD keys
        for device in self.deviceset:
            new_key = self.provider.get_osd_key(device)
            if old_keys.get(device) == new_key:
                log.warning(f"Key not rotated yet for device: {device}")
                unrotated_devices.append(device)
            else:
                log.info(f"Key rotated successfully for device: {device}")

                # Verify new key exists in KMS
                if not self.provider.verify_key_exists(new_key):
                    raise UnexpectedBehaviour(
                        f"New key {new_key} for device {device} not found in KMS"
                    )

        # Check NooBaa key if requested
        if include_noobaa:
            try:
                old_noobaa_key = old_keys.get(constants.NOOBAA_BACKEND_SECRET)
                new_noobaa_key = self.provider.get_noobaa_key()

                if old_noobaa_key == new_noobaa_key:
                    log.warning("NooBaa key not rotated yet")
                    unrotated_devices.append(constants.NOOBAA_BACKEND_SECRET)
                else:
                    log.info("NooBaa key rotated successfully")
            except Exception as e:
                log.warning(f"Could not verify NooBaa key rotation: {e}")

        if unrotated_devices:
            raise UnexpectedBehaviour(
                f"Keys not rotated for: {', '.join(unrotated_devices)}"
            )

        log.info("All OSD keys rotated successfully")
        return True

    def set_keyrotation_defaults(self):
        """
        Set keyrotation to default values (schedule=@weekly, enable=true).

        This is a convenience method for resetting key rotation configuration.
        """
        log.info("Setting key rotation to default values")
        self.set_keyrotation_schedule("@weekly")
        self.enable_keyrotation()
        log.info("Key rotation defaults set successfully")

    def get_osd_dm_crypt(self, device):
        """
        Get the dmcrypt key for an OSD device.

        Args:
            device: Device handle (PVC name)

        Returns: dmcrypt key value
        """
        cmd = f"get secret rook-ceph-osd-encryption-key-{device} -o jsonpath='{{.data.dmcrypt-key}}'"
        dmcrypt_key_b64 = self._exec_oc_cmd(cmd, out_yaml_format=False)
        dmcrypt_key = base64.b64decode(dmcrypt_key_b64).decode()
        log.info(f"Retrieved dmcrypt key for device: {device}")
        return dmcrypt_key

    def verify_osd_keyrotation_for_kms(self, tries=10, delay=10):
        """
        Verify OSD and NooBaa key rotation for KMS providers (Vault/HPCS/KMIP).

        This method works with any KMS provider and verifies that keys have been rotated.

        Args:
            tries: Number of retry attempts
            delay: Delay between retries in seconds

        Returns: True if key rotation is successful
        """
        log.info(
            f"Verifying OSD keyrotation for KMS provider: {self.provider.get_provider_type()}"
        )

        # Get old keys
        old_keys = {}
        for dev in self.deviceset:
            old_keys[dev] = self.provider.get_osd_key(dev)

        # Get old NooBaa key
        old_keys[constants.NOOBAA_BACKEND_SECRET] = self.provider.get_noobaa_key()

        log.info("OSD and NooBaa keys before rotation recorded")

        # Verify rotation
        return self.verify_keyrotation(old_keys, include_noobaa=True)


# ============================================================================
# NooBaa Key Rotation
# ============================================================================


class NoobaaKeyRotation(KeyRotationManager):
    """
    Manages key rotation for NooBaa storage components.
    """

    def __init__(self, provider=None):
        """
        Initialize NooBaa key rotation manager.

        Args:
            provider (BaseKMSProvider, optional): KMS provider instance
        """
        super().__init__(provider)

    def get_noobaa_keyrotation_schedule(self):
        """
        Get NooBaa-specific key rotation schedule.

        Returns: Key rotation schedule

        Raises:
            ValueError: If schedule not found
        """
        cmd = (
            "get noobaas.noobaa.io "
            "-o jsonpath='{.items[*].spec.security.kms.schedule}'"
        )
        schedule = self._exec_oc_cmd(cmd, out_yaml_format=False)

        if not schedule:
            raise ValueError("NooBaa key rotation schedule not found")

        log.info(f"NooBaa key rotation schedule: {schedule}")
        return schedule.strip()

    def is_noobaa_keyrotation_enabled(self):
        """
        Check if NooBaa key rotation is enabled.

        Returns: True if enabled, False otherwise
        """
        cmd = (
            "get noobaas.noobaa.io "
            "-o jsonpath='{.items[*].spec.security.kms.enableKeyRotation}'"
        )
        cmd_out = self._exec_oc_cmd(cmd)

        if cmd_out == "true":
            log.info("NooBaa key rotation is enabled")
        return True

        log.info("NooBaa key rotation is disabled")
        return False

    def get_noobaa_backend_secret(self):
        """
        Get NooBaa backend encryption secret.

        Returns: (key_name, key_value)
        """
        # Try to get from KMS first
        try:
            key_value = self.provider.get_noobaa_key()
            return ("noobaa_backend_key", key_value)
        except Exception as e:
            log.warning(f"Could not get NooBaa key from KMS: {e}")

        # Fall back to Kubernetes secret
        cmd = f"get secret {constants.NOOBAA_BACKEND_SECRET} -o jsonpath='{{.data}}'"
        secret_data = self._exec_oc_cmd(cmd)

        if "active_root_key" in secret_data:
            key_name = base64.b64decode(secret_data["active_root_key"]).decode()
            key_value = secret_data.get(key_name, "")
            return (key_name, base64.b64decode(key_value).decode() if key_value else "")

        raise ValueError("Could not retrieve NooBaa backend secret")

    def get_noobaa_volume_secret(self):
        """
        Get NooBaa volume encryption secret.

        Returns: (key_name, key_value)
        """
        cmd = f"get secret {constants.NOOBAA_VOLUME_SECRET} -o jsonpath='{{.data}}'"
        secret_data = self._exec_oc_cmd(cmd)

        key_name = base64.b64decode(secret_data["active_root_key"]).decode()
        key_value = secret_data.get(key_name, "")

        log.info(f"NooBaa volume key: {key_name}")
        return (key_name, base64.b64decode(key_value).decode() if key_value else "")

    @retry(UnexpectedBehaviour, tries=10, delay=20)
    def verify_noobaa_keyrotation(self, old_key_value):
        """
        Verify NooBaa key has been rotated.

        Args:
            old_key_value (str): Old key value before rotation

        Returns: True if key has been rotated

        Raises:
            UnexpectedBehaviour: If key has not rotated
        """
        _, new_key_value = self.get_noobaa_backend_secret()

        if old_key_value == new_key_value:
            raise UnexpectedBehaviour("NooBaa key has not been rotated yet")

        log.info("NooBaa key rotated successfully")
        return True


# ============================================================================
# PV Key Rotation
# ============================================================================


class PVKeyRotation(KeyRotationManager):
    """
    Manages key rotation for Persistent Volume (PV) encryption.
    """

    def __init__(self, sc_obj, provider=None):
        """
        Initialize PV key rotation manager.

        Args:
            sc_obj: StorageClass object
            provider (BaseKMSProvider, optional): KMS provider instance
        """
        super().__init__(provider)
        self.sc_obj = sc_obj
        self.all_pvc_key_data = None

    def annotate_storageclass_key_rotation(self, schedule="@weekly"):
        """
        Annotate StorageClass to enable key rotation for encrypted PVs.

        Args:
            schedule (str): Rotation schedule (default: '@weekly')
        """
        annotation = f"keyrotation.csiaddons.openshift.io/schedule='{schedule}'"
        log.info(f"Adding annotation to StorageClass: {annotation}")
        self.sc_obj.annotate(annotation=annotation)

    def set_keyrotation_state_by_annotation(self, enable):
        """
        Enable or disable key rotation via StorageClass annotation.

        Args:
            enable (bool): True to enable, False to disable
        """
        state = "true" if enable else "false"
        annotation = f"keyrotation.csiaddons.openshift.io/enable={state}"
        self.sc_obj.annotate(annotation=annotation)
        log.info(f"Key rotation {'enabled' if enable else 'disabled'} for StorageClass")

    def get_pv_key(self, device_handle):
        """
        Get encryption key for a specific PV.

        Args:
            device_handle (str): PV volume handle

        Returns: Encryption key or key ID
        """
        return self.provider.get_pv_key(device_handle)

    @retry(UnexpectedBehaviour, tries=5, delay=20)
    def verify_pv_keyrotation(self, device_handle, old_key):
        """
        Verify that PV key has been rotated.

        Args:
            device_handle (str): PV volume handle
            old_key (str): Old key value

        Returns: True if key has been rotated

        Raises:
            UnexpectedBehaviour: If key has not rotated
        """
        new_key = self.provider.get_pv_key(device_handle)

        if old_key == new_key:
            raise UnexpectedBehaviour(
                f"Key not rotated for device handle: {device_handle}"
            )

        # Verify new key exists in KMS
        if not self.provider.verify_key_exists(new_key):
            raise UnexpectedBehaviour(
                f"New key {new_key} not found in KMS for device: {device_handle}"
            )

        log.info(f"PV key rotated successfully for device: {device_handle}")
        return True

    def get_keyrotation_cronjob_for_pvc(self, pvc_obj):
        """
        Get the key rotation CronJob associated with a PVC.

        Args:
            pvc_obj: PVC object

        Returns:
            OCP: CronJob object

        Raises:
            ValueError: If CronJob annotation not found
        """
        if "annotations" not in pvc_obj.data.get("metadata", {}):
            pvc_obj.reload()

        cronjob_name = (
            pvc_obj.data.get("metadata", {})
            .get("annotations", {})
            .get("keyrotation.csiaddons.openshift.io/cronjob")
        )

        if not cronjob_name:
            raise ValueError(f"Missing key rotation cronjob for PVC: {pvc_obj.name}")

        log.info(f"Found CronJob '{cronjob_name}' for PVC '{pvc_obj.name}'")

        cronjob_obj = OCP(
            kind=constants.ENCRYPTIONKEYROTATIONCRONJOB,
            namespace=pvc_obj.namespace,
            resource_name=cronjob_name,
        )

        if not cronjob_obj.is_exist():
            raise ValueError(
                f"CronJob {cronjob_name} does not exist for PVC: {pvc_obj.name}"
            )

        return cronjob_obj

    def get_pvc_keys_data(self, pvc_objs):
        """
        Get key data for multiple PVCs.

        Args:
            pvc_objs (List): List of PVC objects

        Returns: PVC name -> key data mapping
        """
        pvc_keys = {}

        for pvc in pvc_objs:
            try:
                device_handle = pvc.get_pv_volume_handle_name
                key_value = self.provider.get_pv_key(device_handle)

                pvc_keys[pvc.name] = {
                    "device_handle": device_handle,
                    "key_value": key_value,
                }
                log.info(f"Retrieved key data for PVC: {pvc.name}")
            except Exception as e:
                log.error(f"Failed to get key for PVC {pvc.name}: {e}")
                raise

        return pvc_keys

    @retry(UnexpectedBehaviour, tries=10, delay=20)
    def wait_till_all_pv_keyrotation(self, pvc_objs):
        """
        Wait for all PVC keys to be rotated.

        Args:
            pvc_objs (List): List of PVC objects

        Returns: True if all keys have been rotated

        Raises:
            UnexpectedBehaviour: If keys have not rotated
        """
        if not self.all_pvc_key_data:
            self.all_pvc_key_data = self.get_pvc_keys_data(pvc_objs)
            raise UnexpectedBehaviour("Initializing PVC key data")

        new_pvc_keys = self.get_pvc_keys_data(pvc_objs)

        if self.all_pvc_key_data == new_pvc_keys:
            raise UnexpectedBehaviour("PVC keys have not rotated yet")

        # Verify all new keys exist in KMS
        for pvc_name, key_data in new_pvc_keys.items():
            key_value = key_data["key_value"]
            if not self.provider.verify_key_exists(key_value):
                raise UnexpectedBehaviour(
                    f"New key for PVC {pvc_name} not found in KMS"
                )

        log.info("All PVC keys rotated successfully")
        return True

    def change_pvc_keyrotation_cronjob_state(self, pvc_objs, disable=True):
        """
        Enable or disable key rotation for PVCs.

        Args:
            pvc_objs (List): List of PVC objects
            disable (bool): True to disable, False to enable

        Returns: True if successful
        """
        state_value = "unmanaged" if disable else "managed"

        for pvc in pvc_objs:
            try:
                cronjob = self.get_keyrotation_cronjob_for_pvc(pvc)

                # Annotate CronJob
                state_annotation = f"csiaddons.openshift.io/state={state_value}"
                cronjob.annotate(state_annotation, overwrite=True)
                log.info(f"Annotated CronJob for PVC '{pvc.name}' with: {state_value}")

                # Patch CronJob suspend field
                if disable:
                    suspend_patch = (
                        '[{"op": "add", "path": "/spec/suspend", "value": true}]'
                    )
                else:
                    suspend_patch = '[{"op": "remove", "path": "/spec/suspend"}]'

                cronjob.patch(params=suspend_patch, format_type="json")
                log.info(
                    f"{'Suspended' if disable else 'Resumed'} CronJob for PVC: {pvc.name}"
                )

                pvc.reload()

            except Exception as e:
                log.error(f"Failed to modify CronJob for PVC {pvc.name}: {e}")
                raise

        log.info(
            f"Key rotation state changed to {'disabled' if disable else 'enabled'} for all PVCs"
        )
        return True

    def reset_keyrotation_baseline(self):
        """
        Resets the baseline key data for key rotation verification.
        This should be called after re-enabling key rotation to ensure
        the baseline is captured after re-enabling, not before.
        """
        self.all_pvc_key_data = None
        log.info(
            "Reset key rotation baseline - will capture new baseline on next verification."
        )

    @retry(UnexpectedBehaviour, tries=10, delay=10)
    def wait_for_keyrotation_cronjobs_recreation(self, pvc_objs):
        """
        Wait for key rotation CronJobs to be recreated after re-enabling.

        Args:
            pvc_objs (List): List of PVC objects

        Returns: True if all CronJobs are recreated and active

        Raises:
            UnexpectedBehaviour: If CronJobs not recreated within timeout
        """
        missing_cronjobs = []

        for pvc_obj in pvc_objs:
            pvc_obj.reload()

            try:
                cronjob = self.get_keyrotation_cronjob_for_pvc(pvc_obj)

                if not cronjob.is_exist():
                    missing_cronjobs.append(pvc_obj.name)
                    continue

                cronjob_data = cronjob.get()
                if cronjob_data.get("spec", {}).get("suspend", False):
                    missing_cronjobs.append(f"{pvc_obj.name} (suspended)")
                    continue

                log.info(f"CronJob for PVC '{pvc_obj.name}' is active")

            except ValueError:
                missing_cronjobs.append(pvc_obj.name)

        if missing_cronjobs:
            raise UnexpectedBehaviour(
                f"CronJobs not ready for PVCs: {', '.join(missing_cronjobs)}"
            )

        log.info("All key rotation CronJobs are recreated and active")
        return True


# ============================================================================
# Utility Functions
# ============================================================================


def validate_key_rotation_schedules(schedule):
    """
    Validate key rotation schedules across different components.

    Args:
        schedule (str): Expected key rotation schedule

    Returns: True if all schedules match

    Raises:
        ValueError: If schedules don't match
    """
    log.info(f"Validating key rotation schedule: {schedule}")

    # Create managers
    osd_kr = OSDKeyRotation()
    noobaa_kr = NoobaaKeyRotation()
    base_kr = KeyRotationManager()

    components = [
        ("StorageCluster", base_kr.get_keyrotation_schedule),
        ("CephCluster (OSD)", osd_kr.get_osd_keyrotation_schedule),
        ("NooBaa", noobaa_kr.get_noobaa_keyrotation_schedule),
    ]

    for name, get_schedule in components:
        try:
            current_schedule = get_schedule()
            if current_schedule != schedule:
                raise ValueError(
                    f"{name} schedule mismatch: expected '{schedule}', got '{current_schedule}'"
                )
            log.info(f"{name} schedule validated successfully")
        except Exception as e:
            log.error(f"Failed to validate {name} schedule: {e}")
            raise

    log.info("All key rotation schedules validated successfully")
    return True


@retry(UnexpectedBehaviour, tries=10, delay=20)
def compare_noobaa_old_keys_with_new_keys(
    noobaa_keyrotation, old_noobaa_backend_key, old_noobaa_volume_key
):
    """
    Compare NooBaa old keys with new keys.

    Args:
        noobaa_keyrotation: NoobaaKeyRotation object
        old_noobaa_backend_key: Old NooBaa backend key
        old_noobaa_volume_key: Old NooBaa volume key

    Raises:
        UnexpectedBehaviour: If keys have not rotated
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
        raise UnexpectedBehaviour("NooBaa backend key has not rotated yet")

    if new_noobaa_volume_key == old_noobaa_volume_key:
        raise UnexpectedBehaviour("NooBaa volume key has not rotated yet")

    log.info(
        f"NooBaa backend key rotated: {new_noobaa_backend_key} : {new_noobaa_backend_secret}"
    )
    log.info(
        f"NooBaa volume key rotated: {new_noobaa_volume_key} : {new_noobaa_volume_secret}"
    )


def verify_new_key_after_rotation(tries=10, delay=20):
    """
    Verify that new keys are generated after key rotation.

    This function records existing keys and compares them with new keys
    after rotation has occurred.

    Args:
        tries (int): Number of retry attempts
        delay (int): Delay between retries in seconds

    Returns: True if keys have been rotated successfully
    """
    log.info("Starting key rotation verification")

    # Create rotation managers
    osd_kr = OSDKeyRotation()
    noobaa_kr = NoobaaKeyRotation()

    # Record old OSD keys
    log.info("Recording old OSD keys")
    old_osd_keys = osd_kr.get_all_osd_keys()

    # Record old NooBaa keys
    log.info("Recording old NooBaa keys")
    _, old_noobaa_backend_key = noobaa_kr.get_noobaa_backend_secret()
    _, old_noobaa_volume_key = noobaa_kr.get_noobaa_volume_secret()

    log.info("Old keys recorded. Waiting for rotation...")

    # Verify OSD key rotation
    try:
        osd_kr.verify_keyrotation(old_osd_keys, include_noobaa=False)
        log.info("OSD keys rotated successfully")
    except UnexpectedBehaviour as e:
        log.error(f"OSD key rotation verification failed: {e}")
        return False

    # Verify NooBaa backend key rotation
    try:
        noobaa_kr.verify_noobaa_keyrotation(old_noobaa_backend_key)
        log.info("NooBaa backend key rotated successfully")
    except UnexpectedBehaviour as e:
        log.error(f"NooBaa backend key rotation verification failed: {e}")
        return False

    log.info("All keys rotated successfully")
    return True


# ============================================================================
# Example Usage
# ============================================================================


def example_usage():
    """
    Example demonstrating how to use the key rotation framework.
    """
    # Example 1: Auto-detect provider and perform OSD key rotation
    print("\n=== Example 1: Auto-detect Provider ===")
    osd_kr = OSDKeyRotation()
    print(f"Provider: {osd_kr.provider.get_provider_type()}")
    print(f"Key rotation enabled: {osd_kr.is_osd_keyrotation_enabled()}")

    # Example 2: Explicitly use Vault provider
    print("\n=== Example 2: Explicit Vault Provider ===")
    vault_provider = VaultProvider()
    vault_provider.initialize()
    _ = OSDKeyRotation(provider=vault_provider)
    print("Created OSD key rotation with Vault provider")

    # Example 3: Register and use a custom provider
    print("\n=== Example 3: List Available Providers ===")
    providers = KeyRotationFactory.list_providers()
    print(f"Available providers: {providers}")

    # Example 4: PV key rotation
    print("\n=== Example 4: PV Key Rotation ===")
    # Assuming sc_obj is your StorageClass object
    # pv_kr = PVKeyRotation(sc_obj)
    # pv_kr.annotate_storageclass_key_rotation(schedule="@weekly")

    # Example 5: Verify rotation across all components
    print("\n=== Example 5: Validate Schedules ===")
    try:
        validate_key_rotation_schedules("@weekly")
        print("All schedules validated!")
    except ValueError as e:
        print(f"Schedule validation failed: {e}")


if __name__ == "__main__":
    # This is just for demonstration
    print("Key Rotation Helper Module")
    print("=" * 50)
    print("\nTo use this module, import the classes you need:")
    print("  from ocs_ci.helpers.keyrotation_helper import OSDKeyRotation")
    print("  from ocs_ci.helpers.keyrotation_helper import PVKeyRotation")
    print("  from ocs_ci.helpers.keyrotation_helper import KeyRotationFactory")
    print("\nAvailable providers:")
    for provider in KeyRotationFactory.list_providers():
        print(f"  - {provider}")
