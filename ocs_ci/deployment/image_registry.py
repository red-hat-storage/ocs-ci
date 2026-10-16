"""
This module contains functionality required for configuring OpenShift internal image registry
with PVC storage backend.
"""

import logging
import tempfile
import json

from ocs_ci.framework import config
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, exec_cmd
from ocs_ci.ocs import exceptions
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


class ImageRegistryConfigurator(object):
    """
    Image Registry Configurator class for configuring internal image registry with PVC
    """

    def __init__(self):
        self.namespace = constants.OPENSHIFT_IMAGE_REGISTRY_NAMESPACE
        self.registry_config_name = "cluster"
        self.pvc_name = "image-registry-storage"

    def check_registry_storage_type(self):
        """
        Check the current storage type of the image registry

        Returns:
            dict: Storage configuration of the image registry
        """
        logger.info("Checking current image registry storage configuration")
        ocp = OCP(
            kind="configs.imageregistry.operator.openshift.io",
            resource_name=self.registry_config_name,
        )
        try:
            storage_config = ocp.exec_oc_cmd(
                f"get configs.imageregistry.operator.openshift.io {self.registry_config_name} "
                "-o jsonpath='{.spec.storage}'"
            )
            logger.info(f"Current storage configuration: {storage_config}")
            return storage_config
        except CommandFailed as e:
            logger.error(f"Failed to get image registry storage configuration: {e}")
            raise

    def is_registry_using_emptydir(self):
        """
        Check if the image registry is using emptyDir storage

        Returns:
            bool: True if using emptyDir, False otherwise
        """
        storage_config = self.check_registry_storage_type()
        if "emptyDir" in str(storage_config):
            logger.info("Image registry is currently using emptyDir storage")
            return True
        logger.info("Image registry is not using emptyDir storage")
        return False

    def is_registry_using_pvc(self):
        """
        Check if the image registry is already using PVC storage

        Returns:
            bool: True if using PVC, False otherwise
        """
        storage_config = self.check_registry_storage_type()
        if "pvc" in str(storage_config).lower():
            logger.info("Image registry is already using PVC storage")
            return True
        logger.info("Image registry is not using PVC storage")
        return False

    def check_pvc_exists(self):
        """
        Check if the image registry PVC already exists

        Returns:
            bool: True if PVC exists, False otherwise
        """
        logger.info(
            f"Checking if PVC {self.pvc_name} exists in namespace {self.namespace}"
        )
        ocp = OCP(kind="pvc", namespace=self.namespace)
        try:
            pvc = ocp.get(resource_name=self.pvc_name)
            if pvc:
                logger.info(f"PVC {self.pvc_name} already exists")
                return True
        except (CommandFailed, exceptions.CommandFailed):
            logger.info(f"PVC {self.pvc_name} does not exist")
            return False

    def create_image_registry_pvc(self, storage_class=None, size=None):
        """
        Create PVC for image registry storage

        Args:
            storage_class (str): Storage class name to use for PVC
            size (str): Size of the PVC (e.g., "100Gi")

        Raises:
            CommandFailed: If PVC creation fails
        """
        if self.check_pvc_exists():
            logger.info(f"PVC {self.pvc_name} already exists, skipping creation")
            return

        # Get storage class and size from config or use defaults
        storage_class = storage_class or config.DEPLOYMENT.get(
            "image_registry_pvc_storageclass", "thin-csi"
        )
        size = size or config.DEPLOYMENT.get("image_registry_pvc_size", "100Gi")

        logger.info(
            f"Creating PVC {self.pvc_name} with storage class {storage_class} and size {size}"
        )

        pvc_data = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": self.pvc_name, "namespace": self.namespace},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "resources": {"requests": {"storage": size}},
                "storageClassName": storage_class,
            },
        }

        pvc_manifest = tempfile.NamedTemporaryFile(
            mode="w+", prefix="image_registry_pvc", delete=False
        )
        templating.dump_data_to_temp_yaml(pvc_data, pvc_manifest.name)

        try:
            run_cmd(f"oc apply -f {pvc_manifest.name}")
            logger.info(f"PVC {self.pvc_name} created successfully")
        except CommandFailed as e:
            logger.error(f"Failed to create PVC: {e}")
            raise

        # Note: We don't wait for PVC to be bound here because storage class uses
        # WaitForFirstConsumer binding mode. PVC will bind when registry pod tries to use it.
        logger.info(
            f"PVC {self.pvc_name} created. It will bind when the image registry pod consumes it."
        )

    def wait_for_pvc_bound(self, timeout=300):
        """
        Wait for the PVC to be in Bound state

        Args:
            timeout (int): Timeout in seconds

        Raises:
            TimeoutExpiredError: If PVC does not reach Bound state within timeout
        """
        logger.info(f"Waiting for PVC {self.pvc_name} to be Bound")
        ocp = OCP(kind="pvc", namespace=self.namespace, resource_name=self.pvc_name)

        ocp.wait_for_resource(
            condition="Bound",
            resource_name=self.pvc_name,
            timeout=timeout,
            sleep=10,
        )
        logger.info(f"PVC {self.pvc_name} is now Bound")

    def patch_image_registry_to_use_pvc(self):
        """
        Patch the image registry configuration to use PVC storage

        Raises:
            CommandFailed: If patching fails
        """
        logger.info("Patching image registry configuration to use PVC storage")

        patch = {
            "spec": {
                "managementState": "Managed",
                "storage": {
                    "emptyDir": None,
                    "pvc": {"claim": self.pvc_name},
                },
                "replicas": 1,
                "rolloutStrategy": "Recreate",
            }
        }
        patch_cmd = (
            f"oc patch configs.imageregistry.operator.openshift.io {self.registry_config_name} "
            f"--type=merge --patch='{json.dumps(patch)}'"
        )

        try:
            result = exec_cmd(patch_cmd, shell=True)
            if result.returncode == 0:
                logger.info("Image registry configuration patched successfully")
                logger.info(result.stdout.decode("utf-8") if result.stdout else "")
            else:
                error_msg = (
                    result.stderr.decode("utf-8") if result.stderr else "Unknown error"
                )
                raise CommandFailed(f"Failed to patch image registry: {error_msg}")
        except Exception as e:
            logger.error(f"Failed to patch image registry configuration: {e}")
            raise

    def verify_registry_using_pvc(self, timeout=300):
        """
        Verify that the image registry is using PVC storage

        Args:
            timeout (int): Timeout in seconds

        Returns:
            bool: True if registry is using PVC, False otherwise

        Raises:
            TimeoutExpiredError: If verification fails within timeout
        """
        logger.info("Verifying that image registry is using PVC storage")

        for sample in TimeoutSampler(timeout, 10, self.check_registry_storage_type):
            storage_config = sample
            if "pvc" in str(storage_config).lower() and "emptyDir" not in str(
                storage_config
            ):
                logger.info("Image registry is successfully using PVC storage")
                return True
            logger.debug(
                f"Current storage config: {storage_config}, waiting for PVC configuration"
            )

        raise exceptions.TimeoutExpiredError(
            f"Image registry did not switch to PVC storage within {timeout} seconds"
        )

    def wait_for_registry_pods_ready(self, timeout=600):
        """
        Wait for image registry pods to be ready after configuration change

        Args:
            timeout (int): Timeout in seconds

        Raises:
            TimeoutExpiredError: If pods are not ready within timeout
        """
        logger.info("Waiting for image registry pods to be ready")
        ocp = OCP(kind="pod", namespace=self.namespace)

        ocp.wait_for_resource(
            condition="Running",
            selector=constants.OPENSHIFT_IMAGE_SELECTOR,
            timeout=timeout,
            sleep=15,
        )
        logger.info("Image registry pods are ready")

    def configure_image_registry_with_pvc(
        self, storage_class=None, size=None, force=False
    ):
        """
        Configure the internal image registry to use PVC storage

        Args:
            storage_class (str): Storage class name to use for PVC
            size (str): Size of the PVC (e.g., "100Gi")
            force (bool): Force reconfiguration even if already using PVC

        Returns:
            bool: True if configuration was successful, False otherwise
        """
        logger.info("Starting image registry PVC configuration")

        # Check if already using PVC
        if self.is_registry_using_pvc() and not force:
            logger.info(
                "Image registry is already using PVC storage, skipping configuration"
            )
            return True

        # Check if using emptyDir
        if self.is_registry_using_emptydir():
            logger.info("Image registry is using emptyDir, will configure PVC storage")
        else:
            logger.info("Image registry storage type is not emptyDir")

        try:
            # Step 1: Create PVC (it will remain in Pending state with WaitForFirstConsumer)
            self.create_image_registry_pvc(storage_class=storage_class, size=size)

            # Step 2: Patch image registry configuration to use PVC
            # This will trigger the registry pod to consume the PVC, causing it to bind
            self.patch_image_registry_to_use_pvc()

            # Step 3: Wait for registry pods to be ready (this also allows PVC to bind)
            self.wait_for_registry_pods_ready()

            # Step 4: Wait for PVC to be bound (should be quick now that pod is using it)
            logger.info("Waiting for PVC to be bound after registry pod consumes it")
            self.wait_for_pvc_bound(timeout=300)

            # Step 5: Verify configuration
            self.verify_registry_using_pvc()

            logger.info("Image registry PVC configuration completed successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to configure image registry with PVC: {e}")
            raise

    def post_configuration_verification(self):
        """
        Perform post-configuration verification

        Returns:
            bool: True if verification passes, False otherwise
        """
        logger.info("Performing post-configuration verification")

        try:
            # Verify PVC exists and is bound
            if not self.check_pvc_exists():
                logger.error("PVC does not exist")
                return False

            ocp_pvc = OCP(
                kind="pvc", namespace=self.namespace, resource_name=self.pvc_name
            )
            pvc_data = ocp_pvc.get()
            if pvc_data and isinstance(pvc_data, dict):
                pvc_status = pvc_data.get("status", {}).get("phase")
                if pvc_status != "Bound":
                    logger.error(f"PVC is not Bound, current status: {pvc_status}")
                    return False
            else:
                logger.error("Failed to get PVC data")
                return False

            # Verify registry is using PVC
            if not self.is_registry_using_pvc():
                logger.error("Image registry is not using PVC storage")
                return False

            # Verify registry is not using emptyDir
            if self.is_registry_using_emptydir():
                logger.error("Image registry is still using emptyDir storage")
                return False

            logger.info("Post-configuration verification passed")
            return True

        except Exception as e:
            logger.error(f"Post-configuration verification failed: {e}")
            return False


def configure_image_registry_with_pvc():
    """
    Configure image registry with PVC based on configuration

    """
    if config.DEPLOYMENT.get("image_registry_pvc_deployment"):
        logger.info("Image registry PVC deployment is enabled in configuration")
        registry_configurator = ImageRegistryConfigurator()

        storage_class = config.DEPLOYMENT.get("image_registry_pvc_storageclass")
        size = config.DEPLOYMENT.get("image_registry_pvc_size")

        registry_configurator.configure_image_registry_with_pvc(
            storage_class=storage_class, size=size
        )

        # Perform post-configuration verification
        if registry_configurator.post_configuration_verification():
            logger.info(
                "Image registry PVC configuration and verification completed successfully"
            )
        else:
            logger.warning(
                "Image registry PVC configuration completed but verification failed"
            )
    else:
        logger.info("Image registry PVC deployment is not enabled in configuration")


# Made with Bob
