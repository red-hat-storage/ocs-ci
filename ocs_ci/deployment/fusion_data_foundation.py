"""
This module contains functions needed to install IBM Fusion Data Foundation.
"""

import json
import logging
import os

import yaml

from ocs_ci.ocs import constants, defaults
from ocs_ci.framework import config
from ocs_ci.ocs.resources.ocs import OCP
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)


class FusionDataFoundationDeployment:
    def __init__(self):
        self.pre_release = config.DEPLOYMENT.get("fdf_pre_release", False)
        self.kubeconfig = config.RUN["kubeconfig"]

    def deploy(self):
        """
        Installs IBM Fusion Data Foundation.
        """
        logger.info("Installing IBM Fusion Data Foundation")
        if self.pre_release:
            self.create_image_tag_mirror_set()
            self.create_image_digest_mirror_set()
            self.setup_fdf_pre_release_deployment()
        self.create_fdf_service_cr()
        self.verify_fdf_installation()

    def create_image_tag_mirror_set(self):
        """
        Create ImageTagMirrorSet.
        """
        logger.info("Creating FDF ImageTagMirrorSet")
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} create -f {constants.FDF_IMAGE_TAG_MIRROR_SET}"
        )

    def create_image_digest_mirror_set(self):
        """
        Create ImageDigestMirrorSet.
        """
        logger.info("Creating FDF ImageDigestMirrorSet")
        image_digest_mirror_set = extract_image_digest_mirror_set()
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} create -f {image_digest_mirror_set}"
        )
        os.remove(image_digest_mirror_set)

    def create_fdf_service_cr(self):
        """
        Create Fusion Data Foundation Service CR.
        """
        logger.info("Creating FDF service CR")
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} create -f {constants.FDF_SERVICE_CR}"
        )

    def setup_fdf_pre_release_deployment(self):
        """
        Perform steps to prepare for a Pre-release deployment of FDF.
        """
        fdf_image_tag = config.DEPLOYMENT.get("fdf_image_tag")
        fdf_catalog_name = defaults.FUSION_CATALOG_NAME
        fdf_registry = config.DEPLOYMENT.get("fdf_pre_release_registry")
        fdf_image_digest = config.DEPLOYMENT.get("fdf_pre_release_image_digest")
        pull_secret = os.path.join(constants.DATA_DIR, "pull-secret")

        if not fdf_image_digest:
            logger.info("Retrieving imageDigest")
            cmd = f"skopeo inspect docker://{fdf_registry}/{fdf_catalog_name}:{fdf_image_tag} --authfile {pull_secret}"
            catalog_data = run_cmd(cmd)
            fdf_image_digest = json.loads(catalog_data).get("Digest")
            logger.info(f"Retrieved image digest: {fdf_image_digest}")
            config.DEPLOYMENT["fdf_pre_release_image_digest"] = fdf_image_digest

        logger.info("Updating FusionServiceDefinition")
        params_dict = {
            "spec": {
                "onboarding": {
                    "serviceOperatorSubscription": {
                        "multiVersionCatSrcDetails": {
                            "ocp418-t": {
                                "imageDigest": fdf_image_digest,
                                "registryPath": fdf_registry,
                            }
                        }
                    }
                }
            }
        }
        params = json.dumps(params_dict)
        cmd = (
            f"oc --kubeconfig {self.kubeconfig} -n {constants.FDF_NAMESPACE} patch FusionServiceDefinition "
            f"data-foundation-service -p '{params}' --type merge"
        )
        out = run_cmd(cmd)
        assert "patched" in out

    def verify_fdf_installation(self):
        """
        Verify the FDF installation was successful.
        """
        logger.info("Verifying FDF installation")
        fusion_service_instance_health_check()
        self.get_installed_version()
        logger.info("FDF successfully installed")

    def get_installed_version(self):
        """
        Retrieve the installed FDF version.

        Returns:
            str: Installed FDF version.

        """
        logger.info("Retrieving installed FDF version")
        results = run_cmd(
            f"oc get FusionServiceInstance {constants.FDF_SERVICE_NAME} "
            f"-n {constants.FDF_NAMESPACE} --kubeconfig {self.kubeconfig} -o yaml"
        )
        version = yaml.safe_load(results)["status"]["currentVersion"]
        config.ENV_DATA["fdf_version"] = version
        logger.info(f"Installed FDF version: {version}")
        return version


@retry((AssertionError, KeyError), 20, 60, backoff=1)
def fusion_service_instance_health_check():
    """
    Ensure the FusionServiceInstance is in the Healthy state.

    Raises:
        AssertionError: If the FusionServiceInstance is not in a completed state.
        KeyError: If the health status isn't present in the FusionServiceInstance data.

    """
    instance = OCP(
        resource_name=constants.FDF_SERVICE_NAME,
        kind="FusionServiceInstance",
        namespace=constants.FDF_NAMESPACE,
    )
    instance_status = instance.data["status"]
    service_health = instance_status["health"]
    install_percent = instance_status["installStatus"]["progressPercentage"]
    assert service_health == "Healthy"
    assert install_percent == 100


def extract_image_digest_mirror_set():
    """
    Extract the ImageDigestMirrorSet from the FDF build.

    Returns:
        str: Name of the extracted ImageDigestMirrorSet

    """
    pull_secret = os.path.join(constants.DATA_DIR, "pull-secret")
    fdf_registry = config.DEPLOYMENT.get("fdf_pre_release_registry")
    fdf_catalog_name = defaults.FUSION_CATALOG_NAME
    fdf_image_tag = config.DEPLOYMENT.get("fdf_image_tag")

    filename = constants.FDF_IMAGE_DIGEST_MIRROR_SET_FILENAME
    cmd = (
        f"oc image extract --filter-by-os linux/amd64 --registry-config "
        f"{pull_secret} {fdf_registry}/{fdf_catalog_name}:{fdf_image_tag} --confirm --path /{filename}:./"
    )
    run_cmd(cmd)
    return filename
