"""
This module contains functions needed to install IBM Fusion Data Foundation.
"""

import json
import logging
import os

from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework import config
from ocs_ci.ocs.resources.ocs import OCP, OCS
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)


class FusionDataFoundationDeployment:
    def __init__(self):
        self.pre_release = config.DEPLOYMENT.get("fdf_pre_release", False)

    def deploy(self):
        """
        Installs IBM Fusion Data Foundation.
        """
        logger.info("Installing IBM Fusion Data Foundation")
        create_image_tag_mirror_set()
        self.create_image_digest_mirror_set()
        create_spectrum_fusion_cr()
        if self.pre_release:
            setup_fdf_pre_release_deployment()
        create_fdf_service_cr()
        verify_fdf_installation()

    def create_image_digest_mirror_set(self):
        """
        Create ImageDigestMirrorSet.
        """
        logger.info("Creating FDF ImageDigestMirrorSet")
        if self.pre_release:
            image_digest_mirror_set = extract_image_digest_mirror_set()
            run_cmd(f"oc create -f {image_digest_mirror_set}")
            os.remove(image_digest_mirror_set)
        else:
            run_cmd(f"oc create -f {constants.FDF_IMAGE_DIGEST_MIRROR_SET}")


def create_image_tag_mirror_set():
    """
    Create ImageTagMirrorSet.
    """
    logger.info("Creating FDF ImageTagMirrorSet")
    run_cmd(f"oc create -f {constants.FDF_IMAGE_TAG_MIRROR_SET}")


def create_spectrum_fusion_cr():
    """
    Create SpectrumFusion CR if it doesn't already exist.
    """
    if spectrum_fusion_existstance_check():
        spectrum_fusion_status_check()
        logger.info("SpectrumFusion already exists and is Completed")
    else:
        logger.info("Creating SpectrumFusion")
        run_cmd(f"oc create -f {constants.FDF_SPECTRUM_FUSION_CR}")
        spectrum_fusion_status_check()


def create_fdf_service_cr():
    """
    Create Fusion Data Foundation Service CR.
    """
    logger.info("Creating FDF service CR")
    run_cmd(f"oc create -f {constants.FDF_SERVICE_CR}")


def verify_fdf_installation():
    """
    Verify the FDF installation was successful.
    """
    logger.info("Verifying FDF installation")
    fusion_service_instance_health_check()
    logger.info("FDF successfully installed")


def spectrum_fusion_existstance_check():
    """
    Check for the existance of SpectrumFusion.

    Returns:
        bool: Existance of SpectrumFusion

    """
    spectrumfusion = OCS(
        kind="SpectrumFusion",
        metadata={
            "namespace": constants.FDF_NAMESPACE,
            "name": "spectrumfusion",
        },
    )
    try:
        spectrumfusion.reload()
    except CommandFailed as e:
        error_msg = '"spectrumfusion" not found'
        if error_msg in str(e):
            return False
    return True


@retry((AssertionError, KeyError), 10, 5)
def spectrum_fusion_status_check():
    """
    Ensure SpectrumFusion is in the Completed state.

    Raises:
        AssertionError: If SpectrumFusion is not in a completed state.
        KeyError: If the status isn't present in the SpectrumFusion data.

    """
    spectrumfusion = OCS(
        kind="SpectrumFusion",
        metadata={
            "namespace": constants.FDF_NAMESPACE,
            "name": "spectrumfusion",
        },
    )
    spectrumfusion.reload()
    spectrumfusion_status = spectrumfusion.data["status"]["status"]
    assert spectrumfusion_status == "Completed"


@retry((AssertionError, KeyError), 20, 60, backoff=1)
def fusion_service_instance_health_check():
    """
    Ensure the FusionServiceInstance is in the Healthy state.

    Raises:
        AssertionError: If the FusionServiceInstance is not in a completed state.
        KeyError: If the health status isn't present in the FusionServiceInstance data.

    """
    instance = OCP(
        resource_name="odfmanager",
        kind="FusionServiceInstance",
        namespace=constants.FDF_NAMESPACE,
    )
    instance_status = instance.data["status"]
    service_health = instance_status["health"]
    install_percent = instance_status["installStatus"]["progressPercentage"]
    assert service_health == "Healthy"
    assert install_percent == 100


def setup_fdf_pre_release_deployment():
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
        f"oc -n {constants.FDF_NAMESPACE} patch FusionServiceDefinition "
        f"data-foundation-service -p '{params}' --type merge"
    )
    out = run_cmd(cmd)
    assert "patched" in out


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
