"""
This module contains functions needed to install IBM Fusion Data Foundation.
"""

import logging

from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.ocs import OCP, OCS
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)


def deploy_fdf():
    """
    Installs IBM Fusion Data Foundation.
    """
    logger.info("Installing IBM Fusion Data Foundation")
    create_image_tag_mirror_set()
    create_image_digest_mirror_set()
    create_spectrum_fusion_cr()
    create_fdf_service_cr()
    verify_fdf_installation()


def create_image_tag_mirror_set():
    """
    Create ImageTagMirrorSet.
    """
    logger.info("Creating FDF ImageTagMirrorSet")
    run_cmd(f"oc create -f {constants.FDF_IMAGE_TAG_MIRROR_SET}")


def create_image_digest_mirror_set():
    """
    Create ImageDigestMirrorSet.
    """
    logger.info("Creating FDF ImageDigestMirrorSet")
    run_cmd(f"oc create -f {constants.FDF_IMAGE_DIGEST_MIRROR_SET}")


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


@retry((AssertionError, KeyError), 20, 30)
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
