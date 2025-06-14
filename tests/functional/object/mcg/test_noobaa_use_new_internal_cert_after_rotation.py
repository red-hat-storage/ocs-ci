import logging

from ocs_ci.ocs.constants import SECRET, NOOBAA_S3_SERVING_CERT
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    polarion_id,
    skipif_ocs_version,
    red_squad,
    mcg,
    config,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_noobaa_endpoint_pods
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


def get_validity_time_of_certificate(noobaa_endpoint_pods):
    """

    Args:
        noobaa_endpoint_pods (List): List containing noobaa endpoint pod objects

    Returns:
        validity (str): Validity time of the certificate

    """
    cmd = (
        "openssl s_client -connect localhost:6443 -showcerts 2>/dev/null </dev/null | sed -ne "
        "'/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' | openssl x509 -text -noout"
    )
    new_cmd_output = noobaa_endpoint_pods[0].exec_sh_cmd_on_pod(command=cmd)
    logger.info(f"Certificate validity info: {new_cmd_output}")
    # Get the validity time of certificate
    cmd_output = new_cmd_output.split(",")
    cmd_output = cmd_output[0].split("\n")
    validity = cmd_output[7].split(",")[0] + cmd_output[8].split(",")[0]
    return validity


@retry(UnexpectedBehaviour, tries=20, delay=3, backoff=1)
def verify_cert_validity(noobaa_endpoint_pods, old_validity):
    """

    Args:
        noobaa_endpoint_pods (List): List containing noobaa endpoint pod objects
        old_validity (str): Validity time of the certificate before the secret deletion

    Returns:
        bool: True if new certificate is created

    """
    new_validity = get_validity_time_of_certificate(noobaa_endpoint_pods)
    if new_validity == old_validity:
        logger.warn(
            "New certificate is not created \n New validity: {new_validity}\n Old validity: {old_validity}"
        )
        raise UnexpectedBehaviour(
            f"New certificate not created post the deletion of secret {NOOBAA_S3_SERVING_CERT}"
        )
    return True


@mcg
@red_squad
@tier1
@skipif_ocs_version("<4.12")
@polarion_id("OCS-6191")
class TestNoobaaUseNewInternalCertAfterRotation:
    def test_noobaa_use_new_internal_cert_after_rotation(self):
        """

        Objective of this test is to verify that noobaa use the new internal certificate after rotation

        """

        # Get the validity of certificate before deleting secret
        noobaa_endpoint_pods = get_noobaa_endpoint_pods()
        old_validity = get_validity_time_of_certificate(noobaa_endpoint_pods)
        logger.info(f"The validity time of certificate: {old_validity}")

        # Delete the generated secret for the service
        logger.info(
            f"Delete the generated secret {NOOBAA_S3_SERVING_CERT} for the service"
        )
        secret_obj = OCP(kind=SECRET, namespace=config.ENV_DATA["cluster_namespace"])
        secret_obj.delete(resource_name=NOOBAA_S3_SERVING_CERT)

        # Verify the new secret is recreated
        logger.info("Verify new secret created post the deletion")
        nb_endpoint_secret = secret_obj.get(
            resource_name=NOOBAA_S3_SERVING_CERT, retry=10
        )
        logger.info(f"New secret {nb_endpoint_secret} created")

        # After deleting old secret, verify the new secret created new internal certificate
        # Examine the validity time of certificate
        noobaa_endpoint_pods = get_noobaa_endpoint_pods()
        assert verify_cert_validity(
            noobaa_endpoint_pods, old_validity
        ), "New certificate is not created post the deletion of the secret"
        logger.info("New certificate created post the deletion of the secret")
