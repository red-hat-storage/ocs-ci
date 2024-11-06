from datetime import datetime
import logging

from ocs_ci.ocs.constants import SECRET, NOOBAA_S3_SERVING_CERT
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    polarion_id,
    bugzilla,
    skipif_ocs_version,
    red_squad,
    mcg,
    config,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import get_noobaa_endpoint_pods
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


@retry(UnexpectedBehaviour, tries=10, delay=3, backoff=1)
def get_validity_time_of_certificate(noobaa_endpoint_pods, cmd, old_validity=None):
    """
    Number of attempts to retry to get the validity of certificate

    """

    new_cmd_output = noobaa_endpoint_pods[0].exec_sh_cmd_on_pod(command=cmd)
    # Get the validity time of certificate
    cmd_output = new_cmd_output.split(",")
    cmd_output = cmd_output[0].split("\n")
    new_validity = cmd_output[7].split(",")[0] + cmd_output[8].split(",")[0]
    if new_validity == old_validity:
        logger.warn(
            f"New certificate not created post the deletion of secret {NOOBAA_S3_SERVING_CERT}, retrying again"
        )
        raise UnexpectedBehaviour(
            f"New certificate not created post the deletion of secret {NOOBAA_S3_SERVING_CERT}"
        )
    logger.info("New certificate created")
    return new_validity, new_cmd_output


@mcg
@red_squad
@tier1
@skipif_ocs_version("<4.12")
@bugzilla("2237903")
@polarion_id("OCS-6191")
class TestNoobaaUseNewInternalCertAfterRotation:
    def test_noobaa_use_new_internal_cert_after_rotation(self):
        """

        Objective of this test is to verify that noobaa use the new internal certificate after rotation

        """

        # Run command to get the validity of certificate before deleting secret
        cmd = (
            "openssl s_client -connect localhost:6443 -showcerts 2>/dev/null </dev/null | sed -ne "
            "'/-BEGIN CERTIFICATE-/,/-END CERTIFICATE-/p' | openssl x509 -text -noout"
        )
        noobaa_endpoint_pods = get_noobaa_endpoint_pods()
        old_cmd_output = noobaa_endpoint_pods[0].exec_sh_cmd_on_pod(command=cmd)

        # Get the validity time of certificate
        cmd_output = old_cmd_output.split(",")
        cmd_output = cmd_output[0].split("\n")
        validity = cmd_output[7].split(",")[0] + cmd_output[8].split(",")[0]
        logger.info(f"The validity time of certificate: {validity}")

        # Get the time now
        time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        reg_format_date = datetime.utcnow().isoformat() + "Z"
        reg_format_date = datetime.strptime(reg_format_date, time_format)
        logger.info(f"The current time in utc-iso-format: {reg_format_date}")

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
        creation_timestamp_secret = nb_endpoint_secret.get("metadata").get(
            "creationTimestamp"
        )
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        creation_timestamp_secret = datetime.strptime(
            creation_timestamp_secret, time_format
        )
        time_diff = reg_format_date - creation_timestamp_secret
        assert (
            time_diff.total_seconds() < 7200
        ), f"Failed to recreate new secret: {nb_endpoint_secret}"
        logger.info(f"New secret {NOOBAA_S3_SERVING_CERT} created")

        # After deleting old secret, verify the new secret created new internal certificate
        # Examine the validity time of certificate
        new_validity, new_cmd_output = get_validity_time_of_certificate(
            noobaa_endpoint_pods, cmd, validity
        )

        # Compare the validity time of new certificate and old certificate
        assert validity != new_validity, (
            f"New certificate not created. Old certificate output: {old_cmd_output}\n"
            f"New certificate output: {new_cmd_output}\n"
        )
        logger.info("New certificate created successfully.")
