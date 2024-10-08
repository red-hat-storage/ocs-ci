from datetime import datetime
import logging
import time


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


logger = logging.getLogger(__name__)


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

        # Get the time now
        time_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        reg_format_date = datetime.utcnow().isoformat() + "Z"
        reg_format_date = datetime.strptime(reg_format_date, time_format)

        # Delete the generated secret for the service
        secret_obj = OCP(kind=SECRET, namespace=config.ENV_DATA["cluster_namespace"])
        logger.info(
            f"Delete the generate secret {NOOBAA_S3_SERVING_CERT} for the service"
        )
        secret_obj.delete(resource_name=NOOBAA_S3_SERVING_CERT)

        # Verify the new secret is recreated
        nb_endpoint_secret = secret_obj.get(resource_name=NOOBAA_S3_SERVING_CERT)
        creation_timestamp_secret = nb_endpoint_secret.get("metadata").get(
            "creationTimestamp"
        )
        time_format = "%Y-%m-%dT%H:%M:%SZ"
        creation_timestamp_secret = datetime.strptime(
            creation_timestamp_secret, time_format
        )
        time_diff = reg_format_date - creation_timestamp_secret
        if time_diff.total_seconds() < 7200:
            logger.info(f"New secret {NOOBAA_S3_SERVING_CERT} created")
        else:
            raise UnexpectedBehaviour(
                f"Failed to recreate new secret: {nb_endpoint_secret}"
            )

        # After deleting old secret, verify the new secret created new internal certificate
        # Examine the validity time of certificate
        time.sleep(180)
        new_cmd_output = noobaa_endpoint_pods[0].exec_sh_cmd_on_pod(command=cmd)
        # Get the validity time of certificate
        cmd_output = new_cmd_output.split(",")
        cmd_output = cmd_output[0].split("\n")
        new_validity = cmd_output[7].split(",")[0] + cmd_output[8].split(",")[0]

        # Compare the validity time of new certificate and old certificate
        if validity == new_validity:
            logger.error(
                f"New certificate not created, old_certificate: {old_cmd_output}\n new_certificate: {new_cmd_output}\n"
            )
            raise UnexpectedBehaviour(
                f"New certificate not created post the deletion of secret {NOOBAA_S3_SERVING_CERT}"
            )
        else:
            logger.info("New certificate created")
