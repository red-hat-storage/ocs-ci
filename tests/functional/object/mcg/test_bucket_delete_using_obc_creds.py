import base64
import boto3
import logging

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    red_squad,
    runs_on_provider,
    skipif_ocs_version,
    polarion_id,
    mcg,
)
from ocs_ci.ocs.resources.bucket_policy import HttpResponseParser
from ocs_ci.ocs.ocp import OCP
import botocore.exceptions as boto3exception
from ocs_ci.ocs.constants import (
    SECRET,
)
from ocs_ci.ocs.exceptions import UnexpectedBehaviour

logger = logging.getLogger(__name__)


@tier2
@mcg
@red_squad
@runs_on_provider
@skipif_ocs_version("<4.17")
@polarion_id("OCS-6252")
def test_bucket_delete_using_obc_creds(mcg_obj, bucket_factory):
    """
    Verify that deletion of an OBC's bucket is denied when using the
    OBC's credentials, but permitted with the noobaa admin credentials.

    """

    # create obc
    logger.info("Creating OBC")
    bucket = bucket_factory(amount=1, interface="OC")[0].name
    # Fetch OBC credentials
    secret_ocp_obj = OCP(kind=SECRET, namespace=config.ENV_DATA["cluster_namespace"])
    obc_secret_obj = secret_ocp_obj.get(bucket)
    obc_access_key = base64.b64decode(
        obc_secret_obj.get("data").get("AWS_ACCESS_KEY_ID")
    ).decode("utf-8")
    obc_secret_key = base64.b64decode(
        obc_secret_obj.get("data").get("AWS_SECRET_ACCESS_KEY")
    ).decode("utf-8")

    # Update OBC credentials to MCG object
    mcg_obj.s3_resource = boto3.resource(
        "s3",
        verify=False,
        endpoint_url=mcg_obj.s3_endpoint,
        aws_access_key_id=obc_access_key,
        aws_secret_access_key=obc_secret_key,
    )
    mcg_obj.s3_client = mcg_obj.s3_resource.meta.client

    # Perform bucket delete operation using OBC credentials
    logger.info("Deleting Bucket using OBC credentials")
    try:
        resp = mcg_obj.s3_client.delete_bucket(Bucket=bucket)
        assert not resp, "[Unexpected] Bucket deleted with OBC credentials"
    except boto3exception.ClientError as e:
        logger.info(e.response)
        resp = HttpResponseParser(e.response)
        if resp.error["Code"] == "AccessDenied":
            logger.info("Delete Bucket operation failed as expected")
        else:
            raise UnexpectedBehaviour(f"{e.response} received invalid error code")
    # Update MCG object to use noobaa admin credentials
    mcg_obj.update_s3_creds()
    # Perform bucket delete operation using noobaa admin credentials
    logger.info("Deleting Bucket using Noobaa admin credentials")
    resp = mcg_obj.s3_client.delete_bucket(Bucket=bucket)
    assert (
        resp["ResponseMetadata"]["HTTPStatusCode"] == 204
    ), "Failed to delete bucket using admin credentials"
    logger.info("Bucket deleted successfully using noobaa admin credentials")
