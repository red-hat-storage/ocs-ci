import json
import logging

from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.cloud_manager import S3Client
from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.utility.aws import update_config_from_s3
from ocs_ci.utility.utils import load_auth_config
from botocore.exceptions import EndpointConnectionError

logger = logging.getLogger(__name__)


class TestNoobaaSecrets:

    """
    Objectives of these tests are:
        1) Create a secret with the same credentials and see if the duplicates are allowed
        2) Delete any of the BS and see if the ownerReference for that particular resource is
         removed from secret (only created through CLI)
        3) Modify the existing secret credentials see if the owned BS/NS is getting reconciled
    """

    def test_duplicate_noobaa_secrets(
        self, backingstore_factory, cloud_uls_factory, mcg_obj, teardown_factory
    ):

        # create secret with the same credentials to check if duplicates are allowed
        first_bs_obj = backingstore_factory(
            method="oc", uls_dict={"aws": [(1, "eu-central-1")]}
        )[0]
        try:
            logger.info(
                "Trying to load credentials from ocs-ci-data. "
                "This flow is only relevant when running under OCS-QE environments."
            )
            secret_dict = update_config_from_s3().get("AUTH")
        except (AttributeError, EndpointConnectionError):
            logger.warning(
                "Failed to load credentials from ocs-ci-data.\n"
                "Your local AWS credentials might be misconfigured.\n"
                "Trying to load credentials from local auth.yaml instead"
            )
            secret_dict = load_auth_config().get("AUTH", {})
        aws_client = S3Client(auth_dict=secret_dict["AWS"])
        teardown_factory(aws_client.secret)
        logger.info(f"New secret created: {aws_client.secret.name}")

        cloud = "aws"
        uls_tup = (1, "eu-central-1")
        uls_name = list(cloud_uls_factory({cloud: [uls_tup]})["aws"])[0]
        logger.info(f"ULS dict: {type(uls_name)}")
        second_bs_name = create_unique_resource_name(
            resource_description="backingstore",
            resource_type=cloud.lower(),
        )
        bs_data = templating.load_yaml(constants.MCG_BACKINGSTORE_YAML)
        bs_data["metadata"]["name"] = second_bs_name
        bs_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
        bs_data["spec"] = {
            "type": "aws-s3",
            "awsS3": {
                "targetBucket": uls_name,
                "region": "eu-central-1",
                "secret": {
                    "name": aws_client.secret.name,
                    "namespace": bs_data["metadata"]["namespace"],
                },
            },
        }
        second_bs_obj = create_resource(**bs_data)
        teardown_factory(second_bs_obj)

        # check if the duplicate secrets are allowed
        first_bs_dict = OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="backingstore"
        ).get(resource_name=first_bs_obj.name)
        second_bs_dict = OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="backingstore"
        ).get(resource_name=second_bs_name)
        assert (
            first_bs_dict["spec"]["awsS3"]["secret"]["name"]
            == second_bs_dict["spec"]["awsS3"]["secret"]["name"]
        ), "Backingstores are not referring to the same secrets when secrets with duplicate credentials are created!!"
        logger.info(
            "Duplicate secrets are not allowed! only the first secret is being referred"
        )

        # edit the secret credentials to wrong one and see if the backingstores get rejected
        first_secret_name = first_bs_dict["spec"]["awsS3"]["secret"]["name"]
        wrong_access_key_patch = {"data": {"AWS_ACCESS_KEY_ID": "d3JvbmdhY2Nlc3NrZXk="}}
        OCP(namespace=config.ENV_DATA["cluster_namespace"], kind="secret").patch(
            resource_name=first_secret_name,
            params=json.dumps(wrong_access_key_patch),
            format_type="merge",
        )
        logger.info("Patched wrong access key!")
        assert OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="backingstore"
        ).wait_for_resource(
            resource_name=first_bs_obj.name,
            condition="Creating",
            column="PHASE",
            error_condition="Ready",
        ), "Backingstores are not getting reconciled after changing linked secret credentials!"
        logger.info("Backingstores getting reconciled!")
