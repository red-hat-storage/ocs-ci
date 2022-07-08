import json
import logging

from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.cloud_manager import S3Client
from ocs_ci.ocs.resources.backingstore import BackingStore
from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.utility.aws import update_config_from_s3
from ocs_ci.utility.utils import load_auth_config
from botocore.exceptions import EndpointConnectionError

logger = logging.getLogger(__name__)


def create_bs_using_cli(
    mcg_obj, access_key, secret_key, backingstore_name, uls_name, region
):

    mcg_obj.exec_mcg_cmd(
        f"backingstore create aws-s3 {backingstore_name} "
        f"--access-key {access_key} "
        f"--secret-key {secret_key} "
        f"--target-bucket {uls_name} --region {region}",
        use_yes=True,
    )


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

    def test_noobaa_secret_deletion(self, teardown_factory, mcg_obj):

        # create secret
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

        # create ULS
        cloud = "aws"
        first_uls_name = create_unique_resource_name(
            resource_description="uls", resource_type=cloud.lower()
        )
        aws_client.create_uls(name=first_uls_name, region="eu-central-1")

        # create backingstore using CLI and passing secret credentials
        logger.info(f"Secret dict: {secret_dict}")
        access_key = secret_dict["AWS"]["AWS_ACCESS_KEY_ID"]
        secret_key = secret_dict["AWS"]["AWS_SECRET_ACCESS_KEY"]
        first_bs_name = create_unique_resource_name(
            resource_description="backingstore", resource_type=cloud.lower()
        )
        create_bs_using_cli(
            mcg_obj=mcg_obj,
            backingstore_name=first_bs_name,
            access_key=access_key,
            secret_key=secret_key,
            uls_name=first_uls_name,
            region="eu-central-1",
        )
        mcg_obj.check_backingstore_state(
            backingstore_name=first_bs_name, desired_state=constants.BS_OPTIMAL
        )
        first_bs_obj = BackingStore(
            name=first_bs_name,
            method="cli",
            type="cloud",
            uls_name=first_bs_name,
            mcg_obj=mcg_obj,
        )
        teardown_factory(first_bs_obj)
        first_bs_dict = OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="backingstore"
        ).get(resource_name=first_bs_obj.name)
        assert (
            first_bs_dict["spec"]["awsS3"]["secret"]["name"] == aws_client.secret.name
        ), f"Backingstore isn't using the already existing secret {aws_client.secret.name}!!"
        logger.info(
            f"Backingstore {first_bs_name} is using already existing secret {aws_client.secret.name}!!"
        )

        # create the second backingstore using yaml and passing the secret name
        second_uls_name = create_unique_resource_name(
            resource_description="uls", resource_type=cloud.lower()
        )
        aws_client.create_uls(name=second_uls_name, region="eu-central-1")
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
                "targetBucket": second_uls_name,
                "region": "eu-central-1",
                "secret": {
                    "name": aws_client.secret.name,
                    "namespace": bs_data["metadata"]["namespace"],
                },
            },
        }
        second_bs_obj = create_resource(**bs_data)
        teardown_factory(second_bs_obj)

        # Delete both the Backingstores and verify that the secret still exists
        first_bs_obj.delete()
        logger.info(f"First backingstore {first_bs_name} deleted!")
        second_bs_obj.delete()
        logger.info(f"Second backingstore {second_bs_name} deleted!")
        assert (
            OCP(namespace=config.ENV_DATA["cluster_namespace"], kind="secret").get(
                resource_name=aws_client.secret.name, dont_raise=True
            )
            is not None
        ), "[Not expected] Secret got deleted along when backingstores deleted!!"
        logger.info(
            "Secret remains even after the linked backingstores are deleted, as expected!"
        )
