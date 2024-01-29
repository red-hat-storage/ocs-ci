import pytest
import json
import logging
import boto3

from ocs_ci.utility import templating
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.backingstore import BackingStore
from ocs_ci.helpers.helpers import create_unique_resource_name, create_resource
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    polarion_id,
    bugzilla,
    skipif_ocs_version,
    skipif_disconnected_cluster,
    red_squad,
    mcg,
    post_upgrade,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.aws import update_config_from_s3
from ocs_ci.utility.utils import load_auth_config
from botocore.exceptions import EndpointConnectionError
from ocs_ci.ocs.bucket_utils import create_aws_bs_using_cli
from ocs_ci.deployment.helpers.mcg_helpers import check_if_mcg_root_secret_public

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
def cleanup(request):
    """
    Clean up function for the backingstores created using CLI
    where we can't use teardown_factory()

    """
    instances = []

    def factory(resource_obj):
        if isinstance(resource_obj, list):
            instances.extend(resource_obj)
        else:
            instances.append(resource_obj)

    def finalizer():
        for instance in instances[::-1]:
            try:
                instance.delete()
            except CommandFailed as ex:
                if "not found" in str(ex).lower():
                    logger.warning(
                        f"Resource {instance.name} could not be found in cleanup."
                        "\nSkipping deletion."
                    )
                else:
                    raise

    request.addfinalizer(finalizer)
    return factory


@mcg
@red_squad
@tier2
@skipif_ocs_version("<4.11")
@skipif_disconnected_cluster
class TestNoobaaSecrets:
    @bugzilla("1992090")
    @polarion_id("OCS-4466")
    def test_duplicate_noobaa_secrets(
        self,
        backingstore_factory,
        cloud_uls_factory,
        mcg_obj,
        teardown_factory,
        cld_mgr,
    ):
        """
        Objective of this test is:
            * Create a secret with the same credentials and see if the duplicates are allowed when BS created
        """
        # create secret with the same credentials to check if duplicates are allowed
        first_bs_obj = backingstore_factory(
            method="oc", uls_dict={"aws": [(1, constants.AWS_REGION)]}
        )[0]
        aws_secret_obj = cld_mgr.aws_client.create_s3_secret(
            cld_mgr.aws_client.secret_prefix, cld_mgr.aws_client.data_prefix
        )
        logger.info(f"New secret created: {aws_secret_obj.name}")
        teardown_factory(aws_secret_obj)

        cloud = "aws"
        uls_tup = (1, constants.AWS_REGION)
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
                "region": constants.AWS_REGION,
                "secret": {
                    "name": aws_secret_obj.name,
                    "namespace": bs_data["metadata"]["namespace"],
                },
            },
        }
        second_bs_obj = create_resource(**bs_data)
        teardown_factory(second_bs_obj)

        # Check if the duplicate secrets are allowed
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

    @bugzilla("2090956")
    @polarion_id("OCS-4467")
    def test_noobaa_secret_deletion_method1(
        self, backingstore_factory, teardown_factory, mcg_obj, cleanup
    ):
        """
        Objectives of this test is:
            1) create the secret using AWS credentials first
            2) create a backingstore using CLI and passing the secret name
            3) create second backingstore created using oc with the same secret
            4) make sure deleting both the backinstores won't affect the secret

        """
        # create secret and first backingstore using CLI
        first_bs_obj = backingstore_factory(
            method="cli", uls_dict={"aws": [(1, "eu-central-1")]}
        )[0]
        first_bs_dict = OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="backingstore"
        ).get(resource_name=first_bs_obj.name)

        secret_name = first_bs_dict["spec"]["awsS3"]["secret"]["name"]

        # Create the second backingstore by applying a YAML that uses the same secret as the first backingstore
        second_bs_obj = backingstore_factory(
            method="oc", uls_dict={"aws": [(1, "eu-central-1")]}
        )[0]

        # Delete both the Backingstores and verify that the secret still exists
        first_bs_obj.delete()
        logger.info(f"First backingstore {first_bs_obj.name} deleted!")
        second_bs_obj.delete()
        logger.info(f"Second backingstore {second_bs_obj.name} deleted!")
        assert (
            OCP(namespace=config.ENV_DATA["cluster_namespace"], kind="secret").get(
                resource_name=secret_name, dont_raise=True
            )
            is not None
        ), "[Not expected] Secret got deleted along when backingstores deleted!!"
        logger.info(
            "Secret remains even after the linked backingstores are deleted, as expected!"
        )

    @bugzilla("2090956")
    @bugzilla("1992090")
    @polarion_id("OCS-4468")
    def test_noobaa_secret_deletion_method2(self, teardown_factory, mcg_obj, cleanup):
        """
        Objectives of this tests are:
            1) create first backingstore using CLI passing credentials, which creates secret as well
            2) create second backingstore using CLI passing credentials, which recognizes the duplicates
               and uses the secret created above
            3) Modify the existing secret credentials see if the owned BS/NS is getting reconciled
            4) delete the first backingstore and make sure secret is not deleted
            5) check for the ownerReference see if its removed for the above backingstore deletion
            6) delete the second backingstore and make sure secret is now deleted

        """

        # create ULS
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
        access_key = secret_dict["AWS"]["AWS_ACCESS_KEY_ID"]
        secret_key = secret_dict["AWS"]["AWS_SECRET_ACCESS_KEY"]
        first_uls_name = create_unique_resource_name(
            resource_description="uls", resource_type="aws"
        )
        client = boto3.resource(
            "s3",
            verify=True,
            endpoint_url="https://s3.amazonaws.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        client.create_bucket(
            Bucket=first_uls_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        first_bs_name = create_unique_resource_name(
            resource_description="backingstore", resource_type="aws"
        )
        create_aws_bs_using_cli(
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
            uls_name=first_uls_name,
            mcg_obj=mcg_obj,
        )
        cleanup(first_bs_obj)

        # create second backingstore using CLI and pass the secret credentials
        second_uls_name = create_unique_resource_name(
            resource_description="uls", resource_type="aws"
        )
        client.create_bucket(
            Bucket=second_uls_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        second_bs_name = create_unique_resource_name(
            resource_description="backingstore", resource_type="aws"
        )
        create_aws_bs_using_cli(
            mcg_obj=mcg_obj,
            backingstore_name=second_bs_name,
            access_key=access_key,
            secret_key=secret_key,
            uls_name=second_uls_name,
            region="eu-central-1",
        )
        mcg_obj.check_backingstore_state(
            backingstore_name=second_bs_name, desired_state=constants.BS_OPTIMAL
        )
        second_bs_obj = BackingStore(
            name=second_bs_name,
            method="cli",
            type="cloud",
            uls_name=second_uls_name,
            mcg_obj=mcg_obj,
        )
        cleanup(second_bs_obj)

        # Modify the secret credentials to wrong one and see if the backingstores get rejected
        secret_name = OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="backingstore"
        ).get(resource_name=second_bs_name)["spec"]["awsS3"]["secret"]["name"]

        wrong_access_key_patch = {
            "data": {"AWS_ACCESS_KEY_ID": "d3JvbmdhY2Nlc3NrZXk="}
        }  # Invalid Access Key
        OCP(namespace=config.ENV_DATA["cluster_namespace"], kind="secret").patch(
            resource_name=secret_name,
            params=json.dumps(wrong_access_key_patch),
            format_type="merge",
        )
        logger.info("Patched wrong access key!")
        assert OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="backingstore"
        ).wait_for_resource(
            resource_name=second_bs_name,
            condition="Creating",
            column="PHASE",
        ), "Backingstores are not getting reconciled after changing linked secret credentials!"
        logger.info("Backingstores getting reconciled!")

        # delete first backingstore
        first_bs_obj.delete()
        logger.info(f"First backingstore {first_bs_name} deleted!")
        assert (
            OCP(namespace=config.ENV_DATA["cluster_namespace"], kind="secret").get(
                resource_name=secret_name, dont_raise=True
            )
            is not None
        ), "[Not expected] Secret got deleted along when first backingstore deleted!!"
        logger.info("Secret exists after the first backingstore deletion!")

        # check for the owner reference
        secret_owner_ref = OCP(
            namespace=config.ENV_DATA["cluster_namespace"], kind="secret"
        ).get(resource_name=secret_name)["metadata"]["ownerReferences"]
        for owner in secret_owner_ref:
            assert owner["name"] != first_bs_name, (
                f"Owner reference for {first_bs_name} still exists in the secret {secret_name} "
                f"even after backingstore {first_bs_name} got deleted!"
            )
        logger.info(
            f"Owner reference for first backingstore {first_bs_name} is deleted in {secret_name} !!"
        )

        # delete second backingstore
        second_bs_obj.delete()
        logger.info(f"Second backingstore {second_bs_name} deleted!")
        assert (
            OCP(namespace=config.ENV_DATA["cluster_namespace"], kind="secret").get(
                resource_name=secret_name, dont_raise=True
            )
            is None
        ), "[Not expected] Secret still exists even after all backingstores linked are deleted!"
        logger.info(
            "Secret got deleted after the all the linked backingstores are deleted!"
        )


@mcg
@post_upgrade
@red_squad
@bugzilla("2219522")
@polarion_id("OCS-5205")
@tier2
def test_noobaa_root_secret():
    """
    This test verifies if the noobaa root secret is publicly
    exposed or not during upgrade scenario

    """

    assert (
        check_if_mcg_root_secret_public() is False
    ), "Seems like MCG root secrets are exposed publicly, please check"
    logger.info("MCG root secrets are not exposed to public")
