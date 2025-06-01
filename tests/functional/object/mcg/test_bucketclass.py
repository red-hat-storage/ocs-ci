import logging


from ocs_ci.framework.pytest_customization.marks import (
    mcg,
    red_squad,
    polarion_id,
    tier2,
)
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.bucket_utils import write_random_test_objects_to_bucket
from ocs_ci.ocs.resources.pod import get_pod_logs, get_noobaa_operator_pod

logger = logging.getLogger(__name__)


@mcg
@red_squad
class TestBucketClass:

    @tier2
    @polarion_id("OCS-6295")
    def test_bucketclass_modification(
        self,
        bucket_factory,
        backingstore_factory,
        awscli_pod,
        test_directory_setup,
        mcg_obj,
    ):
        """
        Test to verify bucket class placement policy modification
        from 'Spread' to 'Mirror'

        """

        # create a bucket
        bucketclass_dict = {
            "interface": "CLI",
            "backingstore_dict": {"aws": [(1, "eu-central-1")]},
        }
        bucket = bucket_factory(interface="CLI", bucketclass=bucketclass_dict)[0]

        # write some object data to the bucket
        logger.info(f"Writing some objects to the bucket {bucket.name}")
        write_random_test_objects_to_bucket(
            awscli_pod,
            bucket.name,
            test_directory_setup.origin_dir,
            amount=1,
            pattern="FirstWrite-",
            mcg_obj=mcg_obj,
        )

        # create new backingstore
        logger.info("Creating a new backingstore")
        backingstore = backingstore_factory(
            method="CLI",
            uls_dict=bucketclass_dict["backingstore_dict"],
        )[0]

        # modify the existing bucketclass placement policy to Mirror from Spread
        # and add new backingstore under backingstores
        logger.info(
            f"Changing bucketclass {bucket.bucketclass.name} placement policy from Spread to Mirror"
        )
        bucketclass_obj = OCP(
            kind=constants.BUCKETCLASS,
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name=bucket.bucketclass.name,
        )
        bucketclass_obj.patch(
            params=f'[{{"op": "add", "path": "/spec/placementPolicy/tiers/0/backingStores/-", '
            f'"value": "{backingstore.name}"}},'
            f' {{"op": "replace", "path": "/spec/placementPolicy/tiers/0/placement", "value": "Mirror"}}]',
            format_type="json",
        )
        assert (
            bucketclass_obj.get()["spec"]["placementPolicy"]["tiers"][0]["placement"]
            == "Mirror"
        ), "Placement policy `Mirror` didn't get updated!"
        assert (
            bucketclass_obj.get()["status"]["phase"] == constants.STATUS_READY
        ), f"Bucketclass {bucketclass_obj.resource_name} is not in {constants.STATUS_READY} phase!"

        # Perform IO after the bucketclass is updated
        logger.info(
            f"Writing some new objects to the bucket {bucket.name} after bucketclass is modified"
        )
        write_random_test_objects_to_bucket(
            awscli_pod,
            bucket.name,
            test_directory_setup.origin_dir,
            amount=1,
            pattern="SecondWrite-",
            mcg_obj=mcg_obj,
        )

        # create new bucket on top of updated bucketclass and write some data
        logger.info("Creating new bucket using the update bucketclass")
        new_bucket = bucket_factory(bucketclass=bucket.bucketclass)[0]
        logger.info(f"Created bucket {new_bucket.name}")
        logger.info(f"Writing some object to the new bucket {new_bucket.name}")
        write_random_test_objects_to_bucket(
            awscli_pod,
            new_bucket.name,
            test_directory_setup.origin_dir,
            amount=1,
            mcg_obj=mcg_obj,
        )

        # verify no invalid syntax error is seen in noobaa operator logs
        nb_operator_logs = get_pod_logs(get_noobaa_operator_pod().name)
        assert (
            "invalid input syntax" not in nb_operator_logs
        ), "Looks like there are some `Invalid input syntax` errors in noobaa operator logs"
        logger.info(
            "There is are no `Invalid input syntax` errors seen in noobaa operator logs"
        )
