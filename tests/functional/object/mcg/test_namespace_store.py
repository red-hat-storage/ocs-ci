import logging

from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.bucket_utils import (
    write_random_objects_in_pod,
    write_individual_s3_objects,
)
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.cloud_manager import CloudManager
from ocs_ci.ocs.resources.namespacestore import (
    cli_create_namespacestore,
    NamespaceStore,
)

logger = logging.getLogger(__name__)


class TestNamespaceStore:

    def test_namespacestore_with_rgw(
        self,
        rgw_bucket_factory,
        mcg_obj,
        bucket_factory,
        test_directory_setup,
        awscli_pod,
    ):

        # Create a RGW bucket which will be used as backend for NS
        rgw_bucket = rgw_bucket_factory(amount=1)[0]
        logger.info(f"Created RGW bucket {rgw_bucket.name}")

        # Create OBC and CloudManager object for the above bucket, hence it
        # can be used to create NS
        rgw_obc_object = OBC(rgw_bucket.name)
        cld_mgr = CloudManager(obc_obj=rgw_obc_object)

        # Create the Namespacestore using the credentials
        # of rgw bucket
        nss_name = create_unique_resource_name(constants.MCG_NSS, resource_type="rgw")
        cli_create_namespacestore(nss_name, "rgw", mcg_obj, rgw_bucket.name, cld_mgr)
        nss_obj = NamespaceStore(
            name=nss_name,
            method="cli",
            mcg_obj=mcg_obj,
            uls_name=rgw_bucket.name,
        )
        logger.info(
            f"Created namespacestore {nss_obj.name} on top of bucket {rgw_bucket.name}"
        )

        # Create the bucketclass using the namespacestore created
        bucketclass_dict = {
            "interface": "OC",
            "namespace_policy_dict": {
                "type": "Single",
                "namespacestores": [nss_obj],
            },
        }
        bucket = bucket_factory(interface="CLI", bucketclass=bucketclass_dict)[0]

        # Write objects to the bucket one by one
        obj_list = write_random_objects_in_pod(
            awscli_pod, test_directory_setup.origin_dir, amount=20, bs="10K"
        )
        write_individual_s3_objects(
            mcg_obj,
            awscli_pod,
            bucket_factory,
            obj_list,
            target_dir=test_directory_setup.origin_dir,
            bucket_name=bucket.name,
        )

        # Verify NS health
        nss_obj.verify_health()
        logger.info(f"Verified {nss_obj.name} health")
