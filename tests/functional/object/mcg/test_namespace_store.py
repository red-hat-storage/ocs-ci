import logging
from types import SimpleNamespace

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.bucket_utils import (
    write_random_objects_in_pod,
    write_individual_s3_objects,
)
from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.resources.namespacestore import (
    cli_create_namespacestore,
    NamespaceStore,
)
from ocs_ci.framework.pytest_customization.marks import (
    polarion_id,
    mcg,
    red_squad,
    on_prem_platform_required,
    tier2,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS

logger = logging.getLogger(__name__)


@red_squad
@mcg
class TestNamespaceStore:

    @tier2
    @polarion_id("OCS-6550")
    @on_prem_platform_required
    def test_namespacestore_with_rgw(
        self,
        mcg_obj,
        rgw_bucket_factory,
        teardown_factory,
        bucket_factory,
        test_directory_setup,
        awscli_pod,
    ):
        """
        Test coverage for the scenarios mentioned in the
        bug: https://issues.redhat.com/browse/DFBUGS-700

        """

        # Create a RGW bucket which will be used as backend for NS
        rgw_bucket = rgw_bucket_factory(amount=1, interface="RGW-OC")[0]
        logger.info(f"Created RGW bucket {rgw_bucket.name}")

        # Create OBC and CloudManager object for the above bucket, hence it
        # can be used to create NS
        rgw_obc_object = OBC(rgw_bucket.name)

        # Create the Namespacestore using the credentials
        # of rgw bucket

        # Build a lightweight adapter that mimics the CloudManager interface.
        # cli_create_namespacestore() only cares that cld_mgr.rgw_client.* exists,
        # so we wrap the needed fields into a SimpleNamespace instead of creating
        # a full CloudManager instance.
        cld_mgr_substitute = SimpleNamespace(
            rgw_client=SimpleNamespace(
                s3_internal_endpoint=rgw_obc_object.s3_external_endpoint,
                access_key=rgw_obc_object.access_key_id,
                secret_key=rgw_obc_object.access_key,
            ),
        )
        nss_name = create_unique_resource_name(constants.MCG_NSS, resource_type="rgw")
        cli_create_namespacestore(
            nss_name, "rgw", mcg_obj, rgw_bucket.name, cld_mgr_substitute
        )
        nss_obj = OCP(
            kind=constants.NAMESPACESTORE,
            resource_name=nss_name,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        nss_obj = OCS(**nss_obj.get())
        teardown_factory(nss_obj)
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
        logger.info(f"Created bucket {bucket.name}")

        # Write objects to the bucket one by one
        obj_list = write_random_objects_in_pod(
            awscli_pod, test_directory_setup.origin_dir, amount=20, bs="10K"
        )
        write_individual_s3_objects(
            mcg_obj,
            awscli_pod,
            bucket_factory,
            obj_list,
            target_dir=f"{test_directory_setup.origin_dir}/",
            bucket_name=bucket.name,
        )
        logger.info(f"Successfully uploaded objects to the bucket {bucket.name}")

        # Verify NS health
        nss_obj = NamespaceStore(
            name=nss_name,
            method="cli",
            mcg_obj=mcg_obj,
            uls_name=rgw_bucket.name,
        )
        assert (
            nss_obj.verify_health()
        ), f"{nss_obj.name} is not in {constants.STATUS_READY} state"
        logger.info(f"Verified {nss_obj.name} health")
