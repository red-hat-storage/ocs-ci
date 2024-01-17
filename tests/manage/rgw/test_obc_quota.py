import logging
import pytest
import time

from ocs_ci.ocs.resources.objectbucket import OBC
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.bucket_utils import (
    copy_random_individual_objects,
)
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    bugzilla,
    skipif_ocs_version,
    rgw,
)

logger = logging.getLogger(__name__)


@rgw
@bugzilla("1940823")
@skipif_ocs_version("<4.10")
class TestOBCQuota:
    """
    Test OBC Quota feature
    """

    @pytest.mark.parametrize(
        argnames="amount,interface,quota",
        argvalues=[
            pytest.param(
                *[1, "RGW-OC", {"maxObjects": "1", "maxSize": "50M"}],
                marks=[
                    tier1,
                    pytest.mark.polarion_id("OCS-3904"),
                ],
            ),
        ],
    )
    def test_obc_quota(
        self,
        awscli_pod_session,
        rgw_bucket_factory,
        test_directory_setup,
        mcg_obj_session,
        amount,
        interface,
        quota,
    ):
        """
        Test OBC quota feature
            * create OBC with some quota set
            * check if the quota works
            * change the quota
            * check if the new quota works
        """
        bucket_name = rgw_bucket_factory(amount, interface, quota=quota)[0].name
        obc_obj = OBC(bucket_name)
        full_bucket_path = f"s3://{bucket_name}"
        amount = int(quota["maxObjects"]) + 1
        test_dir = test_directory_setup.result_dir
        err_msg = "(QuotaExceeded)"
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="object-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=amount,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            if err_msg in e.args[0]:
                logger.info(f"Quota {quota} worked as expected!!")
            else:
                logger.error("ERROR: Copying objects to bucket failed unexpectedly!!")
        else:
            assert (
                False
            ), "Quota didnt work!! Since more than maximum number of objects were written to the bucket!"

        # Patch the OBC to change the quota
        new_quota = 4
        new_quota_str = '{"spec": {"additionalConfig":{"maxObjects": "4"}}}'
        cmd = f"patch obc {bucket_name} -p '{new_quota_str}' -n openshift-storage --type=merge"
        OCP().exec_oc_cmd(cmd)
        logger.info(f"Patched new quota to obc {bucket_name}")

        # wait for few seconds to make sure the quota really gets applied
        time.sleep(20)

        # check if the new quota applied works
        amount = new_quota - int(quota["maxObjects"])
        awscli_pod_session.exec_cmd_on_pod(f"mkdir -p {test_dir}")
        try:
            copy_random_individual_objects(
                awscli_pod_session,
                pattern="new-object-",
                file_dir=test_dir,
                target=full_bucket_path,
                amount=amount,
                s3_obj=obc_obj,
                ignore_error=False,
            )
        except CommandFailed as e:
            if err_msg in e.args[0]:
                assert False, f"New quota {new_quota_str} didn't get applied!!"
            else:
                logger.error("Copy objects to bucket failed unexpectedly!!")
        else:
            logger.info(f"New quota {new_quota_str} got applied!!")
